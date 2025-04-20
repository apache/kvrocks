/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include <event2/thread.h>
#include <fcntl.h>
#include <getopt.h>
#include <sys/stat.h>

#include <csignal>
#include <memory>

#include "cli/daemon_util.h"
#include "cli/pid_util.h"
#include "cli/version_util.h"
#include "config.h"
#include "config/config.h"
#include "io_util.h"
#include "logging.h"
#include "parser.h"
#include "redis_writer.h"
#include "spdlog/common.h"
#include "spdlog/logger.h"
#include "spdlog/sinks/daily_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"
#include "storage/storage.h"
#include "sync.h"
#include "version.h"

const char *kDefaultConfPath = "./kvrocks2redis.conf";

std::function<void()> hup_handler;

struct Options {
  std::string conf_file = kDefaultConfPath;
};

extern "C" void SignalHandler([[maybe_unused]] int sig) {
  if (hup_handler) hup_handler();
}

static void Usage(const char *program) {
  std::cout << program << " sync kvrocks to redis\n"
            << "\t-c <path> specifies the config file, defaulting to " << kDefaultConfPath << "\n"
            << "\t-h print this help message\n"
            << "\t-v print version information\n";
  exit(0);
}

static Options ParseCommandLineOptions(int argc, char **argv) {
  int ch = 0;
  Options opts;
  while ((ch = ::getopt(argc, argv, "c:hv")) != -1) {
    switch (ch) {
      case 'c': {
        opts.conf_file = optarg;
        break;
      }
      case 'v':
        std::cout << "kvrocks2redis " << PrintVersion() << std::endl;
        exit(0);
      case 'h':
      default:
        Usage(argv[0]);
    }
  }
  return opts;
}

static void InitSpdlog(const kvrocks2redis::Config &config) {
  std::vector<spdlog::sink_ptr> sinks = {
      std::make_shared<spdlog::sinks::daily_file_sink_mt>(config.output_dir + "/kvrocks2redis.log", 0, 0),
      std::make_shared<spdlog::sinks::stdout_color_sink_mt>()};
  auto logger = std::make_shared<spdlog::logger>("kvrocks2redis", sinks.begin(), sinks.end());
  logger->set_level(config.loglevel);
  logger->flush_on(spdlog::level::info);
  spdlog::set_default_logger(logger);
}

Server *GetServer() { return nullptr; }

int main(int argc, char *argv[]) {
  evthread_use_pthreads();

  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);

  auto opts = ParseCommandLineOptions(argc, argv);
  std::string config_file_path = std::move(opts.conf_file);

  kvrocks2redis::Config config;
  Status s = config.Load(config_file_path);
  if (!s.IsOK()) {
    std::cout << "Failed to load config. Error: " << s.Msg() << std::endl;
    exit(1);
  }

  InitSpdlog(config);
  info("kvrocks2redis {}", PrintVersion());

  if (config.daemonize) Daemonize();

  s = CreatePidFile(config.pidfile);
  if (!s.IsOK()) {
    error("Failed to create pidfile '{}': {}", config.pidfile, s.Msg());
    exit(1);
  }

  Config kvrocks_config;
  kvrocks_config.db_dir = config.db_dir;
  kvrocks_config.cluster_enabled = config.cluster_enabled;
  kvrocks_config.slot_id_encoded = config.cluster_enabled;

  engine::Storage storage(&kvrocks_config);
  s = storage.Open(kDBOpenModeAsSecondaryInstance);
  if (!s.IsOK()) {
    error("Failed to open Kvrocks storage: {}", s.Msg());
    exit(1);
  }

  RedisWriter writer(&config);
  Parser parser(&storage, &writer);

  Sync sync(&storage, &writer, &parser, &config);
  hup_handler = [&sync] {
    if (!sync.IsStopped()) {
      info("Stopping sync");
      sync.Stop();
    }
  };
  sync.Start();

  RemovePidFile(config.pidfile);
  return 0;
}
