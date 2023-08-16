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
#include <glog/logging.h>
#include <sys/stat.h>

#include <csignal>

#include "config.h"
#include "config/config.h"
#include "io_util.h"
#include "parser.h"
#include "redis_writer.h"
#include "storage/storage.h"
#include "sync.h"
#include "version.h"

const char *kDefaultConfPath = "./kvrocks2redis.conf";

std::function<void()> hup_handler;

struct Options {
  std::string conf_file = kDefaultConfPath;
};

std::ostream &PrintVersion(std::ostream &os) {
  os << "kvrocks2redis ";

  if (VERSION != "unstable") {
    os << "version ";
  }

  os << VERSION;

  if (!GIT_COMMIT.empty()) {
    os << " (commit " << GIT_COMMIT << ")";
  }

  return os;
}

extern "C" void SignalHandler(int sig) {
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
        std::cout << PrintVersion << std::endl;
        exit(0);
      case 'h':
      default:
        Usage(argv[0]);
    }
  }
  return opts;
}

static void InitGoogleLog(const kvrocks2redis::Config *config) {
  FLAGS_minloglevel = config->loglevel;
  FLAGS_max_log_size = 100;
  FLAGS_logbufsecs = 0;
  FLAGS_log_dir = config->output_dir;
}

static Status CreatePidFile(const std::string &path) {
  int fd = open(path.data(), O_RDWR | O_CREAT | O_EXCL, 0660);
  if (fd < 0) {
    return {Status::NotOK, strerror(errno)};
  }

  std::string pid_str = std::to_string(getpid());
  auto s = util::Write(fd, pid_str);
  if (!s.IsOK()) {
    return s.Prefixed("failed to write to PID-file");
  }

  close(fd);
  return Status::OK();
}

static void RemovePidFile(const std::string &path) { std::remove(path.data()); }

static void Daemonize() {
  pid_t pid = fork();
  if (pid < 0) {
    LOG(ERROR) << "Failed to fork the process. Error: " << strerror(errno);
    exit(1);
  }

  if (pid > 0) exit(EXIT_SUCCESS);  // parent process
  // change the file mode
  umask(0);
  if (setsid() < 0) {
    LOG(ERROR) << "Failed to setsid. Error: " << strerror(errno);
    exit(1);
  }

  close(STDIN_FILENO);
  close(STDOUT_FILENO);
  close(STDERR_FILENO);
}

Server *GetServer() { return nullptr; }

int main(int argc, char *argv[]) {
  google::InitGoogleLogging("kvrocks2redis");
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

  InitGoogleLog(&config);
  LOG(INFO) << PrintVersion;

  if (config.daemonize) Daemonize();

  s = CreatePidFile(config.pidfile);
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to create pidfile '" << config.pidfile << "': " << s.Msg();
    exit(1);
  }

  Config kvrocks_config;
  kvrocks_config.db_dir = config.db_dir;
  kvrocks_config.cluster_enabled = config.cluster_enabled;
  kvrocks_config.slot_id_encoded = config.cluster_enabled;

  engine::Storage storage(&kvrocks_config);
  s = storage.Open(true);
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to open Kvrocks storage: " << s.Msg();
    exit(1);
  }

  RedisWriter writer(&config);
  Parser parser(&storage, &writer);

  Sync sync(&storage, &writer, &parser, &config);
  hup_handler = [&sync] {
    if (!sync.IsStopped()) {
      LOG(INFO) << "Bye Bye";
      sync.Stop();
    }
  };
  sync.Start();

  RemovePidFile(config.pidfile);
  return 0;
}
