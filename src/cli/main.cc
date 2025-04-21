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

#include "spdlog/common.h"
#ifdef __linux__
#define _XOPEN_SOURCE 700  // NOLINT
#else
#define _XOPEN_SOURCE
#endif

#include <event2/thread.h>

#include <iomanip>
#include <memory>
#include <ostream>

#include "daemon_util.h"
#include "io_util.h"
#include "logging.h"
#include "pid_util.h"
#include "scope_exit.h"
#include "server/server.h"
#include "signal_util.h"
#include "spdlog/sinks/daily_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "storage/storage.h"
#include "string_util.h"
#include "time_util.h"
#include "vendor/crc64.h"
#include "version_util.h"

Server *srv = nullptr;

extern "C" void SignalHandler(int sig) {
  if (srv && !srv->IsStopped()) {
    info("Signal {} ({}) received, stopping the server", strsignal(sig), sig);
    srv->Stop();
  }
}

struct NewOpt {
  friend auto &operator<<(std::ostream &os, NewOpt) { return os << std::string(4, ' ') << std::setw(32); }
} new_opt;

static void PrintUsage(const char *program) {
  std::cout << program << " implements the Redis protocol based on rocksdb" << std::endl
            << "Usage:" << std::endl
            << std::left << new_opt << "-c, --config <filename>"
            << "set config file to <filename>, or `-` for stdin" << std::endl
            << new_opt << "-v, --version"
            << "print version information" << std::endl
            << new_opt << "-h, --help"
            << "print this help message" << std::endl
            << new_opt << "--<config-key> <config-value>"
            << "overwrite specific config option <config-key> to <config-value>" << std::endl;
}

static CLIOptions ParseCommandLineOptions(int argc, char **argv) {
  using namespace std::string_view_literals;
  CLIOptions opts;

  for (int i = 1; i < argc; ++i) {
    if ((argv[i] == "-c"sv || argv[i] == "--config"sv) && i + 1 < argc) {
      opts.conf_file = argv[++i];
    } else if (argv[i] == "-v"sv || argv[i] == "--version"sv) {
      std::cout << "kvrocks " << PrintVersion() << std::endl;
      std::exit(0);
    } else if (argv[i] == "-h"sv || argv[i] == "--help"sv) {
      PrintUsage(*argv);
      std::exit(0);
    } else if (std::string_view(argv[i], 2) == "--" && std::string_view(argv[i]).size() > 2 && i + 1 < argc) {
      auto key = std::string_view(argv[i] + 2);
      opts.cli_options.emplace_back(key, argv[++i]);
    } else {
      PrintUsage(*argv);
      std::exit(1);
    }
  }

  return opts;
}

static Status InitSpdlog(const Config &config) {
  std::vector<spdlog::sink_ptr> sinks;

  // NOTE: to be compatible with old behaviors, we allow negative log_retention_days (-1)
  auto retention_days = config.log_retention_days < 0 ? 0 : config.log_retention_days;

  for (const auto &i : util::Split(config.log_dir, ",")) {
    auto item = util::Trim(i, " ");
    auto vals = util::Split(item, ":");
    if (vals.empty()) {
      return {Status::NotOK, "cannot get valid directory in config option log-dir"};
    }

    auto dir = vals[0];
    auto level_str = vals.size() >= 2 ? vals[1] : "info";
    auto it = std::find_if(log_levels.begin(), log_levels.end(), [&](const auto &v) { return v.name == level_str; });
    if (it == log_levels.end()) {
      return {Status::NotOK, "failed to set log level with config option log-dir"};
    }
    auto level = it->val;

    if (util::EqualICase(dir, "stdout")) {
      sinks.push_back(std::make_shared<spdlog::sinks::stdout_color_sink_mt>());
    } else if (util::EqualICase(dir, "stderr")) {
      sinks.push_back(std::make_shared<spdlog::sinks::stderr_color_sink_mt>());
    } else {
      sinks.push_back(
          std::make_shared<spdlog::sinks::daily_file_sink_mt>(dir + "/kvrocks.log", 0, 0, false, retention_days));
    }

    sinks.back()->set_level(level);
  }

  auto logger = std::make_shared<spdlog::logger>("kvrocks", sinks.begin(), sinks.end());
  logger->set_level(config.log_level);
  logger->set_pattern("[%Y-%m-%dT%H:%M:%S.%f%z][%^%L%$][%s:%#] %v");
  logger->flush_on(spdlog::level::info);
  spdlog::set_default_logger(logger);

  return Status::OK();
}

int main(int argc, char *argv[]) {
  srand(static_cast<unsigned>(util::GetTimeStamp()));
  crc64_init();

  evthread_use_pthreads();
  auto event_exit = MakeScopeExit(libevent_global_shutdown);

  signal(SIGPIPE, SIG_IGN);
  SetupSigSegvAction(SignalHandler);

  auto opts = ParseCommandLineOptions(argc, argv);

  Config config;
  Status s = config.Load(opts);
  if (!s.IsOK()) {
    std::cout << "Failed to load config. Error: " << s.Msg() << std::endl;
    return 1;
  }
  const auto socket_fd_exit = MakeScopeExit([&config] {
    if (config.socket_fd != -1) {
      close(config.socket_fd);
    }
  });

  if (auto s = InitSpdlog(config); !s) {
    std::cout << "Failed to initialize logging system. Error: " << s.Msg() << std::endl;
    return 1;
  }
  info("kvrocks {}", PrintVersion());
  // Tricky: We don't expect that different instances running on the same port,
  // but the server use REUSE_PORT to support the multi listeners. So we connect
  // the listen port to check if the port has already listened or not.
  if (config.socket_fd == -1 && !config.binds.empty()) {
    uint32_t ports[] = {config.port, config.tls_port, 0};
    for (uint32_t *port = ports; *port; ++port) {
      if (util::IsPortInUse(*port)) {
        error("Could not create the server since the specified port {} is already in use", *port);
        return 1;
      }
    }
  }
  bool is_supervised = IsSupervisedMode(config.supervised_mode);
  if (config.daemonize && !is_supervised) Daemonize();
  s = CreatePidFile(config.pidfile);
  if (!s.IsOK()) {
    error("Failed to create pidfile: {}", s.Msg());
    return 1;
  }
  auto pidfile_exit = MakeScopeExit([&config] { RemovePidFile(config.pidfile); });

#ifdef ENABLE_OPENSSL
  // initialize OpenSSL
  if (config.tls_port || config.tls_replication) {
    InitSSL();
  }
#endif

  engine::Storage storage(&config);
  s = storage.Open();
  if (!s.IsOK()) {
    error("Failed to open the database: {}", s.Msg());
    return 1;
  }
  Server server(&storage, &config);
  srv = &server;
  s = srv->Start();
  if (!s.IsOK()) {
    error("Failed to start server: {}", s.Msg());
    return 1;
  }
  srv->Join();

  return 0;
}
