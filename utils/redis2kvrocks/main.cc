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

#include <execinfo.h>
#include <glog/logging.h>

#include <iomanip>
#include <ostream>
#include <signal.h>
#include <stdlib.h>
#include <string.h>

#include "common/rdb_stream.h"
#include "config.h"
#include "scope_exit.h"
#include "storage/rdb.h"
#include "storage/storage.h"
#include "vendor/crc64.h"
#include "version.h"

namespace google {
bool Symbolize(void *pc, char *out, size_t out_size);
}  // namespace google

Server *srv = nullptr;

extern "C" void SignalHandler(int sig) {
  LOG(INFO) << "Signal " << sig << " received,  force exit now";
  exit(1);
}

std::ostream &PrintVersion(std::ostream &os) {  // TODO should define in common/utils
  os << "redis2kvrocks ";

  if (VERSION != "unstable") {
    os << "version ";
  }

  os << VERSION;

  if (!GIT_COMMIT.empty()) {
    os << " (commit " << GIT_COMMIT << ")";
  }

  return os;
}

struct NewOpt {
  friend auto &operator<<(std::ostream &os, NewOpt) { return os << std::string(4, ' ') << std::setw(32); }
} new_opt;

static void PrintUsage(const char *program) {
  std::cout << program << " load redis's rdb file to kvrocks, only used to create db from rdb file" << std::endl
            << "Usage:" << std::endl
            << std::left << new_opt << "-c, --config <filename>"
            << "set config file to <filename>, or `-` for stdin" << std::endl
            << new_opt << "-v, --version"
            << "print version information" << std::endl
            << new_opt << "-h, --help"
            << "print this help message" << std::endl
            << new_opt << "--<config-key> <config-value>"
            << "overwrite specific config option <config-key> to <config-value>" << std::endl
            << new_opt << "-r --rdb <filename>"
            << "load rdb file <filename> to rocksdb" << std::endl
            << new_opt << "-ns --namespace [namespace]"
            << "config the namespace to load, default ''" << std::endl;
}

struct R2KCLIOptions {
  CLIOptions options;
  std::string rdb_file;
  std::string namespace_;
  R2KCLIOptions() : namespace_(kDefaultNamespace){};
};

static R2KCLIOptions ParseCommandLineOptions(int argc, char **argv) {
  using namespace std::string_view_literals;
  R2KCLIOptions opts;

  for (int i = 1; i < argc; ++i) {
    if ((argv[i] == "-c"sv || argv[i] == "--config"sv) && i + 1 < argc) {
      opts.options.conf_file = argv[++i];
    } else if (argv[i] == "-v"sv || argv[i] == "--version"sv) {
      std::cout << PrintVersion << std::endl;
      std::exit(0);
    } else if (argv[i] == "-h"sv || argv[i] == "--help"sv) {
      PrintUsage(*argv);
      std::exit(0);
    } else if (std::string_view(argv[i], 2) == "--" && std::string_view(argv[i]).size() > 2 && i + 1 < argc) {
      auto key = std::string_view(argv[i] + 2);
      opts.options.cli_options.emplace_back(key, argv[++i]);
    } else if ((argv[i] == "-r"sv || argv[i] == "--rdb"sv) && i + 1 < argc) {
      opts.rdb_file = argv[++i];
    } else if ((argv[i] == "-ns"sv || argv[i] == "--rdb"sv) && i + 1 < argc) {
      opts.namespace_ = argv[++i];
    } else {
      PrintUsage(*argv);
      std::exit(1);
    }
  }

  return opts;
}

static void InitGoogleLog(const Config *config) {
  FLAGS_minloglevel = config->log_level;
  FLAGS_max_log_size = 100;
  FLAGS_logbufsecs = 0;

  if (util::EqualICase(config->log_dir, "stdout")) {
    for (int level = google::INFO; level <= google::FATAL; level++) {
      google::SetLogDestination(level, "");
    }
    FLAGS_stderrthreshold = google::ERROR;
    FLAGS_logtostdout = true;
  } else {
    FLAGS_log_dir = config->log_dir + "/";
    if (config->log_retention_days != -1) {
      google::EnableLogCleaner(config->log_retention_days);
    }
  }
}

extern "C" void SegvHandler(int sig, siginfo_t *info, void *secret) {
  void *trace[100];

  LOG(ERROR) << "======= Ooops! " << PrintVersion << " got signal: " << strsignal(sig) << " (" << sig << ") =======";
  int trace_size = backtrace(trace, sizeof(trace) / sizeof(void *));
  char **messages = backtrace_symbols(trace, trace_size);

  size_t max_msg_len = 0;
  for (int i = 1; i < trace_size; ++i) {
    auto msg_len = strlen(messages[i]);
    if (msg_len > max_msg_len) {
      max_msg_len = msg_len;
    }
  }

  for (int i = 1; i < trace_size; ++i) {
    char func_info[1024] = {};
    if (google::Symbolize(trace[i], func_info, sizeof(func_info) - 1)) {
      LOG(ERROR) << std::left << std::setw(static_cast<int>(max_msg_len)) << messages[i] << "  " << func_info;
    } else {
      LOG(ERROR) << messages[i];
    }
  }

  struct sigaction act;
  /* Make sure we exit with the right signal at the end. So for instance
   * the core will be dumped if enabled.
   */
  sigemptyset(&act.sa_mask);
  /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction
   * is used. Otherwise, sa_handler is used
   */
  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
  act.sa_handler = SIG_DFL;
  sigaction(sig, &act, nullptr);
  kill(getpid(), sig);
}

void SetupSigSegvAction() {
  struct sigaction act;

  sigemptyset(&act.sa_mask);
  /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction
   * is used. Otherwise, sa_handler is used */
  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND | SA_SIGINFO;
  act.sa_sigaction = SegvHandler;
  sigaction(SIGSEGV, &act, nullptr);
  sigaction(SIGBUS, &act, nullptr);
  sigaction(SIGFPE, &act, nullptr);
  sigaction(SIGILL, &act, nullptr);
  sigaction(SIGABRT, &act, nullptr);

  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
  act.sa_handler = SignalHandler;
  sigaction(SIGTERM, &act, nullptr);
  sigaction(SIGINT, &act, nullptr);
}

bool checkDBExist(Config *config) {
  engine::Storage storage(config);
  auto s = storage.Open(true);
  return s.IsOK();
}

int main(int argc, char *argv[]) {
  google::InitGoogleLogging("redis2kvrocks");

  SetupSigSegvAction();

  auto opts = ParseCommandLineOptions(argc, argv);

  Config config;
  Status s = config.Load(opts.options);
  if (!s.IsOK()) {
    std::cout << "Failed to load config. Error: " << s.Msg() << std::endl;
    return 1;
  }

  crc64_init();
  InitGoogleLog(&config);
  LOG(INFO) << PrintVersion;

  // rdb file can only be loaded into empty database.
  if (checkDBExist(&config)) {
    LOG(ERROR) << " Database already exist, can't load from rdb file!";
    return 1;
  }

  engine::Storage storage(&config);
  s = storage.Open();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to open: " << s.Msg();
    return 1;
  }

  auto stream_ptr = std::make_shared<RdbFileStream>(opts.rdb_file);
  s = stream_ptr->Open();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to open: " << s.Msg();
    return 1;
  }
  RDB rdb(&storage, &config, opts.namespace_, stream_ptr);
  s = rdb.LoadRdb();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to load rdb: " << s.Msg();
    return 1;
  }
  LOG(INFO) << "Load rdb file success!";
  storage.CloseDB();
  LOG(INFO) << "Close DB success, redis2kvrocks exit!";
  return 0;
}
