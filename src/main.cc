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

#ifdef __linux__
#define _XOPEN_SOURCE 700  // NOLINT
#else
#define _XOPEN_SOURCE
#endif

#include <dlfcn.h>
#include <event2/thread.h>
#include <execinfo.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <ucontext.h>

#include <iomanip>
#include <ostream>

#include "config.h"
#include "fd_util.h"
#include "io_util.h"
#include "scope_exit.h"
#include "server/server.h"
#include "storage/storage.h"
#include "string_util.h"
#include "time_util.h"
#include "version.h"

namespace google {
bool Symbolize(void *pc, char *out, size_t out_size);
}  // namespace google

Server *srv = nullptr;

Server *GetServer() { return srv; }

extern "C" void signalHandler(int sig) {
  if (srv && !srv->IsStopped()) {
    LOG(INFO) << "Bye Bye";
    srv->Stop();
  }
}

std::ostream &printVersion(std::ostream &os) {
  os << "kvrocks ";

  if (VERSION != "unstable") {
    os << "version ";
  }

  os << VERSION;

  if (!GIT_COMMIT.empty()) {
    os << " (commit " << GIT_COMMIT << ")";
  }

  return os;
}

extern "C" void segvHandler(int sig, siginfo_t *info, void *secret) {
  void *trace[100];

  LOG(ERROR) << "======= Ooops! " << printVersion << " got signal: " << strsignal(sig) << " (" << sig << ") =======";
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

void setupSigSegvAction() {
  struct sigaction act;

  sigemptyset(&act.sa_mask);
  /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction
   * is used. Otherwise, sa_handler is used */
  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND | SA_SIGINFO;
  act.sa_sigaction = segvHandler;
  sigaction(SIGSEGV, &act, nullptr);
  sigaction(SIGBUS, &act, nullptr);
  sigaction(SIGFPE, &act, nullptr);
  sigaction(SIGILL, &act, nullptr);
  sigaction(SIGABRT, &act, nullptr);

  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
  act.sa_handler = signalHandler;
  sigaction(SIGTERM, &act, nullptr);
  sigaction(SIGINT, &act, nullptr);
}

struct NewOpt {
  friend auto &operator<<(std::ostream &os, NewOpt) { return os << std::string(4, ' ') << std::setw(32); }
} new_opt;

static void printUsage(const char *program) {
  std::cout << program << " implements the Redis protocol based on rocksdb" << std::endl
            << "Usage:" << std::endl
            << std::left << new_opt << "-c, --config <filename>"
            << "set config file to <filename>, or `-' for stdin" << std::endl
            << new_opt << "-v, --version"
            << "print version information" << std::endl
            << new_opt << "-h, --help"
            << "print this help message" << std::endl
            << new_opt << "--<config-key> <config-value>"
            << "overwrite specific config option <config-key> to <config-value>" << std::endl;
}

static CLIOptions parseCommandLineOptions(int argc, char **argv) {
  using namespace std::string_view_literals;
  CLIOptions opts;

  for (int i = 1; i < argc; ++i) {
    if ((argv[i] == "-c"sv || argv[i] == "--config"sv) && i + 1 < argc) {
      opts.conf_file = argv[++i];
    } else if (argv[i] == "-v"sv || argv[i] == "--version"sv) {
      std::cout << printVersion << std::endl;
      std::exit(0);
    } else if (argv[i] == "-h"sv || argv[i] == "--help"sv) {
      printUsage(*argv);
      std::exit(0);
    } else if (std::string_view(argv[i], 2) == "--" && std::string_view(argv[i]).size() > 2 && i + 1 < argc) {
      auto key = std::string_view(argv[i] + 2);
      opts.cli_options.emplace_back(key, argv[++i]);
    } else {
      printUsage(*argv);
      std::exit(1);
    }
  }

  return opts;
}

static void initGoogleLog(const Config *config) {
  FLAGS_minloglevel = config->log_level;
  FLAGS_max_log_size = 100;
  FLAGS_logbufsecs = 0;

  if (Util::ToLower(config->log_dir) == "stdout") {
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

bool supervisedUpstart() {
  const char *upstart_job = getenv("UPSTART_JOB");
  if (!upstart_job) {
    LOG(WARNING) << "upstart supervision requested, but UPSTART_JOB not found";
    return false;
  }
  LOG(INFO) << "supervised by upstart, will stop to signal readiness";
  raise(SIGSTOP);
  unsetenv("UPSTART_JOB");
  return true;
}

bool supervisedSystemd() {
  const char *notify_socket = getenv("NOTIFY_SOCKET");
  if (!notify_socket) {
    LOG(WARNING) << "systemd supervision requested, but NOTIFY_SOCKET not found";
    return false;
  }

  auto fd = UniqueFD(socket(AF_UNIX, SOCK_DGRAM, 0));
  if (!fd) {
    LOG(WARNING) << "Can't connect to systemd socket " << notify_socket;
    return false;
  }

  struct sockaddr_un su;
  memset(&su, 0, sizeof(su));
  su.sun_family = AF_UNIX;
  strncpy(su.sun_path, notify_socket, sizeof(su.sun_path) - 1);
  su.sun_path[sizeof(su.sun_path) - 1] = '\0';
  if (notify_socket[0] == '@') su.sun_path[0] = '\0';

  struct iovec iov;
  memset(&iov, 0, sizeof(iov));
  std::string ready = "READY=1";
  iov.iov_base = &ready[0];
  iov.iov_len = ready.size();

  struct msghdr hdr;
  memset(&hdr, 0, sizeof(hdr));
  hdr.msg_name = &su;
  hdr.msg_namelen = offsetof(struct sockaddr_un, sun_path) + strlen(notify_socket);
  hdr.msg_iov = &iov;
  hdr.msg_iovlen = 1;

  int sendto_flags = 0;
  unsetenv("NOTIFY_SOCKET");
#ifdef HAVE_MSG_NOSIGNAL
  sendto_flags |= MSG_NOSIGNAL;
#endif
  if (sendmsg(*fd, &hdr, sendto_flags) < 0) {
    LOG(WARNING) << "Can't send notification to systemd";
    return false;
  }
  return true;
}

bool isSupervisedMode(int mode) {
  if (mode == kSupervisedAutoDetect) {
    const char *upstart_job = getenv("UPSTART_JOB");
    const char *notify_socket = getenv("NOTIFY_SOCKET");
    if (upstart_job) {
      mode = kSupervisedUpStart;
    } else if (notify_socket) {
      mode = kSupervisedSystemd;
    }
  }
  if (mode == kSupervisedUpStart) {
    return supervisedUpstart();
  } else if (mode == kSupervisedSystemd) {
    return supervisedSystemd();
  }
  return false;
}

static Status createPidFile(const std::string &path) {
  auto fd = UniqueFD(open(path.data(), O_RDWR | O_CREAT, 0660));
  if (!fd) {
    return Status::FromErrno();
  }

  std::string pid_str = std::to_string(getpid());
  auto s = Util::Write(*fd, pid_str);
  if (!s.IsOK()) {
    return s.Prefixed("failed to write to PID-file");
  }

  return Status::OK();
}

static void removePidFile(const std::string &path) { std::remove(path.data()); }

static void daemonize() {
  pid_t pid = fork();
  if (pid < 0) {
    LOG(ERROR) << "Failed to fork the process, err: " << strerror(errno);
    exit(1);
  }
  if (pid > 0) exit(EXIT_SUCCESS);  // parent process
  // change the file mode
  umask(0);
  if (setsid() < 0) {
    LOG(ERROR) << "Failed to setsid, err: %s" << strerror(errno);
    exit(1);
  }
  close(STDIN_FILENO);
  close(STDOUT_FILENO);
  close(STDERR_FILENO);
}

int main(int argc, char *argv[]) {
  srand(static_cast<unsigned>(Util::GetTimeStamp()));
  google::InitGoogleLogging("kvrocks");
  evthread_use_pthreads();

  signal(SIGPIPE, SIG_IGN);
  setupSigSegvAction();

  auto opts = parseCommandLineOptions(argc, argv);

  Config config;
  Status s = config.Load(opts);
  if (!s.IsOK()) {
    std::cout << "Failed to load config. Error: " << s.Msg() << std::endl;
    return 1;
  }

  initGoogleLog(&config);
  LOG(INFO) << printVersion;
  // Tricky: We don't expect that different instances running on the same port,
  // but the server use REUSE_PORT to support the multi listeners. So we connect
  // the listen port to check if the port has already listened or not.
  if (!config.binds.empty()) {
    uint32_t ports[] = {config.port, config.tls_port, 0};
    for (uint32_t *port = ports; *port; ++port) {
      if (Util::IsPortInUse(*port)) {
        LOG(ERROR) << "Could not create server TCP since the specified port[" << *port << "] is already in use";
        return 1;
      }
    }
  }
  bool is_supervised = isSupervisedMode(config.supervised_mode);
  if (config.daemonize && !is_supervised) daemonize();
  s = createPidFile(config.pidfile);
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to create pidfile: " << s.Msg();
    return 1;
  }
  auto pidfile_exit = MakeScopeExit([&config] { removePidFile(config.pidfile); });

#ifdef ENABLE_OPENSSL
  // initialize OpenSSL
  if (config.tls_port) {
    InitSSL();
  }
#endif

  Engine::Storage storage(&config);
  s = storage.Open();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to open: " << s.Msg();
    return 1;
  }
  Server server(&storage, &config);
  srv = &server;
  s = srv->Start();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to start server: " << s.Msg();
    return 1;
  }
  srv->Join();

  google::ShutdownGoogleLogging();
  return 0;
}
