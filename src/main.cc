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

#include <getopt.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dlfcn.h>
#ifdef __linux__
#define _XOPEN_SOURCE 700
#else
#define _XOPEN_SOURCE
#endif
#include <sys/un.h>
#include <signal.h>
#include <execinfo.h>
#include <ucontext.h>
#include <event2/thread.h>
#include <glog/logging.h>
#include "worker.h"
#include "storage.h"
#include "version.h"
#include "config.h"
#include "server.h"
#include "util.h"

namespace google {
bool Symbolize(void* pc, char* out, size_t out_size);
}  // namespace google

std::function<void()> hup_handler;

struct Options {
  std::string conf_file;
  bool show_usage = false;
};

Server *srv = nullptr;

Server *GetServer() {
  return srv;
}

extern "C" void signal_handler(int sig) {
  if (hup_handler) hup_handler();
}

extern "C" void segvHandler(int sig, siginfo_t *info, void *secret) {
  void *trace[100];

  LOG(ERROR) << "======= Ooops! kvrocks "<< VERSION << " @" << GIT_COMMIT
    << " got signal: " << strsignal(sig) << " (" << sig << ") =======";
  int trace_size = backtrace(trace, sizeof(trace) / sizeof(void *));
  char **messages = backtrace_symbols(trace, trace_size);
  for (int i = 1; i < trace_size; ++i) {
    char func_info[1024] = {};
    if (google::Symbolize(trace[i], func_info, sizeof(func_info) - 1)) {
      LOG(ERROR) << messages[i] << ": " << func_info;
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
  act.sa_handler = signal_handler;
  sigaction(SIGTERM, &act, nullptr);
  sigaction(SIGINT, &act, nullptr);
}

static void usage(const char* program) {
  std::cout << program << " implements the Redis protocol based on rocksdb\n"
            << "\t-c config file\n"
            << "\t-h help\n";
  exit(0);
}

static Options parseCommandLineOptions(int argc, char **argv) {
  int ch;
  Options opts;
  while ((ch = ::getopt(argc, argv, "c:hv")) != -1) {
    switch (ch) {
      case 'c': opts.conf_file = optarg; break;
      case 'h': opts.show_usage = true; break;
      case 'v': exit(0);
      default: usage(argv[0]);
    }
  }
  return opts;
}

static void initGoogleLog(const Config *config) {
  FLAGS_minloglevel = config->loglevel;
  FLAGS_max_log_size = 100;
  FLAGS_logbufsecs = 0;

  if (Util::ToLower(config->log_dir) == "stdout") {
    for (int level = google::INFO; level <= google::FATAL; level++) {
      google::SetLogDestination(level, "");
    }
    FLAGS_stderrthreshold = google::ERROR;
    FLAGS_logtostdout = true;
  } else {
    FLAGS_log_dir = config->log_dir;
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

  int fd = 1;
  if ((fd = socket(AF_UNIX, SOCK_DGRAM, 0)) == -1) {
    LOG(WARNING) << "Can't connect to systemd socket " << notify_socket;
    return false;
  }

  struct sockaddr_un su;
  memset(&su, 0, sizeof(su));
  su.sun_family = AF_UNIX;
  strncpy (su.sun_path, notify_socket, sizeof(su.sun_path) -1);
  su.sun_path[sizeof(su.sun_path) - 1] = '\0';
  if (notify_socket[0] == '@')
    su.sun_path[0] = '\0';

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
  if (sendmsg(fd, &hdr, sendto_flags) < 0) {
    LOG(WARNING) << "Can't send notification to systemd";
    close(fd);
    return false;
  }
  close(fd);
  return true;
}

bool isSupervisedMode(int mode) {
  if (mode == SUPERVISED_AUTODETECT)  {
    const char *upstart_job = getenv("UPSTART_JOB");
    const char *notify_socket = getenv("NOTIFY_SOCKET");
    if (upstart_job) {
      mode = SUPERVISED_UPSTART;
    }  else if (notify_socket) {
      mode = SUPERVISED_SYSTEMD;
    }
  }
  if (mode == SUPERVISED_UPSTART) {
    return supervisedUpstart();
  } else if (mode == SUPERVISED_SYSTEMD) {
    return supervisedSystemd();
  }
  return false;
}

static Status createPidFile(const std::string &path) {
  int fd = open(path.data(), O_RDWR|O_CREAT, 0660);
  if (fd < 0) {
    return Status(Status::NotOK, strerror(errno));
  }
  std::string pid_str = std::to_string(getpid());
  write(fd, pid_str.data(), pid_str.size());
  close(fd);
  return Status::OK();
}

static void removePidFile(const std::string &path) {
  std::remove(path.data());
}

static void daemonize() {
  pid_t pid;

  pid = fork();
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

int main(int argc, char* argv[]) {
  google::InitGoogleLogging("kvrocks");
  evthread_use_pthreads();

  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  setupSigSegvAction();

  auto opts = parseCommandLineOptions(argc, argv);
  if (opts.show_usage) usage(argv[0]);

  Redis::InitCommandsTable();
  Redis::PopulateCommands();

  Config config;
  Status s = config.Load(opts.conf_file);
  if (!s.IsOK()) {
    std::cout << "Failed to load config, err: " << s.Msg() << std::endl;
    exit(1);
  }
  initGoogleLog(&config);
  LOG(INFO) << "Version: " << VERSION << " @" << GIT_COMMIT << std::endl;
  // Tricky: We don't expect that different instances running on the same port,
  // but the server use REUSE_PORT to support the multi listeners. So we connect
  // the listen port to check if the port has already listened or not.
  if (!config.binds.empty()) {
    int ports[] = {config.port, config.tls_port, 0};
    for (int *port = ports; *port; ++port) {
      if (Util::IsPortInUse(*port)) {
        LOG(ERROR)<< "Could not create server TCP since the specified port["
                  << *port << "] is already in use" << std::endl;
        exit(1);
      }
    }
  }
  bool is_supervised = isSupervisedMode(config.supervised_mode);
  if (config.daemonize && !is_supervised) daemonize();
  s = createPidFile(config.pidfile);
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to create pidfile: " << s.Msg();
    exit(1);
  }

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
    removePidFile(config.pidfile);
    exit(1);
  }
  srv = new Server(&storage, &config);
  hup_handler = [] {
    if (!srv->IsStopped()) {
      LOG(INFO) << "Bye Bye";
      srv->Stop();
    }
  };
  s = srv->Start();
  if (!s.IsOK()) {
    removePidFile(config.pidfile);
    exit(1);
  }
  srv->Join();

  delete srv;
  removePidFile(config.pidfile);
  google::ShutdownGoogleLogging();
  libevent_global_shutdown();
  return 0;
}
