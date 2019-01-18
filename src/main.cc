#include <getopt.h>
#include <event2/thread.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <csignal>

#include "worker.h"
#include "storage.h"
#include "version.h"
#include "config.h"
#include "server.h"
#include "util.h"

const char *kDefaultConfPath = "../kvrocks.conf";

std::function<void()> hup_handler;

struct Options {
  std::string conf_file = kDefaultConfPath;
  bool show_usage = false;
};

extern "C" void signal_handler(int sig) {
  if (hup_handler) hup_handler();
}

static void usage(const char* program) {
  std::cout << program << " implements the Redis protocol based on rocksdb\n"
            << "\t-c config file, default is " << kDefaultConfPath << "\n"
            << "\t-h help\n";
  exit(0);
}

static Options *parseCommandLineOptions(int argc, char **argv) {
  int ch;
  auto opts = new Options();
  while((ch = ::getopt(argc, argv, "c:h")) != -1) {
    switch (ch) {
      case 'c': opts->conf_file = optarg; break;
      case 'h': opts->show_usage = true; break;
      default: break;
    }
  }
  return opts;
}

static void initGoogleLog(const Config *config) {
  FLAGS_minloglevel = config->loglevel;
  FLAGS_max_log_size = 100;
  FLAGS_logbufsecs = 0;
  FLAGS_log_dir = config->dir;
}

static void daemonize() {
  pid_t pid;

  pid = fork();
  if (pid < 0) {
    LOG(ERROR) << "Failed to fork the process, err: " << strerror(errno);
    exit(1);
  }
  if (pid > 0) exit(EXIT_SUCCESS); // parent process
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
  gflags::SetUsageMessage("kvrocks");
  evthread_use_pthreads();

  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);

  std::cout << "Version: " << VERSION << " @" << GIT_COMMIT << std::endl;
  auto opts = parseCommandLineOptions(argc, argv);
  if (opts->show_usage) usage(argv[0]);
  std::string config_file_path = std::move(opts->conf_file);
  delete opts;

  Config config;
  Status s = config.Load(config_file_path);
  if (!s.IsOK()) {
    std::cout << "Failed to load config, err: " << s.Msg() << std::endl;
    exit(1);
  }
  initGoogleLog(&config);

  // Tricky: We don't expect that different instances running on the same port,
  // but the server use REUSE_PORT to support the multi listeners. So we connect
  // the listen port to check if the port has already listened or not.
  if (Util::IsPortInUse(config.port)) {
    std::cout << "Failed to start the server, the specified port["
              << config.port << "] is already in use" << std::endl;
    exit(1);
  }

  if (config.daemonize) daemonize();
  Engine::Storage storage(&config);
  s = storage.Open();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to open: " << s.Msg();
    exit(1);
  }

  Server svr(&storage, &config);
  hup_handler = [&svr]() {
    if (!svr.IsStopped()) {
      LOG(INFO) << "bye bye";
      svr.Stop();
    }
  };
  svr.Start();
  svr.Join();
  return 0;
}
