#include <event2/thread.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <csignal>

#include "worker.h"
#include "storage.h"
#include "version.h"
#include "config.h"
#include "server.h"

DEFINE_string(conf_path, "../kvrocks.conf", "config file");

std::function<void()> hup_handler;

extern "C" void signal_handler(int sig) {
  if (hup_handler) hup_handler();
}

int main(int argc, char* argv[]) {
  google::InitGoogleLogging("ev");
  gflags::SetUsageMessage("A Useless Worker");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  evthread_use_pthreads();

  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, signal_handler);

  LOG(INFO) << "Version: " << VERSION << " @" << GIT_COMMIT;
  Config config;
  std::string err;
  if (!config.Load(FLAGS_conf_path, &err)) {
    LOG(ERROR) << "Failed to load config, err: " << err;
    exit(1);
  }
  FLAGS_logtostderr = true;
  FLAGS_minloglevel = config.loglevel;

  Engine::Storage storage(&config);
  auto s = storage.Open();
  if (!s.IsOK()) {
    LOG(ERROR) << "failed to open: " << s.msg();
    exit(1);
  }

  Server svr(&storage, &config);
  hup_handler = [&svr]() {
    LOG(INFO) << "bye bye";
    svr.Stop();
  };
  svr.Start();
  svr.Join();
  return 0;
}
