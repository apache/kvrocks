#include <event2/thread.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <csignal>

#include "server.h"
#include "storage.h"
#include "version.h"

DEFINE_string(dbpath, "/tmp/ev", "db path");
DEFINE_string(bkpath, "/tmp/ev_bk", "db backup path");
DEFINE_uint32(port, 3333, "listening port");

std::function<void()> hup_handler;

extern "C" void signal_handler(int sig) {
  if (hup_handler) hup_handler();
}

int main(int argc, char* argv[]) {
  FLAGS_logtostderr = true;
  FLAGS_minloglevel = 0;
  google::InitGoogleLogging("ev");
  gflags::SetUsageMessage("A Useless Server");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  evthread_use_pthreads();

  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, signal_handler);

  LOG(INFO) << "Version: " << VERSION << " @" << GIT_COMMIT;
  Engine::Storage storage(FLAGS_dbpath, FLAGS_bkpath);
  auto s = storage.Open();
  if (!s.IsOK()) {
    LOG(ERROR) << "failed to open: " << s.msg();
    exit(1);
  }
  std::vector<Server*> servers;
  for (int i = 0; i < 2; ++i) {
    servers.push_back(new Server(&storage, FLAGS_port));
  }
  hup_handler = [&servers]() {
    LOG(INFO) << "bye bye";
    for (auto svr : servers) {
      svr->Stop();
    }
  };
  std::vector<ServerThread* > threads;
  for (auto svr : servers) {
    threads.push_back(new ServerThread(svr));
    threads.back()->Start();
  }
  for (auto t : threads) {
    t->Join();
  }
  return 0;
}
