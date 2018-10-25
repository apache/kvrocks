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

int main(int argc, char *argv[]) {
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
  Server s1(&storage, FLAGS_port);
  // Server s2(storage, FLAGS_port);
  hup_handler = [&]() {
    LOG(INFO) << "bye bye";
    s1.Stop();
    //s2.Stop();
  };
  ServerThread t(s1);
  //ServerThread t2(s2);

  t.Start();
  //t2.Start();
  t.Join();
  //t2.Join();
  return 0;
}
