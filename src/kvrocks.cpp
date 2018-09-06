#include <iostream>
#include "server.h"
#include <glog/logging.h>

void initLogger() {
  // FIXME: remove this option before releasing
  FLAGS_alsologtostderr=1;
  ::google::InitGoogleLogging("kvrocks");
}

int main() {
  initLogger();
  LOG(INFO) << "server started";
  Server *server = new Server(4);
  server->start();
}
