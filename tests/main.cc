#include <gtest/gtest.h>

#include "server.h"

Server *GetServer() {
  return nullptr;
}

int main(int argc, char **argv) {
//  gflags::SetUsageMessage("kvrocks unittest");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
