#include <gtest/gtest.h>
#include <gflags/gflags.h>

int main(int argc, char **argv) {
  gflags::SetUsageMessage("kvrocks unittest");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
