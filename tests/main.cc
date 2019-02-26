#include <gtest/gtest.h>
#include <gflags/gflags.h>

int main(int argc, char **argv) {
  gflags::SetUsageMessage("kvrocks unittest");
  testing::InitGoogleTest(&argc, argv);
  testing::FLAGS_gtest_filter = "RedisBitmap*";
  return RUN_ALL_TESTS();
}
