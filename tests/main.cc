#include <gtest/gtest.h>

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  testing::FLAGS_gtest_filter = "Stats*";
  return RUN_ALL_TESTS();
}
