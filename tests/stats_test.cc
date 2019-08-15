#include <gtest/gtest.h>

#include "stats.h"

TEST(Stats, GetMemoryRss) {
  Stats stats;
  std::cout << "rss: " << stats.GetMemoryRSS() << std::endl;
}
