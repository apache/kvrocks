#include <gtest/gtest.h>

#include "log_collector.h"

TEST(LogCollector, PushEntry) {
  LogCollector<PerfEntry> perf_log;
  perf_log.SetMaxEntries(1);
  perf_log.PushEntry(new PerfEntry());
  perf_log.PushEntry(new PerfEntry());
  EXPECT_EQ(perf_log.Size(), 1);
  perf_log.SetMaxEntries(2);
  perf_log.PushEntry(new PerfEntry());
  perf_log.PushEntry(new PerfEntry());
  EXPECT_EQ(perf_log.Size(), 2);
  perf_log.Reset();
  EXPECT_EQ(perf_log.Size(), 0);
}
