/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "stats/log_collector.h"

#include <gtest/gtest.h>

TEST(LogCollector, PushEntry) {
  LogCollector<PerfEntry> perf_log;
  perf_log.SetMaxEntries(1);
  perf_log.PushEntry(std::make_unique<PerfEntry>());
  perf_log.PushEntry(std::make_unique<PerfEntry>());
  EXPECT_EQ(perf_log.Size(), 1);
  perf_log.SetMaxEntries(2);
  perf_log.PushEntry(std::make_unique<PerfEntry>());
  perf_log.PushEntry(std::make_unique<PerfEntry>());
  EXPECT_EQ(perf_log.Size(), 2);
  perf_log.Reset();
  EXPECT_EQ(perf_log.Size(), 0);
}
