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

#pragma once

#include <unistd.h>

#include <atomic>
#include <map>
#include <string>
#include <vector>

enum StatsMetricFlags {
  STATS_METRIC_COMMAND = 0,       // Number of commands executed
  STATS_METRIC_NET_INPUT,         // Bytes read to network
  STATS_METRIC_NET_OUTPUT,        // Bytes written to network
  STATS_METRIC_ROCKSDB_PUT,       // Number of calls of Put and Write in rocksdb
  STATS_METRIC_ROCKSDB_GET,       // Number of calls of get in rocksdb
  STATS_METRIC_ROCKSDB_MULTIGET,  // Number of calls of mulget in rocksdb
  STATS_METRIC_ROCKSDB_SEEK,      // Number of calls of seek in rocksdb
  STATS_METRIC_ROCKSDB_NEXT,      // Number of calls of next in rocksdb
  STATS_METRIC_ROCKSDB_PREV,      // Number of calls of prev in rocksdb
  STATS_METRIC_COUNT
};

const int STATS_METRIC_SAMPLES = 16;  // Number of samples per metric

struct command_stat {
  std::atomic<uint64_t> calls;
  std::atomic<uint64_t> latency;
};

struct inst_metric {
  uint64_t last_sample_time;   // Timestamp of the last sample in ms
  uint64_t last_sample_count;  // Count in the last sample
  uint64_t samples[STATS_METRIC_SAMPLES];
  int idx;
};

class Stats {
 public:
  std::atomic<uint64_t> total_calls = {0};
  std::atomic<uint64_t> in_bytes = {0};
  std::atomic<uint64_t> out_bytes = {0};
  std::vector<struct inst_metric> inst_metrics;

  std::atomic<uint64_t> fullsync_counter = {0};
  std::atomic<uint64_t> psync_err_counter = {0};
  std::atomic<uint64_t> psync_ok_counter = {0};
  std::map<std::string, command_stat> commands_stats;

  Stats();
  void IncrCalls(const std::string &command_name);
  void IncrLatency(uint64_t latency, const std::string &command_name);
  void IncrInbondBytes(uint64_t bytes) { in_bytes.fetch_add(bytes, std::memory_order_relaxed); }
  void IncrOutbondBytes(uint64_t bytes) { out_bytes.fetch_add(bytes, std::memory_order_relaxed); }
  void IncrFullSyncCounter() { fullsync_counter.fetch_add(1, std::memory_order_relaxed); }
  void IncrPSyncErrCounter() { psync_err_counter.fetch_add(1, std::memory_order_relaxed); }
  void IncrPSyncOKCounter() { psync_ok_counter.fetch_add(1, std::memory_order_relaxed); }
  static int64_t GetMemoryRSS();
  void TrackInstantaneousMetric(int metric, uint64_t current_reading);
  uint64_t GetInstantaneousMetric(int metric);
};
