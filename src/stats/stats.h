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
  STATS_METRIC_BLOCK_CACHE_MISS,
  STATS_METRIC_BLOCK_CACHE_HIT,
  STATS_METRIC_BLOCK_CACHE_ADD,
  STATS_METRIC_BLOCK_CACHE_BYTES_READ,
  STATS_METRIC_BLOCK_CACHE_BYTES_WRITE,
  STATS_METRIC_BLOOM_FILTER_USEFUL,
  STATS_METRIC_BLOOM_FILTER_FULL_POSITIVE,
  STATS_METRIC_BLOOM_FILTER_FULL_TRUE_POSITIVE,
  STATS_METRIC_PERSISTENT_CACHE_HIT,
  STATS_METRIC_PERSISTENT_CACHE_MISS,
  STATS_METRIC_MEMTABLE_HIT,
  STATS_METRIC_MEMTABLE_MISS,
  STATS_METRIC_GET_HIT_L0,
  STATS_METRIC_GET_HIT_L1,
  STATS_METRIC_GET_HIT_L2_AND_UP,
  STATS_METRIC_COMPACTION_KEY_DROP_NEWER_ENTRY,         // key was written with a newer value.
                                                        // Also includes keys dropped for range del.
  STATS_METRIC_COMPACTION_KEY_DROP_OBSOLETE,            // The key is obsolete.
  STATS_METRIC_COMPACTION_KEY_DROP_RANGE_DEL,           // key was covered by a range tombstone.
  STATS_METRIC_COMPACTION_KEY_DROP_USER,                // user compaction function has dropped the key.
  STATS_METRIC_COMPACTION_RANGE_DEL_DROP_OBSOLETE,      // all keys in range were deleted.
  STATS_METRIC_COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE,  // Deletions obsoleted before bottom level due to file gap
                                                        // optimization.
  STATS_METRIC_COMPACTION_CANCELLED,                    // If a compaction was canceled in sfm to prevent ENOSPC
  STATS_METRIC_BYTES_WRITTEN,         // The number of uncompressed bytes issued by DB::Put(), DB::Delete(),
                                      // DB::Merge(), and DB::Write().
  STATS_METRIC_BYTES_READ,            // The number of uncompressed bytes read from DB::Get().  It could be
                                      // either from memtables, cache, or table files.
                                      // For the number of logical bytes read from DB::MultiGet(),
                                      // please use NUMBER_MULTIGET_BYTES_READ.
  STATS_METRIC_NUMBER_DB_SEEK_FOUND,  // The number of calls to seek/next/prev that returned data
  STATS_METRIC_NUMBER_DB_NEXT_FOUND,
  STATS_METRIC_NUMBER_DB_PREV_FOUND,

  STATS_METRIC_BLOOM_FILTER_PREFIX_CHECKED,
  STATS_METRIC_BLOOM_FILTER_PREFIX_USEFUL,

  STATS_METRIC_COMPACT_READ_BYTES,   // Bytes read during compaction
  STATS_METRIC_COMPACT_WRITE_BYTES,  // Bytes written during compaction
  STATS_METRIC_FLUSH_WRITE_BYTES,    // Bytes written during flush

  // Row cache.
  STATS_METRIC_ROW_CACHE_HIT,
  STATS_METRIC_ROW_CACHE_MISS,

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

 public:
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
