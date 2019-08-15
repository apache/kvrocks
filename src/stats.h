#pragma once

#include <unistd.h>
#include <map>
#include <atomic>
#include <string>

struct command_stat {
  std::atomic<uint64_t> calls;
  std::atomic<uint64_t> latency;
};

class Stats {
 public:
  std::atomic<uint64_t> total_calls = {0};
  std::atomic<uint64_t> in_bytes = {0};
  std::atomic<uint64_t> out_bytes = {0};

  std::atomic<uint64_t> fullsync_counter = {0};
  std::atomic<uint64_t> psync_err_counter = {0};
  std::atomic<uint64_t> psync_ok_counter = {0};
  std::map<std::string, command_stat> commands_stats;

 public:
  void IncrCalls(const std::string &command_name);
  void IncrLatency(uint64_t latency, const std::string &command_name);
  void IncrInbondBytes(uint64_t bytes) { in_bytes.fetch_add(bytes, std::memory_order_relaxed); }
  void IncrOutbondBytes(uint64_t bytes) { out_bytes.fetch_add(bytes, std::memory_order_relaxed); }
  void IncrFullSyncCounter() { fullsync_counter.fetch_add(1, std::memory_order_relaxed); }
  void IncrPSyncErrCounter() { psync_err_counter.fetch_add(1, std::memory_order_relaxed); }
  void IncrPSyncOKCounter() { psync_ok_counter.fetch_add(1, std::memory_order_relaxed); }
  static int64_t GetMemoryRSS();
};
