#ifndef KVROCKS_STATS_H
#define KVROCKS_STATS_H

#include <map>
#include <string>
#include <unistd.h>

class Stats {
 public:
  std::atomic<uint64_t> calls = {0};
  std::atomic<uint64_t> in_bytes = {0};
  std::atomic<uint64_t> out_bytes = {0};

 public:
  void incr_calls() { calls.fetch_add(1, std::memory_order_relaxed);}
  void incr_in_byte(uint64_t bytes) { in_bytes.fetch_add(bytes, std::memory_order_relaxed); }
  void incr_out_byte(uint64_t bytes) { out_bytes.fetch_add(bytes, std::memory_order_relaxed); }
  static long GetMemoryRSS();
  static void GetRocksdbStats(std::map<std::string, std::string> *stats);
};

#endif //KVROCKS_STATS_H