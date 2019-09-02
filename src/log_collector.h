#pragma once

#include <time.h>

#include <string>
#include <list>
#include <vector>
#include <mutex>
#include <cstdint>
#include <functional>

class SlowEntry {
 public:
  uint64_t id;
  time_t time;
  uint64_t duration;
  std::vector<std::string> args;

 public:
  std::string ToRedisString();
};

class PerfEntry {
 public:
  uint64_t id;
  time_t time;
  uint64_t duration;
  std::string cmd_name;
  std::string perf_context;
  std::string iostats_context;

 public:
  std::string ToRedisString();
};

template <class T>
class LogCollector {
 public:
  ~LogCollector();
  ssize_t Size();
  void Reset();
  void SetMaxEntries(int64_t max_entries);
  void PushEntry(T *entry);
  std::string GetLatestEntries(int64_t cnt);

 private:
  std::mutex mu_;
  uint64_t id_ = 0;
  int64_t max_entries_ = 128;
  std::list<T*> entries_;
};
