#include <algorithm>
#include "log_collector.h"
#include "redis_reply.h"


std::string SlowEntry::ToRedisString() {
  std::string output;
  output.append(Redis::MultiLen(4));
  output.append(Redis::Integer(id));
  output.append(Redis::Integer(time));
  output.append(Redis::Integer(duration));
  output.append(Redis::MultiBulkString(args));
  return output;
}

std::string PerfEntry::ToRedisString() {
  std::string output;
  output.append(Redis::MultiLen(6));
  output.append(Redis::Integer(id));
  output.append(Redis::Integer(time));
  output.append(Redis::BulkString(cmd_name));
  output.append(Redis::Integer(duration));
  output.append(Redis::BulkString(perf_context));
  output.append(Redis::BulkString(iostats_context));
  return output;
}

template <class T>
LogCollector<T>::~LogCollector() {
  Reset();
}

template <class T>
ssize_t LogCollector<T>::Size() {
  size_t n;
  mu_.lock();
  n = entries_.size();
  mu_.unlock();
  return n;
}

template <class T>
void LogCollector<T>::Reset() {
  mu_.lock();
  while (!entries_.empty()) {
    delete entries_.front();
    entries_.pop_front();
  }
  mu_.unlock();
}

template <class T>
void LogCollector<T>::SetMaxEntries(int64_t max_entries) {
  mu_.lock();
  while (max_entries > 0 && static_cast<int64_t>(entries_.size()) > max_entries) {
    delete entries_.back();
    entries_.pop_back();
  }
  max_entries_ = max_entries;
  mu_.unlock();
}

template <class T>
void LogCollector<T>::PushEntry(T *entry) {
  mu_.lock();
  entry->id = ++id_;
  entry->time = time(nullptr);
  if (max_entries_ > 0 && !entries_.empty()
      && entries_.size() >= static_cast<size_t>(max_entries_)) {
    delete entries_.back();
    entries_.pop_back();
  }
  entries_.push_front(entry);
  mu_.unlock();
}

template <class T>
std::string LogCollector<T>::GetLatestEntries(int64_t cnt) {
  size_t n;
  std::string output;

  mu_.lock();
  if (cnt > 0) {
    n = std::min(entries_.size(), static_cast<size_t>(cnt));
  } else {
    n = entries_.size();
  }
  output.append(Redis::MultiLen(n));
  for (const auto &entry : entries_) {
    output.append(entry->ToRedisString());
    if (--n == 0) break;
  }
  mu_.unlock();
  return output;
}

template class LogCollector<SlowEntry>;
template class LogCollector<PerfEntry>;
