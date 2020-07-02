#pragma once

#include <cstdint>
#include <vector>
#include <list>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

#include "status.h"

struct Task {
  std::function<void(void*)> callback;
  void *arg;
};

class TaskRunner {
 public:
  explicit TaskRunner(int n_thread = 1, uint32_t max_queue_size = 10240)
  :max_queue_size_(max_queue_size), n_thread_(n_thread) {}
  ~TaskRunner() = default;
  Status Publish(Task task);
  size_t QueueSize() { return task_queue_.size(); }
  void Start();
  void Stop();
  void Join();
  void Purge();
 private:
  void run();
  bool stop_ = false;
  uint32_t max_queue_size_;
  std::list<Task> task_queue_;
  std::mutex mu_;
  std::condition_variable cond_;
  int n_thread_;
  std::vector<std::thread> threads_;
};
