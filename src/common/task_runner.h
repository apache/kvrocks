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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <thread>

#include "oneapi/tbb/concurrent_queue.h"
#include "status.h"

using Task = std::function<void()>;

class TaskRunner {
 public:
  static constexpr uint32_t default_n_threads = 1;
  static constexpr uint32_t default_max_queue_size = 10240;

  explicit TaskRunner(size_t n_threads = default_n_threads, ptrdiff_t max_queue_size = default_max_queue_size)
      : threads_(n_threads) {
    task_queue_.set_capacity(max_queue_size);
  }

  TaskRunner(const TaskRunner&) = delete;
  TaskRunner& operator=(const TaskRunner&) = delete;

  ~TaskRunner() {
    if (state_ != Stopped) {
      Cancel();
      auto _ = Join();
    }
  }

  template <typename T>
  void Publish(T&& task) {
    task_queue_.push(std::forward<T>(task));
  }

  template <typename T>
  Status TryPublish(T&& task) {
    if (!task_queue_.try_push(std::forward<T>(task))) {
      return {Status::NotOK, "Task number limit is exceeded"};
    }

    return Status::OK();
  }

  size_t Size() { return task_queue_.size(); }
  void Cancel() {
    state_ = Stopping;
    task_queue_.abort();
  }

  Status Start();
  Status Join();

 private:
  void run();

  enum State { Running, Stopping, Stopped };

  std::atomic<State> state_ = Stopped;
  tbb::concurrent_bounded_queue<Task> task_queue_;
  std::vector<std::thread> threads_;
};
