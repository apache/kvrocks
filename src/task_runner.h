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

#include <cstdint>
#include <vector>
#include <list>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

#include "status.h"

using Task = std::function<void()>;

class TaskRunner {
 public:
  explicit TaskRunner(int n_thread = 1, uint32_t max_queue_size = 10240)
  :max_queue_size_(max_queue_size), n_thread_(n_thread) {}
  ~TaskRunner() = default;
  Status Publish(const Task &task);
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
