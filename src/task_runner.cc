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

#include "task_runner.h"

#include <thread>
#include "util.h"

Status TaskRunner::Publish(const Task &task) {
  std::lock_guard<std::mutex> guard(mu_);
  if (stop_) {
    return Status(Status::NotOK, "the runner was stopped");
  }
  if (task_queue_.size() >= max_queue_size_) {
    return Status(Status::NotOK, "the task queue was reached max length");
  }
  task_queue_.emplace_back(task);
  cond_.notify_all();
  return Status::OK();
}

void TaskRunner::Start() {
  stop_ = false;
  for (int i = 0; i < n_thread_; i++) {
    threads_.emplace_back(std::thread([this]() {
      Util::ThreadSetName("task-runner");
      this->run();
    }));
  }
}

void TaskRunner::Stop() {
  std::lock_guard<std::mutex> guard(mu_);
  stop_ = true;
  cond_.notify_all();
}

void TaskRunner::Join() {
  for (size_t i = 0; i < threads_.size(); i++) {
    if (threads_[i].joinable()) threads_[i].join();
  }
}

void TaskRunner::Purge() {
  std::lock_guard<std::mutex> guard(mu_);
  threads_.clear();
  task_queue_.clear();
}

void TaskRunner::run() {
  Task task;
  std::unique_lock<std::mutex> lock(mu_);
  while (!stop_) {
    cond_.wait(lock, [this]() -> bool { return stop_ || !task_queue_.empty();});
    while (!stop_ && !task_queue_.empty()) {
      task = task_queue_.front();
      task_queue_.pop_front();
      lock.unlock();
      if (task) task();
      lock.lock();
    }
  }
  task_queue_.clear();
  lock.unlock();
  // CAUTION: drop the rest of tasks, don't use task runner if the task can't be drop
}
