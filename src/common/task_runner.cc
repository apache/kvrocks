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

#include "oneapi/tbb/concurrent_queue.h"
#include "thread_util.h"

Status TaskRunner::Start() {
  if (state_ != Stopped) {
    return {Status::NotOK, "Task runner is expected to stop before starting"};
  }

  state_ = Running;
  for (auto &thread : threads_) {
    thread = GET_OR_RET(Util::CreateThread("task-runner", [this] { run(); }));
  }

  return Status::OK();
}

Status TaskRunner::Join() {
  if (state_ == Stopped) {
    return {Status::NotOK, "Task runner is expected to start before joining"};
  }

  for (auto &thread : threads_) {
    if (auto s = Util::ThreadJoin(thread); !s) {
      return s.Prefixed("Task thread operation failed");
    }
  }

  task_queue_.clear();
  state_ = Stopped;

  return Status::OK();
}

void TaskRunner::run() {
  while (state_ == Running) {
    Task task;

    try {
      task_queue_.pop(task);
    } catch (tbb::user_abort &e) {
      break;
    }

    if (state_ != Running) break;

    if (task) task();
  }
}
