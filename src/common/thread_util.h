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

#include <system_error>
#include <thread>

#include "fmt/core.h"
#include "status.h"

namespace util {

void ThreadSetName(const char *name);

template <typename F>
StatusOr<std::thread> CreateThread(const char *name, F f) {
  try {
    return std::thread([name, f = std::move(f)] {
      ThreadSetName(name);
      f();
    });
  } catch (const std::system_error &e) {
    return {Status::NotOK, fmt::format("thread '{}' cannot be started: {}", name, e.what())};
  }
}

Status ThreadJoin(std::thread &t);
Status ThreadDetach(std::thread &t);

}  // namespace util
