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

#include "memory_profiler.h"

#ifdef ENABLE_JEMALLOC
#include <unistd.h>
#include <jemalloc/jemalloc.h>
#endif

#ifdef ENABLE_JEMALLOC
template <typename T>
Status setJemallocOption(const char *name, T value) {
  T old_value;
  size_t old_value_size = sizeof(T);
  int ret = mallctl(name, &old_value, &old_value_size, reinterpret_cast<void *>(&value), sizeof(T));
  if (ret != 0) {
    return {Status::NotOK, fmt::format("unable to set the jemalloc option: {}, error: {}", name, strerror(errno))};
  }
  return Status::OK();
}

template <typename T>
Status getJemallocOption(const char *name, T *value) {
  size_t value_size = sizeof(T);
  if (mallctl(name, value, &value_size, nullptr, 0) != 0) {
    return {Status::NotOK, fmt::format("unable to get the jemalloc option: {}, error: {}", name, strerror(errno))};
  }
  return Status::OK();
}

Status checkIfProfilingEnabled() {
  bool enabled = false;
  size_t value_size = sizeof(enabled);
  if (mallctl("opt.prof", &enabled, &enabled_size, nullptr, 0) != 0) {
    return {Status::NotOK, fmt::format("unable to check if profiling is enabled: {}", strerror(errno))};
  }
  if (!enabled) {
    return {Status::NotOK, "jemalloc profiling isn't enabled, please run Kvrocks with following enviroments: `{}`",
            "export MALLOC_CONF=\"prof:true,background_thread:true\""};
  }
  return Status::OK();
}
#endif

Status MemoryProfiler::SetProfiling(bool enabled) {
#ifdef ENABLE_JEMALLOC
  if (auto s = checkIfProfilingEnabled(); !s.IsOK()) {
    return s;
  }
  return setJemallocOption("prof.active", enabled);
#else
  (void)enabled;
  return {Status::NotOK, "memory profiling is not supported in this build"};
#endif
}

Status MemoryProfiler::Dump(std::string_view dir) const {
#ifdef ENABLE_JEMALLOC
  static std::atomic<size_t> profile_counter{0};
  std::string dump_path = fmt::format("{}/jeprof.{}.{}.heap", dir, getpid(), profile_counter.fetch_add(1));
  const auto *dump_path_str = dump_path.c_str();
  return setJemallocOption("prof.dump", dump_path_str);
#else
  (void)dir;
  return {Status::NotOK, "memory profiling is not supported in this build"};
#endif
}
