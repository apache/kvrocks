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
#include <jemalloc/jemalloc.h>
#include <unistd.h>
#endif

inline constexpr char errProfilingUnsupported[] = "memory profiling is not supported in this build";

#ifdef ENABLE_JEMALLOC
template <typename T>
Status SetJemallocOption(const char *name, T value) {
  T old_value;
  size_t old_value_size = sizeof(T);
  int ret = mallctl(name, &old_value, &old_value_size, reinterpret_cast<void *>(&value), sizeof(T));
  if (ret != 0) {
    return {Status::NotOK, fmt::format("unable to set the jemalloc option: {}, error: {}", name, strerror(errno))};
  }
  return Status::OK();
}

template <typename T>
Status GetJemallocOption(const char *name, T *value) {
  size_t value_size = sizeof(T);
  if (mallctl(name, value, &value_size, nullptr, 0) != 0) {
    return {Status::NotOK, fmt::format("unable to get the jemalloc option: {}, error: {}", name, strerror(errno))};
  }
  return Status::OK();
}

Status CheckIfProfilingEnabled() {
  bool enabled = false;
  size_t enabled_size = sizeof(enabled);
  if (mallctl("opt.prof", &enabled, &enabled_size, nullptr, 0) != 0) {
    return {Status::NotOK, fmt::format("unable to check if profiling is enabled: {}", strerror(errno))};
  }
  if (!enabled) {
    return {Status::NotOK,
            fmt::format("jemalloc profiling isn't enabled, please run Kvrocks with following environments: `{}`",
                        "export MALLOC_CONF=\"prof:true,background_thread:true\"")};
  }
  return Status::OK();
}
#endif

std::string MemoryProfiler::AllocatorName() {
#ifdef ENABLE_JEMALLOC
  return "jemalloc";
#else
  return "libc";
#endif
}

Status MemoryProfiler::SetProfiling(bool enabled) {
#ifdef ENABLE_JEMALLOC
  if (auto s = CheckIfProfilingEnabled(); !s.IsOK()) {
    return s;
  }
  return SetJemallocOption("prof.active", enabled);
#else
  (void)enabled;
  return {Status::NotOK, errProfilingUnsupported};
#endif
}

StatusOr<bool> MemoryProfiler::GetProfilingStatus() {
#ifdef ENABLE_JEMALLOC
  bool is_prof_enabled = false;
  if (auto s = GetJemallocOption("opt.prof", &is_prof_enabled); !s.IsOK()) {
    return s;
  }
  if (!is_prof_enabled) return false;

  bool is_prof_active = false;
  if (auto s = GetJemallocOption("prof.active", &is_prof_active); !s.IsOK()) {
    return s;
  }
  return is_prof_active;
#else
  return {Status::NotOK, errProfilingUnsupported};
#endif
}

Status MemoryProfiler::Dump(std::string_view dir) {
#ifdef ENABLE_JEMALLOC
  if (auto s = CheckIfProfilingEnabled(); !s.IsOK()) {
    return s;
  }

  bool is_prof_active = false;
  if (auto s = GetJemallocOption("prof.active", &is_prof_active); !s.IsOK()) {
    return s;
  }
  if (!is_prof_active) {
    return {Status::NotOK, "jemalloc profiling is not active, please enable it first"};
  }

  char *prefix_buffer = nullptr;
  size_t prefix_size = sizeof(prefix_buffer);
  int ret = mallctl("opt.prof_prefix", &prefix_buffer, &prefix_size, nullptr, 0);
  if (!ret && std::string_view(prefix_buffer) != "jeprof") {
    mallctl("prof.dump", nullptr, nullptr, nullptr, 0);
    return Status::OK();
  }

  static std::atomic<size_t> profile_counter{0};
  std::string dump_path = fmt::format("{}/jeprof.{}.{}.heap", dir, getpid(), profile_counter.fetch_add(1));
  const auto *dump_path_str = dump_path.c_str();
  mallctl("prof.dump", nullptr, nullptr, &dump_path_str, sizeof(dump_path_str));
  return Status::OK();
#else
  (void)dir;
  return {Status::NotOK, errProfilingUnsupported};
#endif
}
