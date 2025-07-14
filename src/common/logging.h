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

#include <cstdlib>
#include <iterator>

#include "fmt/base.h"
#include "fmt/ostream.h"
#include "spdlog/common.h"
#include "spdlog/spdlog.h"

// just like std::source_location::current() in C++20 and __builtin_source_location(),
// but works in lower version compilers (GCC and Clang)
inline constexpr spdlog::source_loc CurrentLocation(const char *filename = __builtin_FILE(),
                                                    int lineno = __builtin_LINE(),
                                                    const char *funcname = __builtin_FUNCTION()) {
  return {filename, lineno, funcname};
}

struct FormatMessageWithLoc {
  template <typename T>
  constexpr FormatMessageWithLoc(const T &v, spdlog::source_loc loc = CurrentLocation())  // NOLINT
      : fmt(v), current_loc(loc) {}

  std::string_view fmt;
  spdlog::source_loc current_loc;
};

template <typename... Args>
inline void log(spdlog::level::level_enum lvl, FormatMessageWithLoc fmt, Args &&...args) {  // NOLINT
  spdlog::default_logger_raw()->log(fmt.current_loc, lvl, fmt.fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void debug(FormatMessageWithLoc fmt, Args &&...args) {  // NOLINT
  spdlog::default_logger_raw()->log(fmt.current_loc, spdlog::level::debug, fmt.fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void info(FormatMessageWithLoc fmt, Args &&...args) {  // NOLINT
  spdlog::default_logger_raw()->log(fmt.current_loc, spdlog::level::info, fmt.fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void warn(FormatMessageWithLoc fmt, Args &&...args) {  // NOLINT
  spdlog::default_logger_raw()->log(fmt.current_loc, spdlog::level::warn, fmt.fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void error(FormatMessageWithLoc fmt, Args &&...args) {  // NOLINT
  spdlog::default_logger_raw()->log(fmt.current_loc, spdlog::level::err, fmt.fmt, std::forward<Args>(args)...);
}

template <typename... Args>
[[noreturn]] inline void fatal(FormatMessageWithLoc fmt, Args &&...args) {  // NOLINT
  spdlog::default_logger_raw()->log(fmt.current_loc, spdlog::level::critical, fmt.fmt, std::forward<Args>(args)...);
  std::abort();
}

[[noreturn]] inline void unreachable(spdlog::source_loc loc = CurrentLocation()) {  // NOLINT
  fatal({"UNREACHABLE REACHED: please submit a bug report with the stacktrace below.", loc});
}

// NOLINTNEXTLINE
#define CHECK(cond) \
  if (!(cond)) fatal("Check `{}` failed.", #cond);
