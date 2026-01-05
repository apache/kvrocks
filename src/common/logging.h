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

#include "spdlog/spdlog.h"

// just like std::source_location::current() in C++20 and __builtin_source_location(),
// but works in lower version compilers (GCC and Clang)
// NOTE: even now we use C++20, it is not supported until libstdc++ 11 and libc++ 16.
inline constexpr spdlog::source_loc CurrentLocation(const char *filename = __builtin_FILE(),
                                                    int lineno = __builtin_LINE(),
                                                    const char *funcname = __builtin_FUNCTION()) {
  return {filename, lineno, funcname};
}

// NOLINTNEXTLINE
#define LOG(...) spdlog::log(CurrentLocation(), __VA_ARGS__)

// NOLINTNEXTLINE
#define DEBUG(...) spdlog::log(CurrentLocation(), spdlog::level::debug, __VA_ARGS__)

// NOLINTNEXTLINE
#define INFO(...) spdlog::log(CurrentLocation(), spdlog::level::info, __VA_ARGS__)

// NOLINTNEXTLINE
#define WARN(...) spdlog::log(CurrentLocation(), spdlog::level::warn, __VA_ARGS__)

// NOLINTNEXTLINE
#define ERROR(...) spdlog::log(CurrentLocation(), spdlog::level::err, __VA_ARGS__)

// NOLINTNEXTLINE
#define FATAL(...) (spdlog::log(CurrentLocation(), spdlog::level::critical, __VA_ARGS__), std::abort())

// NOLINTNEXTLINE
#define UNREACHABLE(...) FATAL("UNREACHABLE REACHED: please submit a bug report with the stacktrace below.")

// NOLINTNEXTLINE
#define CHECK(cond) \
  if (!(cond)) FATAL("Check `{}` failed.", #cond);
