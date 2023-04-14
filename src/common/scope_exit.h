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

#include <utility>

// ref to https://en.cppreference.com/w/cpp/experimental/scope_exit
template <typename F>
struct ScopeExit {
  explicit ScopeExit(F f, bool enabled) : enabled(enabled), f(std::move(f)) {}
  explicit ScopeExit(F f) : enabled(true), f(std::move(f)) {}

  ScopeExit(const ScopeExit&) = delete;
  ScopeExit(ScopeExit&& se) : enabled(se.enabled), f(std::move(se.f)) {}

  ScopeExit& operator=(const ScopeExit&) = delete;

  ~ScopeExit() {
    if (enabled) f();
  }

  void Enable() { enabled = true; }

  void Disable() { enabled = false; }

  bool enabled;
  F f;
};

// use CTAD in C++17 or above
template <typename F>
ScopeExit<F> MakeScopeExit(F&& f, bool enabled = true) {
  return ScopeExit<F>(std::forward<F>(f), enabled);
}
