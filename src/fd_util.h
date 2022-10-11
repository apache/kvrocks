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

#include <unistd.h>
#include <utility>

constexpr const int NullFD = -1;

// just like an unique_ptr, but for (int) fd
struct UniqueFD {
  UniqueFD() : fd_(NullFD) {}
  explicit UniqueFD(int fd) : fd_(fd) {}

  ~UniqueFD() { if (fd_ != NullFD) close(fd_); }

  UniqueFD(const UniqueFD&) = delete;
  UniqueFD(UniqueFD&& f) : fd_(f.fd_) { f.fd_ = NullFD; }

  UniqueFD& operator=(const UniqueFD&) = delete;
  UniqueFD& operator=(UniqueFD&& f) {
    fd_ = f.fd_;
    f.fd_ = NullFD;
    return *this;
  }

  int Release() {
    int fd = fd_;
    fd_ = NullFD;
    return fd;
  }

  void Reset(int fd = NullFD) {
    int old_fd = fd_;
    fd_ = fd;
    if (old_fd != NullFD) close(old_fd);
  }

  void Close() {
    Reset();
  }

  void Swap(UniqueFD& other) {
    std::swap(fd_, other.fd_);
  }

  int Get() const { return fd_; }
  int operator*() const { return fd_; }
  explicit operator bool() const { return fd_ != NullFD; }

  bool operator==(const UniqueFD& f) const {
    return fd_ == f.fd_;
  }

  bool operator!=(const UniqueFD& f) const {
    return !(*this == f);
  }

 private:
  int fd_;
};


// ref to https://en.cppreference.com/w/cpp/experimental/scope_exit
template <typename F>
struct ScopeExit {
  explicit ScopeExit(F f, bool enabled) : enabled(enabled), f(std::move(f)) {}
  explicit ScopeExit(F f) : enabled(true), f(std::move(f)) {}

  ScopeExit(const ScopeExit&) = delete;
  ScopeExit(ScopeExit&& se) : enabled(se.enabled), f(std::move(se.f)) {}

  ~ScopeExit() {
    if (enabled) f();
  }

  void Enable() {
    enabled = false;
  }

  void Disable() {
    enabled = true;
  }

  bool enabled;
  F f;
};

// use CTAD in C++17 or above
template <typename F>
ScopeExit<F> MakeScopeExit(F&& f, bool enabled = true) {
  return ScopeExit<F>(std::forward<F>(f), enabled);
}
