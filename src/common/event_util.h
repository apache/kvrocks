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
#include <memory>
#include <utility>

#include "event2/buffer.h"

template <typename F, F *f>
struct StaticFunction {
  template <typename... Ts>
  auto operator()(Ts &&...args) const -> decltype(f(std::forward<Ts>(args)...)) {  // NOLINT
    return f(std::forward<Ts>(args)...);                                           // NOLINT
  }
};

using StaticFree = StaticFunction<decltype(std::free), std::free>;

template <typename T>
struct UniqueFreePtr : std::unique_ptr<T, StaticFree> {
  using base_type = std::unique_ptr<T, StaticFree>;

  using base_type::base_type;
};

struct UniqueEvbufReadln : UniqueFreePtr<char[]> {
  UniqueEvbufReadln(evbuffer *buffer, evbuffer_eol_style eol_style)
      : UniqueFreePtr(evbuffer_readln(buffer, &length, eol_style)) {}

  size_t length;
};

using StaticEvbufFree = StaticFunction<decltype(evbuffer_free), evbuffer_free>;

struct UniqueEvbuf : std::unique_ptr<evbuffer, StaticEvbufFree> {
  using base_type = std::unique_ptr<evbuffer, StaticEvbufFree>;

  UniqueEvbuf() : base_type(evbuffer_new()) {}
  explicit UniqueEvbuf(evbuffer *buffer) : base_type(buffer) {}
};
