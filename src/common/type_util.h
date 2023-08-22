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

template <typename F, F *f>
struct StaticFunction {
  template <typename... Ts>
  auto operator()(Ts &&...args) const -> decltype(f(std::forward<Ts>(args)...)) {  // NOLINT
    return f(std::forward<Ts>(args)...);                                           // NOLINT
  }
};

template <typename... Ts>
using FirstElement = typename std::tuple_element_t<0, std::tuple<Ts...>>;

template <typename T>
using RemoveCVRef = typename std::remove_cv_t<typename std::remove_reference_t<T>>;

// dependent false for static_assert with constexpr if, see CWG2518/P2593R1
template <typename T>
constexpr bool AlwaysFalse = false;
