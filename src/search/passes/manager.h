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

#include <memory>
#include <utility>

#include "search/ir.h"
#include "search/ir_pass.h"
#include "search/passes/push_down_not_expr.h"
#include "search/passes/simplify_and_or_expr.h"
#include "search/passes/simplify_boolean.h"

namespace kqir {

struct PassManager {
  template <typename... PN>
  static std::unique_ptr<Node> Execute(std::unique_ptr<Node> node) {
    return executeImpl<PN...>(std::move(node), std::make_index_sequence<sizeof...(PN)>{});
  }

  static constexpr auto Default = Execute<SimplifyAndOrExpr, PushDownNotExpr, SimplifyBoolean>;

 private:
  template <typename... PN, size_t... I>
  static std::unique_ptr<Node> executeImpl(std::unique_ptr<Node> node, std::index_sequence<I...>) {
    std::tuple<PN...> passes;

    return std::move(((node = std::get<I>(passes).Transform(std::move(node))), ...));
  }
};

}  // namespace kqir
