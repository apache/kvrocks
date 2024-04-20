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

using PassSequence = std::vector<std::unique_ptr<Pass>>;

struct PassManager {
  static std::unique_ptr<Node> Execute(const PassSequence &seq, std::unique_ptr<Node> node) {
    for (auto &pass : seq) {
      node = pass->Transform(std::move(node));
    }
    return node;
  }

  template <typename... Passes>
  static PassSequence GeneratePasses() {
    PassSequence result;
    (result.push_back(std::make_unique<Passes>()), ...);
    return result;
  }

  static PassSequence ExprPasses() { return GeneratePasses<SimplifyAndOrExpr, PushDownNotExpr, SimplifyBoolean>(); }
};

}  // namespace kqir
