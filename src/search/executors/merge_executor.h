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

#include <variant>

#include "search/plan_executor.h"

namespace kqir {

struct MergeExecutor : ExecutorNode {
  Merge *merge;
  decltype(merge->ops)::iterator iter;

  MergeExecutor(ExecutorContext *ctx, Merge *merge) : ExecutorNode(ctx), merge(merge), iter(merge->ops.begin()) {}

  StatusOr<Result> Next() override {
    if (iter == merge->ops.end()) {
      return end;
    }

    auto v = GET_OR_RET(ctx->Get(*iter)->Next());
    while (std::holds_alternative<End>(v)) {
      iter++;
      if (iter == merge->ops.end()) {
        return end;
      }

      v = GET_OR_RET(ctx->Get(*iter)->Next());
    }

    return v;
  }
};

}  // namespace kqir
