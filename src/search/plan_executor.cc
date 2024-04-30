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

#include "plan_executor.h"

#include <memory>

#include "search/executors/limit_executor.h"
#include "search/executors/merge_executor.h"
#include "search/executors/noop_executor.h"
#include "search/executors/sort_executor.h"

namespace kqir {

namespace details {

struct ExecutorContextVisitor {
  ExecutorContext *ctx;

  void Transform(PlanOperator *op) {
    if (auto v = dynamic_cast<Limit *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<Noop *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<Merge *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<Sort *>(op)) {
      return Visit(v);
    }

    CHECK(false) << "unreachable";
  }

  void Visit(Limit *op) {
    ctx->nodes[op] = std::make_unique<LimitExecutor>(ctx, op);
    Transform(op->op.get());
  }

  void Visit(Sort *op) {
    ctx->nodes[op] = std::make_unique<SortExecutor>(ctx, op);
    Transform(op->op.get());
  }

  void Visit(Noop *op) { ctx->nodes[op] = std::make_unique<NoopExecutor>(ctx, op); }

  void Visit(Merge *op) {
    ctx->nodes[op] = std::make_unique<MergeExecutor>(ctx, op);
    for (const auto &child : op->ops) Transform(child.get());
  }
};

}  // namespace details

ExecutorContext::ExecutorContext(PlanOperator *op) : root(op) {
  details::ExecutorContextVisitor visitor{this};
  visitor.Transform(root);
}

}  // namespace kqir
