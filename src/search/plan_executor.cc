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

#include "search/executors/filter_executor.h"
#include "search/executors/limit_executor.h"
#include "search/executors/merge_executor.h"
#include "search/executors/mock_executor.h"
#include "search/executors/noop_executor.h"
#include "search/executors/projection_executor.h"
#include "search/executors/sort_executor.h"
#include "search/executors/topn_sort_executor.h"
#include "search/indexer.h"
#include "search/ir_plan.h"

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

    if (auto v = dynamic_cast<Filter *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<Projection *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<TopNSort *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<Mock *>(op)) {
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

  void Visit(Filter *op) {
    ctx->nodes[op] = std::make_unique<FilterExecutor>(ctx, op);
    Transform(op->source.get());
  }

  void Visit(Projection *op) {
    ctx->nodes[op] = std::make_unique<ProjectionExecutor>(ctx, op);
    Transform(op->source.get());
  }

  void Visit(TopNSort *op) {
    ctx->nodes[op] = std::make_unique<TopNSortExecutor>(ctx, op);
    Transform(op->op.get());
  }

  void Visit(Mock *op) { ctx->nodes[op] = std::make_unique<MockExecutor>(ctx, op); }
};

}  // namespace details

ExecutorContext::ExecutorContext(PlanOperator *op) : root(op) {
  details::ExecutorContextVisitor visitor{this};
  visitor.Transform(root);
}

auto ExecutorContext::Retrieve(RowType &row, const FieldInfo *field) -> StatusOr<ValueType> {  // NOLINT
  if (auto iter = row.fields.find(field); iter != row.fields.end()) {
    return iter->second;
  }

  auto retriever = GET_OR_RET(
      redis::FieldValueRetriever::Create(field->index->metadata.on_data_type, row.key, storage, field->index->ns));

  std::string result;
  auto s = retriever.Retrieve(field->name, &result);
  if (!s.ok()) return {Status::NotOK, s.ToString()};

  row.fields.emplace(field, result);
  return result;
}

}  // namespace kqir
