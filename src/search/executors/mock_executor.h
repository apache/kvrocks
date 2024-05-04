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

#include "search/ir_plan.h"
#include "search/plan_executor.h"

namespace kqir {

// this operator is only for executor-testing/debugging purpose
struct Mock : PlanOperator {
  std::vector<ExecutorNode::RowType> rows;

  explicit Mock(std::vector<ExecutorNode::RowType> rows) : rows(std::move(rows)) {}

  std::string Dump() const override { return "mock"; }
  std::string_view Name() const override { return "Mock"; }

  std::unique_ptr<Node> Clone() const override { return std::make_unique<Mock>(rows); }
};

struct MockExecutor : ExecutorNode {
  Mock *mock;
  decltype(mock->rows)::iterator iter;

  MockExecutor(ExecutorContext *ctx, Mock *mock) : ExecutorNode(ctx), mock(mock), iter(mock->rows.begin()) {}

  StatusOr<Result> Next() override {
    if (iter == mock->rows.end()) {
      return end;
    }

    return *(iter++);
  }
};

}  // namespace kqir
