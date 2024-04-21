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

#include "search/ir.h"
#include "search/ir_pass.h"
#include "search/ir_plan.h"

namespace kqir {

struct LowerToPlan : Visitor {
  std::unique_ptr<Node> Visit(std::unique_ptr<SearchExpr> node) override {
    auto scan = std::make_unique<FullIndexScan>(node->index->CloneAs<IndexRef>());
    auto filter = std::make_unique<Filter>(std::move(scan), std::move(node->query_expr));

    std::unique_ptr<PlanOperator> op = std::move(filter);

    // order is important here, since limit(sort(op)) is different from sort(limit(op))
    if (node->sort_by) {
      op = std::make_unique<Sort>(std::move(op), std::move(node->sort_by));
    }

    if (node->limit) {
      op = std::make_unique<Limit>(std::move(op), std::move(node->limit));
    }

    return std::make_unique<Projection>(std::move(op), std::move(node->select));
  }
};

}  // namespace kqir
