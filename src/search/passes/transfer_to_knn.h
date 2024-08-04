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

struct TransferSortByToKnnExpr : Visitor {
  std::unique_ptr<Node> Visit(std::unique_ptr<SearchExpr> node) override {
    node = Node::MustAs<SearchExpr>(Visitor::Visit(std::move(node)));

    if (node->sort_by && node->sort_by->IsVectorField() && node->limit) {
      std::vector<std::unique_ptr<QueryExpr>> exprs;
      auto knn_expr = std::make_unique<VectorKnnExpr>(Node::MustAs<FieldRef>(node->sort_by->TakeFieldRef()),
                                                      Node::MustAs<VectorLiteral>(node->sort_by->TakeVectorLiteral()),
                                                      node->limit->Count());
      if (auto b = Node::As<BoolLiteral>(std::move(node->query_expr))) {
        if (b->val) exprs.push_back(std::move(knn_expr));
      } else {
        exprs.push_back(std::move(node->query_expr));
        exprs.push_back(std::move(knn_expr));
      }

      if (exprs.empty()) {
        node->query_expr = std::make_unique<BoolLiteral>(false);
      } else if (exprs.size() == 1) {
        node->query_expr = std::move(exprs[0]);
      } else {
        node->query_expr = std::make_unique<AndExpr>(std::move(exprs));
      }
      node->sort_by.reset();
      node->limit.reset();
    }

    return node;
  }
};

}  // namespace kqir
