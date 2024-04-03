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

namespace kqir {

struct PushDownNotExpr : Visitor {
  std::unique_ptr<Node> Visit(std::unique_ptr<NotExpr> node) override {
    std::unique_ptr<Node> res;

    if (auto v = Node::As<NumericCompareExpr>(std::move(node->inner))) {
      v->op = v->Negative(v->op);
      return v;
    } else if (auto v = Node::As<TagContainExpr>(std::move(node->inner))) {
      return std::make_unique<NotExpr>(std::move(v));
    } else if (auto v = Node::As<AndExpr>(std::move(node->inner))) {
      std::vector<std::unique_ptr<QueryExpr>> nodes;
      for (auto& n : v->inners) {
        nodes.push_back(std::make_unique<NotExpr>(std::move(n)));
      }
      res = std::make_unique<OrExpr>(std::move(nodes));
    } else if (auto v = Node::As<OrExpr>(std::move(node->inner))) {
      std::vector<std::unique_ptr<QueryExpr>> nodes;
      for (auto& n : v->inners) {
        nodes.push_back(std::make_unique<NotExpr>(std::move(n)));
      }
      res = std::make_unique<AndExpr>(std::move(nodes));
    } else if (auto v = Node::As<NotExpr>(std::move(node->inner))) {
      res = std::move(v->inner);
    }

    return Visitor::Transform(std::move(res));
  }
};

}  // namespace kqir
