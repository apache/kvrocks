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

struct SimplifyAndOrExpr : Visitor {
  std::unique_ptr<Node> Visit(std::unique_ptr<OrExpr> node) override {
    node = Node::MustAs<OrExpr>(Visitor::Visit(std::move(node)));

    std::vector<std::unique_ptr<QueryExpr>> merged_nodes;
    for (auto &n : node->inners) {
      if (auto v = Node::As<OrExpr>(std::move(n))) {
        for (auto &m : v->inners) {
          merged_nodes.push_back(std::move(m));
        }
      } else {
        merged_nodes.push_back(std::move(n));
      }
    }

    return std::make_unique<OrExpr>(std::move(merged_nodes));
  }

  std::unique_ptr<Node> Visit(std::unique_ptr<AndExpr> node) override {
    node = Node::MustAs<AndExpr>(Visitor::Visit(std::move(node)));

    std::vector<std::unique_ptr<QueryExpr>> merged_nodes;
    for (auto &n : node->inners) {
      if (auto v = Node::As<AndExpr>(std::move(n))) {
        for (auto &m : v->inners) {
          merged_nodes.push_back(std::move(m));
        }
      } else {
        merged_nodes.push_back(std::move(n));
      }
    }

    return std::make_unique<AndExpr>(std::move(merged_nodes));
  }
};

}  // namespace kqir
