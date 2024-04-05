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

struct SimplifyBoolean : Visitor {
  std::unique_ptr<Node> Visit(std::unique_ptr<OrExpr> node) override {
    node = Node::MustAs<OrExpr>(Visitor::Visit(std::move(node)));

    for (auto iter = node->inners.begin(); iter != node->inners.end();) {
      if (auto v = Node::As<BoolLiteral>(std::move(*iter))) {
        if (!v->val) {
          iter = node->inners.erase(iter);
        } else {
          return v;
        }
      } else {
        ++iter;
      }
    }

    if (node->inners.size() == 0) {
      return std::make_unique<BoolLiteral>(false);
    } else if (node->inners.size() == 1) {
      return std::move(node->inners[0]);
    }

    return node;
  }

  std::unique_ptr<Node> Visit(std::unique_ptr<AndExpr> node) override {
    node = Node::MustAs<AndExpr>(Visitor::Visit(std::move(node)));

    for (auto iter = node->inners.begin(); iter != node->inners.end();) {
      if (auto v = Node::As<BoolLiteral>(std::move(*iter))) {
        if (v->val) {
          iter = node->inners.erase(iter);
        } else {
          return v;
        }
      } else {
        ++iter;
      }
    }

    if (node->inners.size() == 0) {
      return std::make_unique<BoolLiteral>(true);
    } else if (node->inners.size() == 1) {
      return std::move(node->inners[0]);
    }

    return node;
  }

  std::unique_ptr<Node> Visit(std::unique_ptr<NotExpr> node) override {
    node = Node::MustAs<NotExpr>(Visitor::Visit(std::move(node)));

    if (auto v = Node::As<BoolLiteral>(std::move(node->inner))) {
      v->val = !v->val;
      return v;
    }

    return node;
  }
};

}  // namespace kqir
