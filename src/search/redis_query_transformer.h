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

#include "common_transformer.h"
#include "ir.h"
#include "parse_util.h"
#include "redis_query_parser.h"
#include "search/common_parser.h"

namespace kqir {

namespace redis_query {

namespace ir = kqir;

template <typename Rule>
using TreeSelector =
    parse_tree::selector<Rule, parse_tree::store_content::on<Number, String, Identifier, Inf>,
                         parse_tree::remove_content::on<TagList, NumericRange, ExclusiveNumber, FieldQuery, NotExpr,
                                                        AndExpr, OrExpr, Wildcard>>;

template <typename Input>
StatusOr<std::unique_ptr<parse_tree::node>> ParseToTree(Input&& in) {
  if (auto root = parse_tree::parse<seq<QueryExpr, eof>, TreeSelector>(std::forward<Input>(in))) {
    return root;
  } else {
    // TODO: improve parse error message, with source location
    return {Status::NotOK, "invalid syntax"};
  }
}

struct Transformer : ir::TreeTransformer {
  static auto Transform(const TreeNode& node) -> StatusOr<std::unique_ptr<Node>> {
    if (Is<Number>(node)) {
      return Node::Create<ir::NumericLiteral>(*ParseFloat(node->string()));
    } else if (Is<Wildcard>(node)) {
      return Node::Create<ir::BoolLiteral>(true);
    } else if (Is<FieldQuery>(node)) {
      CHECK(node->children.size() == 2);

      auto field = node->children[0]->string();
      const auto& query = node->children[1];

      if (Is<TagList>(query)) {
        std::vector<std::unique_ptr<ir::QueryExpr>> exprs;

        for (const auto& tag : query->children) {
          auto tag_str = Is<Identifier>(tag) ? tag->string() : GET_OR_RET(UnescapeString(tag->string()));
          exprs.push_back(std::make_unique<ir::TagContainExpr>(std::make_unique<FieldRef>(field),
                                                               std::make_unique<StringLiteral>(tag_str)));
        }

        if (exprs.size() == 1) {
          return std::move(exprs[0]);
        } else {
          return std::make_unique<ir::OrExpr>(std::move(exprs));
        }
      } else {  // NumericRange
        std::vector<std::unique_ptr<ir::QueryExpr>> exprs;

        const auto& lhs = query->children[0];
        const auto& rhs = query->children[1];

        if (Is<ExclusiveNumber>(lhs)) {
          exprs.push_back(std::make_unique<NumericCompareExpr>(
              NumericCompareExpr::GT, std::make_unique<FieldRef>(field),
              Node::MustAs<NumericLiteral>(GET_OR_RET(Transform(lhs->children[0])))));
        } else if (Is<Number>(lhs)) {
          exprs.push_back(
              std::make_unique<NumericCompareExpr>(NumericCompareExpr::GET, std::make_unique<FieldRef>(field),
                                                   Node::MustAs<NumericLiteral>(GET_OR_RET(Transform(lhs)))));
        } else {  // Inf
          if (lhs->string_view() == "+inf") {
            return {Status::NotOK, "it's not allowed to set the lower bound as positive infinity"};
          }
        }

        if (Is<ExclusiveNumber>(rhs)) {
          exprs.push_back(std::make_unique<NumericCompareExpr>(
              NumericCompareExpr::LT, std::make_unique<FieldRef>(field),
              Node::MustAs<NumericLiteral>(GET_OR_RET(Transform(rhs->children[0])))));
        } else if (Is<Number>(rhs)) {
          exprs.push_back(
              std::make_unique<NumericCompareExpr>(NumericCompareExpr::LET, std::make_unique<FieldRef>(field),
                                                   Node::MustAs<NumericLiteral>(GET_OR_RET(Transform(rhs)))));
        } else {  // Inf
          if (rhs->string_view() == "-inf") {
            return {Status::NotOK, "it's not allowed to set the upper bound as negative infinity"};
          }
        }

        if (exprs.empty()) {
          return std::make_unique<BoolLiteral>(true);
        } else if (exprs.size() == 1) {
          return std::move(exprs[0]);
        } else {
          return std::make_unique<ir::AndExpr>(std::move(exprs));
        }
      }
    } else if (Is<NotExpr>(node)) {
      CHECK(node->children.size() == 1);

      return Node::Create<ir::NotExpr>(Node::MustAs<ir::QueryExpr>(GET_OR_RET(Transform(node->children[0]))));
    } else if (Is<AndExpr>(node)) {
      std::vector<std::unique_ptr<ir::QueryExpr>> exprs;

      for (const auto& child : node->children) {
        exprs.push_back(Node::MustAs<ir::QueryExpr>(GET_OR_RET(Transform(child))));
      }

      return Node::Create<ir::AndExpr>(std::move(exprs));
    } else if (Is<OrExpr>(node)) {
      std::vector<std::unique_ptr<ir::QueryExpr>> exprs;

      for (const auto& child : node->children) {
        exprs.push_back(Node::MustAs<ir::QueryExpr>(GET_OR_RET(Transform(child))));
      }

      return Node::Create<ir::OrExpr>(std::move(exprs));
    } else if (IsRoot(node)) {
      CHECK(node->children.size() == 1);

      return Transform(node->children[0]);
    } else {
      // UNREACHABLE CODE, just for debugging here
      return {Status::NotOK, fmt::format("encountered invalid node type: {}", node->type)};
    }
  }  // NOLINT
};

template <typename Input>
StatusOr<std::unique_ptr<ir::Node>> ParseToIR(Input&& in) {
  return Transformer::Transform(GET_OR_RET(ParseToTree(std::forward<Input>(in))));
}

}  // namespace redis_query

}  // namespace kqir
