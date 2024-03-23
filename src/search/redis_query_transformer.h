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

#include "common_transformer.h"
#include "ir.h"
#include "parse_util.h"
#include "redis_query_parser.h"

namespace kqir {

namespace redis_query {

namespace ir = kqir;

template <typename Rule>
using TreeSelector = parse_tree::selector<
    Rule,
    parse_tree::store_content::on<Number, String, Identifier, Inf>,
    parse_tree::remove_content::on<TagList, NumericRange, ExclusiveNumber, FieldQuery, NotExpr, AndExpr, OrExpr>>;

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
    } else if (Is<TagList>(node)) {
      
    } else {
      // UNREACHABLE CODE, just for debugging here
      return {Status::NotOK, fmt::format("encountered invalid node type: {}", node->type)};
    }
  }
};

template <typename Input>
StatusOr<std::unique_ptr<ir::Node>> ParseToIR(Input&& in) {
  return Transformer::Transform(GET_OR_RET(ParseToTree(std::forward<Input>(in))));
}

}  // namespace redis_query

}  // namespace kqir
