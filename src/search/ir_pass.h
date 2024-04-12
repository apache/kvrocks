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

#include "ir.h"

namespace kqir {

struct Pass {
  virtual std::unique_ptr<Node> Transform(std::unique_ptr<Node> node) = 0;
};

struct Visitor : Pass {
  std::unique_ptr<Node> Transform(std::unique_ptr<Node> node) override {
    if (auto v = Node::As<SearchStmt>(std::move(node))) {
      return Visit(std::move(v));
    } else if (auto v = Node::As<SelectClause>(std::move(node))) {
      return Visit(std::move(v));
    } else if (auto v = Node::As<IndexRef>(std::move(node))) {
      return Visit(std::move(v));
    } else if (auto v = Node::As<LimitClause>(std::move(node))) {
      return Visit(std::move(v));
    } else if (auto v = Node::As<SortByClause>(std::move(node))) {
      return Visit(std::move(v));
    } else if (auto v = Node::As<AndExpr>(std::move(node))) {
      return Visit(std::move(v));
    } else if (auto v = Node::As<OrExpr>(std::move(node))) {
      return Visit(std::move(v));
    } else if (auto v = Node::As<NotExpr>(std::move(node))) {
      return Visit(std::move(v));
    } else if (auto v = Node::As<NumericCompareExpr>(std::move(node))) {
      return Visit(std::move(v));
    } else if (auto v = Node::As<NumericLiteral>(std::move(node))) {
      return Visit(std::move(v));
    } else if (auto v = Node::As<FieldRef>(std::move(node))) {
      return Visit(std::move(v));
    } else if (auto v = Node::As<TagContainExpr>(std::move(node))) {
      return Visit(std::move(v));
    } else if (auto v = Node::As<StringLiteral>(std::move(node))) {
      return Visit(std::move(v));
    } else if (auto v = Node::As<BoolLiteral>(std::move(node))) {
      return Visit(std::move(v));
    }

    __builtin_unreachable();
  }

  template <typename T>
  std::unique_ptr<T> VisitAs(std::unique_ptr<T> n) {
    return Node::MustAs<T>(Visit(std::move(n)));
  }

  template <typename T>
  std::unique_ptr<T> TransformAs(std::unique_ptr<Node> n) {
    return Node::MustAs<T>(Transform(std::move(n)));
  }

  virtual std::unique_ptr<Node> Visit(std::unique_ptr<SearchStmt> node) {
    node->index = VisitAs<IndexRef>(std::move(node->index));
    node->select = VisitAs<SelectClause>(std::move(node->select));
    node->query_expr = TransformAs<QueryExpr>(std::move(node->query_expr));
    if (node->sort_by) node->sort_by = VisitAs<SortByClause>(std::move(node->sort_by));
    if (node->limit) node->limit = VisitAs<LimitClause>(std::move(node->limit));
    return node;
  }

  virtual std::unique_ptr<Node> Visit(std::unique_ptr<SelectClause> node) {
    for (auto &n : node->fields) {
      n = VisitAs<FieldRef>(std::move(n));
    }

    return node;
  }

  virtual std::unique_ptr<Node> Visit(std::unique_ptr<IndexRef> node) { return node; }

  virtual std::unique_ptr<Node> Visit(std::unique_ptr<FieldRef> node) { return node; }

  virtual std::unique_ptr<Node> Visit(std::unique_ptr<BoolLiteral> node) { return node; }

  virtual std::unique_ptr<Node> Visit(std::unique_ptr<StringLiteral> node) { return node; }

  virtual std::unique_ptr<Node> Visit(std::unique_ptr<NumericLiteral> node) { return node; }

  virtual std::unique_ptr<Node> Visit(std::unique_ptr<NumericCompareExpr> node) {
    node->field = VisitAs<FieldRef>(std::move(node->field));
    node->num = VisitAs<NumericLiteral>(std::move(node->num));
    return node;
  }

  virtual std::unique_ptr<Node> Visit(std::unique_ptr<TagContainExpr> node) {
    node->field = VisitAs<FieldRef>(std::move(node->field));
    node->tag = VisitAs<StringLiteral>(std::move(node->tag));
    return node;
  }

  virtual std::unique_ptr<Node> Visit(std::unique_ptr<AndExpr> node) {
    for (auto &n : node->inners) {
      n = TransformAs<QueryExpr>(std::move(n));
    }

    return node;
  }

  virtual std::unique_ptr<Node> Visit(std::unique_ptr<OrExpr> node) {
    for (auto &n : node->inners) {
      n = TransformAs<QueryExpr>(std::move(n));
    }

    return node;
  }

  virtual std::unique_ptr<Node> Visit(std::unique_ptr<NotExpr> node) {
    node->inner = TransformAs<QueryExpr>(std::move(node->inner));
    return node;
  }

  virtual std::unique_ptr<Node> Visit(std::unique_ptr<LimitClause> node) { return node; }

  virtual std::unique_ptr<Node> Visit(std::unique_ptr<SortByClause> node) {
    node->field = VisitAs<FieldRef>(std::move(node->field));
    return node;
  }
};

}  // namespace kqir
