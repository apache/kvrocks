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

#include <limits>
#include <memory>
#include <string>
#include <variant>
#include <vector>

// kqir stands for Kvorcks Query Intermediate Representaion
namespace kqir {

struct FieldRef {
  std::string name;
};

struct TagContainExpr {
  FieldRef field;
  std::string tag;
};

struct NumericCompareExpr {
  enum { EQ, NE, LT, LET, GT, GET } op;
  FieldRef field;
  double num;
};

struct AtomExpr {
  std::variant<TagContainExpr, NumericCompareExpr> expr;
};

struct QueryExpr;

struct LogicalUnaryExpr {
  enum { NOT } op;
  std::unique_ptr<QueryExpr> inner;
};

struct LogicalBinaryExpr {
  enum { AND, OR } op;
  std::unique_ptr<QueryExpr> lhs;
  std::unique_ptr<QueryExpr> rhs;
};

struct QueryExpr {
  std::variant<LogicalUnaryExpr, LogicalBinaryExpr, AtomExpr> expr;
};

struct Limit {
  size_t offset = 0;
  size_t count = std::numeric_limits<size_t>::max();
};

struct SortBy {
  enum { ASC, DESC } order;
  FieldRef field;
};

struct SelectExpr {
  std::vector<FieldRef> fields;
};

struct IndexRef {
  std::string name;
};

struct SearchStmt {
  IndexRef index_ref;
  QueryExpr query_expr;
  Limit limit;
  SortBy sort_by;
  SelectExpr select_expr;
};

}  // namespace kqir
