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

#include <fmt/format.h>

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "fmt/core.h"
#include "string_util.h"

// kqir stands for Kvorcks Query Intermediate Representation
namespace kqir {

struct Node {
  virtual std::string Dump() const = 0;
  virtual ~Node() = default;

  template <typename T, typename U = Node, typename... Args>
  static std::unique_ptr<U> Create(Args &&...args) {
    return std::unique_ptr<U>(new T(std::forward<Args>(args)...));
  }

  template <typename T>
  static std::unique_ptr<T> As(std::unique_ptr<Node> &&original) {
    auto casted = dynamic_cast<T *>(original.release());
    CHECK(casted);
    return std::unique_ptr<T>(casted);
  }
};

struct FieldRef : Node {
  std::string name;

  explicit FieldRef(std::string name) : name(std::move(name)) {}

  std::string Dump() const override { return name; }
};

struct StringLiteral : Node {
  std::string val;

  explicit StringLiteral(std::string val) : val(std::move(val)) {}

  std::string Dump() const override { return fmt::format("\"{}\"", util::EscapeString(val)); }
};

struct QueryExpr : Node {};

struct BoolAtomExpr : QueryExpr {};

struct TagContainExpr : BoolAtomExpr {
  std::unique_ptr<FieldRef> field;
  std::unique_ptr<StringLiteral> tag;

  TagContainExpr(std::unique_ptr<FieldRef> &&field, std::unique_ptr<StringLiteral> &&tag)
      : field(std::move(field)), tag(std::move(tag)) {}

  std::string Dump() const override { return fmt::format("{} hastag {}", field->Dump(), tag->Dump()); }
};

struct NumericLiteral : Node {
  double val;

  explicit NumericLiteral(double val) : val(val) {}

  std::string Dump() const override { return fmt::format("{}", val); }
};

// NOLINTNEXTLINE
#define KQIR_NUMERIC_COMPARE_OPS(X) X(EQ, =, NE) X(NE, !=, EQ) X(LT, <, GET) X(LET, <=, GT) X(GT, >, LET) X(GET, >=, LT)

struct NumericCompareExpr : BoolAtomExpr {
  enum Op {
#define X(n, s, o) n,  // NOLINT
    KQIR_NUMERIC_COMPARE_OPS(X)
#undef X
  } op;
  std::unique_ptr<FieldRef> field;
  std::unique_ptr<NumericLiteral> num;

  NumericCompareExpr(Op op, std::unique_ptr<FieldRef> &&field, std::unique_ptr<NumericLiteral> &&num)
      : op(op), field(std::move(field)), num(std::move(num)) {}

  static constexpr const char *ToOperator(Op op) {
    switch (op) {
// NOLINTNEXTLINE
#define X(n, s, o) \
  case n:          \
    return #s;
      KQIR_NUMERIC_COMPARE_OPS(X)
#undef X
    }

    return nullptr;
  }

  static constexpr std::optional<Op> FromOperator(std::string_view op) {
// NOLINTNEXTLINE
#define X(n, s, o) \
  if (op == #s) return n;
    KQIR_NUMERIC_COMPARE_OPS(X)
#undef X

    return std::nullopt;
  }

  static constexpr Op Negative(Op op) {
    switch (op) {
      // NOLINTNEXTLINE
#define X(n, s, o) \
  case n:          \
    return o;
      KQIR_NUMERIC_COMPARE_OPS(X)
#undef X
    }

    __builtin_unreachable();
  }

  std::string Dump() const override { return fmt::format("{} {} {}", field->Dump(), ToOperator(op), num->Dump()); };
};

struct BoolLiteral : BoolAtomExpr {
  bool val;

  explicit BoolLiteral(bool val) : val(val) {}

  std::string Dump() const override { return val ? "true" : "false"; }
};

struct QueryExpr;

struct NotExpr : QueryExpr {
  std::unique_ptr<QueryExpr> inner;

  explicit NotExpr(std::unique_ptr<QueryExpr> &&inner) : inner(std::move(inner)) {}

  std::string Dump() const override { return fmt::format("not {}", inner->Dump()); }
};

struct AndExpr : QueryExpr {
  std::vector<std::unique_ptr<QueryExpr>> inners;

  explicit AndExpr(std::vector<std::unique_ptr<QueryExpr>> &&inners) : inners(std::move(inners)) {}

  std::string Dump() const override {
    return fmt::format("and ({})", util::StringJoin(inners, [](const auto &v) { return v->Dump(); }));
  }
};

struct OrExpr : QueryExpr {
  std::vector<std::unique_ptr<QueryExpr>> inners;

  explicit OrExpr(std::vector<std::unique_ptr<QueryExpr>> &&inners) : inners(std::move(inners)) {}

  std::string Dump() const override {
    return fmt::format("or ({})", util::StringJoin(inners, [](const auto &v) { return v->Dump(); }));
  }
};

struct Limit : Node {
  size_t offset = 0;
  size_t count = std::numeric_limits<size_t>::max();

  Limit(size_t offset, size_t count) : offset(offset), count(count) {}

  std::string Dump() const override { return fmt::format("limit {} {}", offset, count); }
};

struct SortBy : Node {
  enum Order { ASC, DESC } order = ASC;
  std::unique_ptr<FieldRef> field;

  SortBy(Order order, std::unique_ptr<FieldRef> &&field) : order(order), field(std::move(field)) {}

  static constexpr const char *OrderToString(Order order) { return order == ASC ? "ASC" : "DESC"; }
  std::string Dump() const override { return fmt::format("sortby {} {}", field->Dump(), OrderToString(order)); }
};

struct SelectExpr : Node {
  std::vector<std::unique_ptr<FieldRef>> fields;

  explicit SelectExpr(std::vector<std::unique_ptr<FieldRef>> &&fields) : fields(std::move(fields)) {}

  std::string Dump() const override {
    if (fields.empty()) return "select *";
    return fmt::format("select ({})", util::StringJoin(fields, [](const auto &v) { return v->Dump(); }));
  }
};

struct IndexRef : Node {
  std::string name;

  explicit IndexRef(std::string name) : name(std::move(name)) {}

  std::string Dump() const override { return name; }
};

struct SearchStmt : Node {
  std::unique_ptr<IndexRef> index;
  std::unique_ptr<QueryExpr> query_expr;
  std::unique_ptr<Limit> limit;
  std::unique_ptr<SortBy> sort_by;
  std::unique_ptr<SelectExpr> select_expr;

  SearchStmt(std::unique_ptr<IndexRef> &&index, std::unique_ptr<QueryExpr> &&query_expr, std::unique_ptr<Limit> &&limit,
             std::unique_ptr<SortBy> &&sort_by, std::unique_ptr<SelectExpr> &&select_expr)
      : index(std::move(index)),
        query_expr(std::move(query_expr)),
        limit(std::move(limit)),
        sort_by(std::move(sort_by)),
        select_expr(std::move(select_expr)) {}

  std::string Dump() const override {
    return fmt::format("search {}: query {} {} {} {}", index->Dump(), query_expr->Dump(), limit->Dump(),
                       sort_by->Dump(), select_expr->Dump());
  }
};

}  // namespace kqir
