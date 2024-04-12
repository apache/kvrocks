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

#include "ir.h"
#include "search/ir_sema_checker.h"
#include "string_util.h"

namespace kqir {

struct PlanOperator : Node {};

struct FullIndexScan : PlanOperator {
  IndexInfo *index;

  explicit FullIndexScan(IndexInfo *index) : index(index) {}

  std::string_view Name() const override { return "FullIndexScan"; };
  std::string Content() const override { return index->name; };
  std::string Dump() const override { return fmt::format("full-scan {}", Content()); }

  std::unique_ptr<Node> Clone() const override { return std::make_unique<FullIndexScan>(*this); }
};

struct FieldScan : PlanOperator {
  FieldInfo *field;

  explicit FieldScan(FieldInfo *field) : field(field) {}
};

struct Interval {
  double l, r;  // [l, r)

  explicit Interval(double l, double r) : l(l), r(r) {}

  std::string ToString() const { return fmt::format("[{}, {})", l, r); }
};

struct NumericFieldScan : FieldScan {
  Interval range;

  NumericFieldScan(FieldInfo *field, Interval range) : FieldScan(field), range(range) {}

  std::string_view Name() const override { return "NumericFieldScan"; };
  std::string Content() const override { return fmt::format("{}, {}", field->name, range.ToString()); };
  std::string Dump() const override { return fmt::format("numeric-scan {}", Content()); }

  std::unique_ptr<Node> Clone() const override { return std::make_unique<NumericFieldScan>(*this); }
};

struct TagFieldScan : FieldScan {
  std::string tag;

  TagFieldScan(FieldInfo *field, std::string tag) : FieldScan(field), tag(std::move(tag)) {}

  std::string_view Name() const override { return "TagFieldScan"; };
  std::string Content() const override { return fmt::format("{}, {}", field->name, tag); };
  std::string Dump() const override { return fmt::format("tag-scan {}", Content()); }

  std::unique_ptr<Node> Clone() const override { return std::make_unique<TagFieldScan>(*this); }
};

struct Filter : PlanOperator {
  std::unique_ptr<PlanOperator> source;
  std::unique_ptr<QueryExpr> filter_expr;

  Filter(std::unique_ptr<PlanOperator> &&source, std::unique_ptr<QueryExpr> &&filter_expr)
      : source(std::move(source)), filter_expr(std::move(filter_expr)) {}

  std::string_view Name() const override { return "Filter"; };
  std::string Dump() const override { return fmt::format("(filter {}, {})", source->Dump(), Content()); }

  NodeIterator ChildBegin() override { return {source.get(), filter_expr.get()}; }
  NodeIterator ChildEnd() override { return {}; }

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<Filter>(Node::MustAs<PlanOperator>(source->Clone()),
                                    Node::MustAs<QueryExpr>(filter_expr->Clone()));
  }
};

struct Merge : PlanOperator {
  std::vector<std::unique_ptr<PlanOperator>> ops;

  explicit Merge(std::vector<std::unique_ptr<PlanOperator>> &&ops) : ops(std::move(ops)) {}

  std::string_view Name() const override { return "Merge"; };
  std::string Dump() const override {
    return fmt::format("(merge {})", util::StringJoin(ops, [](const auto &v) { return v->Dump(); }));
  }

  NodeIterator ChildBegin() override { return NodeIterator(ops.begin()); }
  NodeIterator ChildEnd() override { return NodeIterator(ops.end()); }

  std::unique_ptr<Node> Clone() const override {
    std::vector<std::unique_ptr<PlanOperator>> res;
    res.reserve(ops.size());
    for (const auto &op : ops) {
      res.push_back(Node::MustAs<PlanOperator>(op->Clone()));
    }
    return std::make_unique<Merge>(std::move(res));
  }
};

struct Limit : PlanOperator {
  std::unique_ptr<PlanOperator> op;
  std::unique_ptr<LimitClause> limit;

  Limit(std::unique_ptr<PlanOperator> &&op, std::unique_ptr<LimitClause> &&limit)
      : op(std::move(op)), limit(std::move(limit)) {}

  std::string_view Name() const override { return "Limit"; };
  std::string Dump() const override {
    return fmt::format("(limit {}, {}: {})", limit->offset, limit->count, op->Dump());
  }

  NodeIterator ChildBegin() override { return NodeIterator{op.get(), limit.get()}; }
  NodeIterator ChildEnd() override { return {}; }

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<Limit>(Node::MustAs<PlanOperator>(op->Clone()), Node::MustAs<LimitClause>(limit->Clone()));
  }
};

struct Projection : PlanOperator {
  std::unique_ptr<PlanOperator> source;
  std::unique_ptr<SelectClause> select;

  Projection(std::unique_ptr<PlanOperator> &&source, std::unique_ptr<SelectClause> &&select)
      : source(std::move(source)), select(std::move(select)) {}

  std::string_view Name() const override { return "Projection"; };
  std::string Dump() const override { return fmt::format("(project {}: {})", select, source); }

  NodeIterator ChildBegin() override { return {source.get(), select.get()}; }
  NodeIterator ChildEnd() override { return {}; }

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<Projection>(Node::MustAs<PlanOperator>(source->Clone()),
                                        Node::MustAs<SelectClause>(select->Clone()));
  }
};

}  // namespace kqir
