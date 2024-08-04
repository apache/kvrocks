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
#include "search/interval.h"
#include "search/ir_sema_checker.h"
#include "search/value.h"
#include "string_util.h"

namespace kqir {

struct PlanOperator : Node {};

struct Noop : PlanOperator {
  std::string_view Name() const override { return "Noop"; };
  std::string Dump() const override { return "noop"; }

  std::unique_ptr<Node> Clone() const override { return std::make_unique<Noop>(*this); }
};

struct FullIndexScan : PlanOperator {
  std::unique_ptr<IndexRef> index;

  explicit FullIndexScan(std::unique_ptr<IndexRef> index) : index(std::move(index)) {}

  std::string_view Name() const override { return "FullIndexScan"; }
  std::string Dump() const override { return fmt::format("full-scan {}", index->name); }

  NodeIterator ChildBegin() override { return NodeIterator{index.get()}; }
  NodeIterator ChildEnd() override { return {}; }

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<FullIndexScan>(Node::MustAs<IndexRef>(index->Clone()));
  }
};

struct FieldScan : PlanOperator {
  std::unique_ptr<FieldRef> field;

  explicit FieldScan(std::unique_ptr<FieldRef> field) : field(std::move(field)) {}

  NodeIterator ChildBegin() override { return NodeIterator{field.get()}; }
  NodeIterator ChildEnd() override { return {}; }
};

struct NumericFieldScan : FieldScan {
  Interval range;
  SortByClause::Order order;

  NumericFieldScan(std::unique_ptr<FieldRef> field, Interval range, SortByClause::Order order = SortByClause::ASC)
      : FieldScan(std::move(field)), range(range), order(order) {}

  std::string_view Name() const override { return "NumericFieldScan"; };
  std::string Content() const override {
    return fmt::format("{}, {}", range.ToString(), SortByClause::OrderToString(order));
  };
  std::string Dump() const override { return fmt::format("numeric-scan {}, {}", field->name, Content()); }

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<NumericFieldScan>(field->CloneAs<FieldRef>(), range, order);
  }
};

struct TagFieldScan : FieldScan {
  std::string tag;

  TagFieldScan(std::unique_ptr<FieldRef> field, std::string tag) : FieldScan(std::move(field)), tag(std::move(tag)) {}

  std::string_view Name() const override { return "TagFieldScan"; };
  std::string Content() const override { return tag; };
  std::string Dump() const override { return fmt::format("tag-scan {}, {}", field->name, tag); }

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<TagFieldScan>(field->CloneAs<FieldRef>(), tag);
  }
};

struct HnswVectorFieldKnnScan : FieldScan {
  kqir::NumericArray vector;
  uint32_t k;

  HnswVectorFieldKnnScan(std::unique_ptr<FieldRef> field, kqir::NumericArray vector, uint16_t k)
      : FieldScan(std::move(field)), vector(std::move(vector)), k(k) {}

  std::string_view Name() const override { return "HnswVectorFieldKnnScan"; };
  std::string Content() const override {
    return fmt::format("[{}], {}", util::StringJoin(vector, [](auto v) { return std::to_string(v); }), k);
  };
  std::string Dump() const override { return fmt::format("hnsw-vector-knn-scan {}, {}", field->name, Content()); }

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<HnswVectorFieldKnnScan>(field->CloneAs<FieldRef>(), vector, k);
  }
};

struct HnswVectorFieldRangeScan : FieldScan {
  kqir::NumericArray vector;
  uint32_t range;

  HnswVectorFieldRangeScan(std::unique_ptr<FieldRef> field, kqir::NumericArray vector, uint32_t range)
      : FieldScan(std::move(field)), vector(std::move(vector)), range(range) {}

  std::string_view Name() const override { return "HnswVectorFieldRangeScan"; };
  std::string Content() const override {
    return fmt::format("[{}], {}", util::StringJoin(vector, [](auto v) { return std::to_string(v); }), range);
  };
  std::string Dump() const override { return fmt::format("hnsw-vector-range-scan {}, {}", field->name, Content()); }

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<HnswVectorFieldRangeScan>(field->CloneAs<FieldRef>(), vector, range);
  }
};

struct Filter : PlanOperator {
  std::unique_ptr<PlanOperator> source;
  std::unique_ptr<QueryExpr> filter_expr;

  Filter(std::unique_ptr<PlanOperator> &&source, std::unique_ptr<QueryExpr> &&filter_expr)
      : source(std::move(source)), filter_expr(std::move(filter_expr)) {}

  std::string_view Name() const override { return "Filter"; };
  std::string Dump() const override { return fmt::format("(filter {}: {})", filter_expr->Dump(), source->Dump()); }

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

  static std::unique_ptr<PlanOperator> Create(std::vector<std::unique_ptr<PlanOperator>> &&ops) {
    CHECK(!ops.empty());

    if (ops.size() == 1) {
      return std::move(ops.front());
    }

    return std::make_unique<Merge>(std::move(ops));
  }

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

struct Sort : PlanOperator {
  std::unique_ptr<PlanOperator> op;
  std::unique_ptr<SortByClause> order;

  Sort(std::unique_ptr<PlanOperator> &&op, std::unique_ptr<SortByClause> &&order)
      : op(std::move(op)), order(std::move(order)) {}

  std::string_view Name() const override { return "Sort"; };
  std::string Dump() const override {
    return fmt::format("(sort {}, {}: {})", order->field->Dump(), order->OrderToString(order->order), op->Dump());
  }

  NodeIterator ChildBegin() override { return NodeIterator{op.get(), order.get()}; }
  NodeIterator ChildEnd() override { return {}; }

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<Sort>(Node::MustAs<PlanOperator>(op->Clone()), Node::MustAs<SortByClause>(order->Clone()));
  }
};

// operator fusion: Sort + Limit
struct TopNSort : PlanOperator {
  std::unique_ptr<PlanOperator> op;
  std::unique_ptr<SortByClause> order;
  std::unique_ptr<LimitClause> limit;

  TopNSort(std::unique_ptr<PlanOperator> &&op, std::unique_ptr<SortByClause> &&order,
           std::unique_ptr<LimitClause> &&limit)
      : op(std::move(op)), order(std::move(order)), limit(std::move(limit)) {}

  std::string_view Name() const override { return "TopNSort"; };
  std::string Dump() const override {
    return fmt::format("(top-n sort {}, {}, {}, {}: {})", order->field->Dump(), order->OrderToString(order->order),
                       limit->offset, limit->count, op->Dump());
  }

  static inline const std::vector<std::function<Node *(Node *)>> ChildMap = {
      NodeIterator::MemFn<&TopNSort::op>, NodeIterator::MemFn<&TopNSort::order>, NodeIterator::MemFn<&TopNSort::limit>};

  NodeIterator ChildBegin() override { return NodeIterator(this, ChildMap.begin()); }
  NodeIterator ChildEnd() override { return NodeIterator(this, ChildMap.end()); }

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<TopNSort>(Node::MustAs<PlanOperator>(op->Clone()),
                                      Node::MustAs<SortByClause>(order->Clone()),
                                      Node::MustAs<LimitClause>(limit->Clone()));
  }
};

struct Projection : PlanOperator {
  std::unique_ptr<PlanOperator> source;
  std::unique_ptr<SelectClause> select;

  Projection(std::unique_ptr<PlanOperator> &&source, std::unique_ptr<SelectClause> &&select)
      : source(std::move(source)), select(std::move(select)) {}

  std::string_view Name() const override { return "Projection"; };
  std::string Dump() const override {
    auto select_str =
        select->fields.empty() ? "*" : util::StringJoin(select->fields, [](const auto &v) { return v->Dump(); });
    return fmt::format("project {}: {}", select_str, source->Dump());
  }

  NodeIterator ChildBegin() override { return {source.get(), select.get()}; }
  NodeIterator ChildEnd() override { return {}; }

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<Projection>(Node::MustAs<PlanOperator>(source->Clone()),
                                        Node::MustAs<SelectClause>(select->Clone()));
  }
};

}  // namespace kqir
