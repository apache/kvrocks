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

namespace plan {

struct PlanOperator : Node {};

struct FullIndexScan : PlanOperator {
  IndexInfo *index;

  std::string_view Name() const override { return "FullIndexScan"; };
  std::string Content() const override { return index->name; };
  std::string Dump() const override { return fmt::format("full-scan {}", Content()); }
};

struct FieldScan : PlanOperator {
  FieldInfo *field;
};

struct Interval {
  double l, r;  // [l, r)

  std::string ToString() const { return fmt::format("[{}, {})", l, r); }
};

struct NumericFieldScan : FieldScan {
  Interval range;

  std::string_view Name() const override { return "NumericFieldScan"; };
  std::string Content() const override { return fmt::format("{}, {}", field->name, range.ToString()); };
  std::string Dump() const override { return fmt::format("numeric-scan {}", Content()); }
};

struct TagFieldScan : FieldScan {
  std::string tag;

  std::string_view Name() const override { return "TagFieldScan"; };
  std::string Content() const override { return fmt::format("{}, {}", field->name, tag); };
  std::string Dump() const override { return fmt::format("tag-scan {}", Content()); }
};

struct Filter : PlanOperator {
  std::unique_ptr<PlanOperator> source;
  std::unique_ptr<QueryExpr> filter_expr;

  std::string_view Name() const override { return "Filter"; };
  std::string Dump() const override { return fmt::format("(filter {}, {})", source->Dump(), Content()); }

  NodeIterator ChildBegin() override { return {source.get(), filter_expr.get()}; }
  NodeIterator ChildEnd() override { return {}; }
};

struct Merge : PlanOperator {
  std::vector<std::unique_ptr<PlanOperator>> ops;

  std::string_view Name() const override { return "Merge"; };
  std::string Dump() const override {
    return fmt::format("(merge {})", util::StringJoin(ops, [](const auto &v) { return v->Dump(); }));
  }

  NodeIterator ChildBegin() override { return NodeIterator(ops.begin()); }
  NodeIterator ChildEnd() override { return NodeIterator(ops.end()); }
};

struct Limit : PlanOperator {
  std::unique_ptr<PlanOperator> op;
  size_t offset = 0, count = std::numeric_limits<size_t>::max();

  std::string_view Name() const override { return "Limit"; };
  std::string Dump() const override { return fmt::format("(limit {}, {}: {})", offset, count, op->Dump()); }

  NodeIterator ChildBegin() override { return NodeIterator{op.get()}; }
  NodeIterator ChildEnd() override { return {}; }
};

struct Projection : PlanOperator {
  std::unique_ptr<PlanOperator> source;
  std::unique_ptr<SelectExpr> select;

  std::string_view Name() const override { return "Projection"; };
  std::string Dump() const override { return fmt::format("(project {}: {})", select, source); }

  NodeIterator ChildBegin() override { return {source.get(), select.get()}; }
  NodeIterator ChildEnd() override { return {}; }
};

}  // namespace plan

}  // namespace kqir
