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

#include <algorithm>
#include <cmath>
#include <memory>
#include <set>
#include <type_traits>

#include "search/interval.h"
#include "search/ir.h"
#include "search/ir_pass.h"
#include "search/ir_plan.h"
#include "type_util.h"

namespace kqir {

struct IntervalAnalysis : Visitor {
  struct IntervalInfo {
    std::string field_name;
    const FieldInfo *field_info;
    IntervalSet intervals;
  };

  using Result = std::map<Node *, IntervalInfo>;

  Result result;
  const bool simplify_numeric_compare;

  explicit IntervalAnalysis(bool simplify_numeric_compare = false)
      : simplify_numeric_compare(simplify_numeric_compare) {}

  void Reset() override { result.clear(); }

  template <typename T>
  std::unique_ptr<Node> VisitImpl(std::unique_ptr<T> node) {
    node = Node::MustAs<T>(Visitor::Visit(std::move(node)));

    struct LocalIntervalInfo {
      IntervalSet intervals;
      std::set<Node *> nodes;
      const FieldInfo *field;
    };

    std::map<std::string, LocalIntervalInfo> interval_map;
    for (const auto &n : node->inners) {
      IntervalSet new_interval;
      const FieldInfo *new_field_info = nullptr;
      std::string new_field;

      if (auto v = dynamic_cast<NumericCompareExpr *>(n.get())) {
        new_interval = IntervalSet(v->op, v->num->val);
        new_field = v->field->name;
        new_field_info = v->field->info;
      } else if (auto iter = result.find(n.get()); iter != result.end()) {
        new_interval = iter->second.intervals;
        new_field = iter->second.field_name;
        new_field_info = iter->second.field_info;
      } else {
        continue;
      }

      if (auto iter = interval_map.find(new_field); iter != interval_map.end()) {
        if constexpr (std::is_same_v<T, OrExpr>) {
          iter->second.intervals = iter->second.intervals | new_interval;
        } else if constexpr (std::is_same_v<T, AndExpr>) {
          iter->second.intervals = iter->second.intervals & new_interval;
        } else {
          static_assert(AlwaysFalse<T>);
        }
        iter->second.nodes.emplace(n.get());
        iter->second.field = new_field_info;
      } else {
        interval_map.emplace(new_field, LocalIntervalInfo{new_interval, std::set<Node *>{n.get()}, new_field_info});
      }
    }

    if (interval_map.size() == 1) {
      const auto &elem = *interval_map.begin();
      result.emplace(node.get(), IntervalInfo{elem.first, elem.second.field, elem.second.intervals});
    }

    if (simplify_numeric_compare) {
      for (const auto &[field, info] : interval_map) {
        auto iter = std::remove_if(node->inners.begin(), node->inners.end(),
                                   [&info = info](const auto &n) { return info.nodes.count(n.get()) == 1; });
        node->inners.erase(iter, node->inners.end());
        for (const auto &n : info.nodes) {
          if (auto iter = result.find(n); iter != result.end()) result.erase(iter);
        }

        auto field_node = std::make_unique<FieldRef>(field, info.field);
        node->inners.emplace_back(GenerateFromInterval(info.intervals, field_node.get()));
      }
    }

    return node;
  }

  static std::unique_ptr<QueryExpr> GenerateFromInterval(const IntervalSet &intervals, FieldRef *field) {
    if (intervals.IsEmpty()) {
      return std::make_unique<BoolLiteral>(false);
    }

    if (intervals.IsFull()) {
      return std::make_unique<BoolLiteral>(true);
    }

    std::vector<std::unique_ptr<QueryExpr>> exprs;

    if (intervals.intervals.size() > 1 && std::isinf(intervals.intervals.front().first) &&
        std::isinf(intervals.intervals.back().second)) {
      bool is_all_ne = true;
      auto iter = intervals.intervals.begin();
      auto last = iter->second;
      ++iter;
      while (iter != intervals.intervals.end()) {
        if (iter->first != IntervalSet::NextNum(last)) {
          is_all_ne = false;
          break;
        }

        last = iter->second;
        ++iter;
      }

      if (is_all_ne) {
        for (auto i = intervals.intervals.begin(); i != intervals.intervals.end() && !std::isinf(i->second); ++i) {
          exprs.emplace_back(std::make_unique<NumericCompareExpr>(NumericCompareExpr::NE, field->CloneAs<FieldRef>(),
                                                                  std::make_unique<NumericLiteral>(i->second)));
        }

        return std::make_unique<AndExpr>(std::move(exprs));
      }
    }

    for (auto [l, r] : intervals.intervals) {
      if (std::isinf(l)) {
        exprs.emplace_back(std::make_unique<NumericCompareExpr>(NumericCompareExpr::LT, field->CloneAs<FieldRef>(),
                                                                std::make_unique<NumericLiteral>(r)));
      } else if (std::isinf(r)) {
        exprs.emplace_back(std::make_unique<NumericCompareExpr>(NumericCompareExpr::GET, field->CloneAs<FieldRef>(),
                                                                std::make_unique<NumericLiteral>(l)));
      } else if (r == IntervalSet::NextNum(l)) {
        exprs.emplace_back(std::make_unique<NumericCompareExpr>(NumericCompareExpr::EQ, field->CloneAs<FieldRef>(),
                                                                std::make_unique<NumericLiteral>(l)));
      } else {
        std::vector<std::unique_ptr<QueryExpr>> sub_expr;
        sub_expr.emplace_back(std::make_unique<NumericCompareExpr>(NumericCompareExpr::GET, field->CloneAs<FieldRef>(),
                                                                   std::make_unique<NumericLiteral>(l)));
        sub_expr.emplace_back(std::make_unique<NumericCompareExpr>(NumericCompareExpr::LT, field->CloneAs<FieldRef>(),
                                                                   std::make_unique<NumericLiteral>(r)));

        exprs.emplace_back(std::make_unique<AndExpr>(std::move(sub_expr)));
      }
    }

    if (exprs.size() == 1) {
      return std::move(exprs.front());
    } else {
      return std::make_unique<OrExpr>(std::move(exprs));
    }
  }

  std::unique_ptr<Node> Visit(std::unique_ptr<OrExpr> node) override { return VisitImpl(std::move(node)); }

  std::unique_ptr<Node> Visit(std::unique_ptr<AndExpr> node) override { return VisitImpl(std::move(node)); }
};

}  // namespace kqir
