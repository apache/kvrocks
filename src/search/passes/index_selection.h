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
#include <range/v3/view.hpp>
#include <type_traits>
#include <variant>

#include "search/index_info.h"
#include "search/interval.h"
#include "search/ir.h"
#include "search/ir_pass.h"
#include "search/ir_plan.h"
#include "search/passes/cost_model.h"
#include "search/passes/interval_analysis.h"
#include "search/passes/push_down_not_expr.h"
#include "search/passes/simplify_and_or_expr.h"
#include "search/search_encoding.h"

namespace kqir {

struct IndexSelection : Visitor {
  SortByClause *order = nullptr;
  bool sort_removable = false;
  IndexRef *index = nullptr;
  IntervalAnalysis::Result intervals;

  void Reset() override {
    order = nullptr;
    sort_removable = false;
    index = nullptr;
    intervals.clear();
  }

  std::unique_ptr<Node> Visit(std::unique_ptr<Projection> node) override {
    IntervalAnalysis analysis(false);
    node = Node::MustAs<Projection>(analysis.Transform(std::move(node)));
    intervals = std::move(analysis.result);

    return Visitor::Visit(std::move(node));
  }

  std::unique_ptr<Node> Visit(std::unique_ptr<Sort> node) override {
    order = node->order.get();

    node = Node::MustAs<Sort>(Visitor::Visit(std::move(node)));

    if (sort_removable) return std::move(node->op);

    return node;
  }

  bool HasGoodOrder() const { return order && order->field->info->HasIndex(); }

  std::unique_ptr<PlanOperator> GenerateScanFromOrder() const {
    if (order->field->info->MetadataAs<redis::NumericFieldMetadata>()) {
      return std::make_unique<NumericFieldScan>(order->field->CloneAs<FieldRef>(), Interval::Full(), order->order);
    } else {
      CHECK(false) << "current only numeric field is supported for ordering";
    }
  }

  // if there's no Filter node, enter this method
  std::unique_ptr<Node> Visit(std::unique_ptr<FullIndexScan> node) override {
    if (HasGoodOrder()) {
      sort_removable = true;
      return GenerateScanFromOrder();
    }

    return node;
  }

  std::unique_ptr<Node> Visit(std::unique_ptr<Filter> node) override {
    auto index_scan = Node::MustAs<FullIndexScan>(std::move(node->source));

    if (HasGoodOrder()) {
      // TODO: optimize plan with sorting order via the cost model
      sort_removable = true;

      auto scan = GenerateScanFromOrder();
      return std::make_unique<Filter>(std::move(scan), std::move(node->filter_expr));
    } else {
      index = index_scan->index.get();

      return TransformExpr(node->filter_expr.get());
    }
  }

  std::unique_ptr<PlanOperator> TransformExpr(QueryExpr *node) {
    if (auto v = dynamic_cast<AndExpr *>(node)) {
      return VisitExpr(v);
    }
    if (auto v = dynamic_cast<OrExpr *>(node)) {
      return VisitExpr(v);
    }
    if (auto v = dynamic_cast<NumericCompareExpr *>(node)) {
      return VisitExpr(v);
    }
    if (auto v = dynamic_cast<TagContainExpr *>(node)) {
      return VisitExpr(v);
    }
    if (auto v = dynamic_cast<NotExpr *>(node)) {
      return VisitExpr(v);
    }

    CHECK(false) << "unreachable";
  }

  std::unique_ptr<PlanOperator> MakeFullIndexFilter(QueryExpr *node) const {
    return std::make_unique<Filter>(std::make_unique<FullIndexScan>(index->CloneAs<IndexRef>()),
                                    node->CloneAs<QueryExpr>());
  }

  std::unique_ptr<PlanOperator> VisitExpr(NotExpr *node) const {
    // after PushDownNotExpr, `node->inner` should be one of TagContainExpr and NumericCompareExpr
    return MakeFullIndexFilter(node);
  }

  std::unique_ptr<PlanOperator> VisitExpr(TagContainExpr *node) const {
    if (node->field->info->HasIndex()) {
      return std::make_unique<TagFieldScan>(node->field->CloneAs<FieldRef>(), node->tag->val);
    }

    return MakeFullIndexFilter(node);
  }

  // enter only if there's just a single NumericCompareExpr, without and/or expression
  std::unique_ptr<PlanOperator> VisitExpr(NumericCompareExpr *node) const {
    if (node->field->info->HasIndex() && node->op != NumericCompareExpr::NE) {
      IntervalSet is(node->op, node->num->val);
      return PlanFromInterval(is, node->field.get(), SortByClause::ASC);
    }

    return MakeFullIndexFilter(node);
  }

  template <typename Expr>
  std::unique_ptr<PlanOperator> VisitExprImpl(Expr *node) {
    struct AggregatedNodes {
      std::set<Node *> nodes;
      IntervalSet intervals;
    };

    std::map<const FieldInfo *, AggregatedNodes> agg_nodes;
    std::vector<std::variant<QueryExpr *, const FieldInfo *>> rest_nodes;

    for (const auto &n : node->inners) {
      IntervalSet is;
      const FieldInfo *field = nullptr;

      if (auto iter = intervals.find(n.get()); iter != intervals.end()) {
        field = iter->second.field_info;
        is = iter->second.intervals;
      } else if (auto expr = dynamic_cast<NumericCompareExpr *>(n.get()); expr && expr->op != NumericCompareExpr::NE) {
        field = expr->field->info;
        is = IntervalSet(expr->op, expr->num->val);
      } else {
        rest_nodes.emplace_back(n.get());
        continue;
      }

      if (!field->HasIndex()) {
        rest_nodes.emplace_back(n.get());
        continue;
      }

      if (auto jter = agg_nodes.find(field); jter != agg_nodes.end()) {
        jter->second.nodes.emplace(n.get());
        if constexpr (std::is_same_v<Expr, AndExpr>) {
          jter->second.intervals = jter->second.intervals & is;
        } else {
          jter->second.intervals = jter->second.intervals | is;
        }
      } else {
        rest_nodes.emplace_back(field);
        agg_nodes.emplace(field, AggregatedNodes{std::set<Node *>{n.get()}, is});
      }
    }

    if constexpr (std::is_same_v<Expr, AndExpr>) {
      struct SelectionInfo {
        std::unique_ptr<PlanOperator> plan;
        std::set<Node *> selected_nodes;
        size_t cost;

        SelectionInfo(std::unique_ptr<PlanOperator> &&plan, std::set<Node *> nodes)
            : plan(std::move(plan)), selected_nodes(std::move(nodes)), cost(CostModel::Transform(this->plan.get())) {}
      };

      std::vector<SelectionInfo> available_plans;

      available_plans.emplace_back(std::make_unique<FullIndexScan>(index->CloneAs<IndexRef>()), std::set<Node *>{});

      for (auto v : rest_nodes) {
        if (std::holds_alternative<QueryExpr *>(v)) {
          auto n = std::get<QueryExpr *>(v);
          auto op = TransformExpr(n);

          available_plans.emplace_back(std::move(op), std::set<Node *>{n});
        } else {
          auto n = std::get<const FieldInfo *>(v);
          const auto &agg_info = agg_nodes.at(n);
          auto field_ref = std::make_unique<FieldRef>(n->name, n);
          available_plans.emplace_back(PlanFromInterval(agg_info.intervals, field_ref.get(), SortByClause::ASC),
                                       agg_info.nodes);
        }
      }

      auto &best_plan = *std::min_element(available_plans.begin(), available_plans.end(),
                                          [](const auto &l, const auto &r) { return l.cost < r.cost; });

      std::vector<std::unique_ptr<QueryExpr>> filter_nodes;
      for (const auto &n : node->inners) {
        if (best_plan.selected_nodes.count(n.get()) == 0) filter_nodes.push_back(n->template CloneAs<QueryExpr>());
      }

      if (filter_nodes.empty()) {
        return std::move(best_plan.plan);
      } else if (filter_nodes.size() == 1) {
        return std::make_unique<Filter>(std::move(best_plan.plan), std::move(filter_nodes.front()));
      } else {
        return std::make_unique<Filter>(std::move(best_plan.plan), std::make_unique<AndExpr>(std::move(filter_nodes)));
      }
    } else {
      auto full_scan_plan = MakeFullIndexFilter(node);

      std::vector<std::unique_ptr<PlanOperator>> merged_elems;
      std::vector<std::unique_ptr<QueryExpr>> elem_filter;

      auto add_filter = [&elem_filter](std::unique_ptr<PlanOperator> op) {
        if (!elem_filter.empty()) {
          std::unique_ptr<QueryExpr> filter = std::make_unique<NotExpr>(OrExpr::Create(CloneExprs(elem_filter)));

          PushDownNotExpr pdne;
          filter = Node::MustAs<QueryExpr>(pdne.Transform(std::move(filter)));
          SimplifyAndOrExpr saoe;
          filter = Node::MustAs<QueryExpr>(saoe.Transform(std::move(filter)));

          op = std::make_unique<Filter>(std::move(op), std::move(filter));
        }

        return op;
      };

      for (auto v : rest_nodes) {
        if (std::holds_alternative<QueryExpr *>(v)) {
          auto n = std::get<QueryExpr *>(v);
          auto op = add_filter(TransformExpr(n));

          merged_elems.push_back(std::move(op));
          elem_filter.push_back(n->CloneAs<QueryExpr>());
        } else {
          auto n = std::get<const FieldInfo *>(v);
          const auto &agg_info = agg_nodes.at(n);
          auto field_ref = std::make_unique<FieldRef>(n->name, n);
          auto elem = PlanFromInterval(agg_info.intervals, field_ref.get(), SortByClause::ASC);
          elem = add_filter(std::move(elem));

          merged_elems.push_back(std::move(elem));
          for (auto nn : agg_info.nodes) {
            elem_filter.push_back(nn->template CloneAs<QueryExpr>());
          }
        }
      }

      auto merge_plan = Merge::Create(std::move(merged_elems));
      auto &best_plan = const_cast<std::unique_ptr<PlanOperator> &>(std::min(
          full_scan_plan, merge_plan,
          [](const auto &l, const auto &r) { return CostModel::Transform(l.get()) < CostModel::Transform(r.get()); }));

      return std::move(best_plan);
    }
  }

  static std::vector<std::unique_ptr<QueryExpr>> CloneExprs(const std::vector<std::unique_ptr<QueryExpr>> &exprs) {
    std::vector<std::unique_ptr<QueryExpr>> result;
    result.reserve(exprs.size());

    for (const auto &e : exprs) result.push_back(e->CloneAs<QueryExpr>());
    return result;
  }

  std::unique_ptr<PlanOperator> VisitExpr(AndExpr *node) { return VisitExprImpl(node); }

  std::unique_ptr<PlanOperator> VisitExpr(OrExpr *node) { return VisitExprImpl(node); }

  static std::unique_ptr<PlanOperator> PlanFromInterval(const IntervalSet &intervals, FieldRef *field,
                                                        SortByClause::Order order) {
    std::vector<std::unique_ptr<PlanOperator>> result;
    if (order == SortByClause::ASC) {
      for (const auto &[l, r] : intervals.intervals) {
        result.push_back(std::make_unique<NumericFieldScan>(field->CloneAs<FieldRef>(), Interval(l, r), order));
      }
    } else {
      for (const auto &[l, r] : ranges::views::reverse(intervals.intervals)) {
        result.push_back(std::make_unique<NumericFieldScan>(field->CloneAs<FieldRef>(), Interval(l, r), order));
      }
    }

    return Merge::Create(std::move(result));
  }
};

}  // namespace kqir
