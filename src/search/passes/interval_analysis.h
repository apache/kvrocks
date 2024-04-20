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
#include <memory>
#include <set>
#include <type_traits>

#include "search/interval.h"
#include "search/ir.h"
#include "search/ir_pass.h"
#include "type_util.h"

namespace kqir {

struct IntervalAnalysis : Visitor {
  std::map<Node *, std::pair<std::string, IntervalSet>> result;

  template <typename T>
  std::unique_ptr<Node> VisitImpl(std::unique_ptr<T> node) {
    node = Node::MustAs<T>(Visitor::Visit(std::move(node)));

    std::map<std::string, std::pair<IntervalSet, std::set<Node *>>> interval_map;
    for (const auto &n : node->inners) {
      IntervalSet new_interval;
      std::string new_field;

      if (auto v = dynamic_cast<NumericCompareExpr *>(n.get())) {
        new_interval = IntervalSet(v->op, v->num->val);
        new_field = v->field->name;
      } else if (auto iter = result.find(n.get()); iter != result.end()) {
        new_interval = iter->second.second;
        new_field = iter->second.first;
      } else {
        continue;
      }

      if (auto iter = interval_map.find(new_field); iter != interval_map.end()) {
        if constexpr (std::is_same_v<T, OrExpr>) {
          iter->second.first = iter->second.first | new_interval;
        } else if constexpr (std::is_same_v<T, AndExpr>) {
          iter->second.first = iter->second.first & new_interval;
        } else {
          static_assert(AlwaysFalse<T>);
        }
        iter->second.second.emplace(n.get());
      } else {
        interval_map.emplace(new_field, std::make_pair(new_interval, std::set<Node *>{n.get()}));
      }
    }

    if (interval_map.size() == 1) {
      const auto &elem = *interval_map.begin();
      result.emplace(node.get(), std::make_pair(elem.first, elem.second.first));
    }

    for (const auto &[field, info] : interval_map) {
      if (info.first.IsEmpty() || info.first.IsFull()) {
        auto iter = std::remove_if(node->inners.begin(), node->inners.end(),
                                   [&info = info](const auto &n) { return info.second.count(n.get()) == 1; });
        node->inners.erase(iter, node->inners.end());
      }

      if (info.first.IsEmpty()) {
        node->inners.emplace_back(std::make_unique<BoolLiteral>(false));
      } else if (info.first.IsFull()) {
        node->inners.emplace_back(std::make_unique<BoolLiteral>(true));
      }
    }

    return node;
  }

  std::unique_ptr<Node> Visit(std::unique_ptr<OrExpr> node) override { return VisitImpl(std::move(node)); }

  std::unique_ptr<Node> Visit(std::unique_ptr<AndExpr> node) override { return VisitImpl(std::move(node)); }
};

}  // namespace kqir
