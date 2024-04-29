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
#include <numeric>

#include "search/interval.h"
#include "search/ir.h"
#include "search/ir_plan.h"

namespace kqir {

// TODO: collect statistical information of index in runtime
// to optimize the cost model
struct CostModel {
  static size_t Transform(const PlanOperator *node) {
    if (auto v = dynamic_cast<const FullIndexScan *>(node)) {
      return Visit(v);
    }
    if (auto v = dynamic_cast<const NumericFieldScan *>(node)) {
      return Visit(v);
    }
    if (auto v = dynamic_cast<const TagFieldScan *>(node)) {
      return Visit(v);
    }
    if (auto v = dynamic_cast<const Filter *>(node)) {
      return Visit(v);
    }
    if (auto v = dynamic_cast<const Merge *>(node)) {
      return Visit(v);
    }

    CHECK(false) << "plan operator type not supported";
  }

  static size_t Visit(const FullIndexScan *node) { return 100; }

  static size_t Visit(const NumericFieldScan *node) {
    if (node->range.r == IntervalSet::NextNum(node->range.l)) {
      return 5;
    }

    size_t base = 10;

    if (std::isinf(node->range.l)) {
      base += 20;
    }

    if (std::isinf(node->range.r)) {
      base += 20;
    }

    return base;
  }

  static size_t Visit(const TagFieldScan *node) { return 10; }

  static size_t Visit(const Filter *node) { return Transform(node->source.get()) + 1; }

  static size_t Visit(const Merge *node) {
    return std::accumulate(node->ops.begin(), node->ops.end(), size_t(0), [](size_t res, const auto &v) {
      if (dynamic_cast<const Filter *>(v.get())) {
        res += 9;
      }
      return res + Transform(v.get());
    });
  }
};

}  // namespace kqir
