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

#include <iterator>
#include <memory>
#include <type_traits>
#include <utility>

#include "search/ir.h"
#include "search/ir_pass.h"
#include "search/passes/index_selection.h"
#include "search/passes/interval_analysis.h"
#include "search/passes/lower_to_plan.h"
#include "search/passes/push_down_not_expr.h"
#include "search/passes/recorder.h"
#include "search/passes/simplify_and_or_expr.h"
#include "search/passes/simplify_boolean.h"
#include "search/passes/sort_limit_fuse.h"
#include "type_util.h"

namespace kqir {

using PassSequence = std::vector<std::unique_ptr<Pass>>;

struct PassManager {
  static std::unique_ptr<Node> Execute(const PassSequence &seq, std::unique_ptr<Node> node) {
    for (auto &pass : seq) {
      pass->Reset();
      node = pass->Transform(std::move(node));
    }
    return node;
  }

  template <typename... Passes>
  static PassSequence Create(Passes &&...passes) {
    static_assert(std::conjunction_v<std::negation<std::is_reference<Passes>>...>);

    PassSequence result;
    result.reserve(sizeof...(passes));
    (result.push_back(std::make_unique<Passes>(std::move(passes))), ...);

    return result;
  }

  static PassSequence FullRecord(PassSequence &&seq, std::vector<std::unique_ptr<Node>> &results) {
    PassSequence res_seq;
    res_seq.push_back(std::make_unique<Recorder>(results));

    for (auto &p : seq) {
      res_seq.push_back(std::move(p));
      res_seq.push_back(std::make_unique<Recorder>(results));
    }

    return res_seq;
  }

  template <typename... PassSeqs>
  static PassSequence Merge(PassSeqs &&...seqs) {
    static_assert(std::conjunction_v<std::negation<std::is_reference<PassSeqs>>...>);
    static_assert(std::conjunction_v<std::is_same<PassSequence, RemoveCVRef<PassSeqs>>...>);

    PassSequence result;
    result.reserve((seqs.size() + ...));
    (result.insert(result.end(), std::make_move_iterator(seqs.begin()), std::make_move_iterator(seqs.end())), ...);

    return result;
  }

  static PassSequence ExprPasses() {
    return Create(SimplifyAndOrExpr{}, PushDownNotExpr{}, SimplifyBoolean{}, SimplifyAndOrExpr{});
  }
  static PassSequence NumericPasses() { return Create(IntervalAnalysis{true}, SimplifyAndOrExpr{}, SimplifyBoolean{}); }
  static PassSequence PlanPasses() { return Create(LowerToPlan{}, IndexSelection{}, SortLimitFuse{}); }

  static PassSequence Default() { return Merge(ExprPasses(), NumericPasses(), PlanPasses()); }
  static PassSequence Debug(std::vector<std::unique_ptr<Node>> &recorded) { return FullRecord(Default(), recorded); }
};

}  // namespace kqir
