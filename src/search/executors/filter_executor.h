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

#include <variant>

#include "parse_util.h"
#include "search/ir.h"
#include "search/plan_executor.h"
#include "search/search_encoding.h"
#include "string_util.h"

namespace kqir {

struct QueryExprEvaluator {
  ExecutorContext *ctx;
  ExecutorNode::RowType &row;

  StatusOr<bool> Transform(QueryExpr *e) const {
    if (auto v = dynamic_cast<AndExpr *>(e)) {
      return Visit(v);
    }
    if (auto v = dynamic_cast<OrExpr *>(e)) {
      return Visit(v);
    }
    if (auto v = dynamic_cast<NotExpr *>(e)) {
      return Visit(v);
    }
    if (auto v = dynamic_cast<NumericCompareExpr *>(e)) {
      return Visit(v);
    }
    if (auto v = dynamic_cast<TagContainExpr *>(e)) {
      return Visit(v);
    }

    CHECK(false) << "unreachable";
  }

  StatusOr<bool> Visit(AndExpr *v) const {
    for (const auto &n : v->inners) {
      if (!GET_OR_RET(Transform(n.get()))) return false;
    }

    return true;
  }

  StatusOr<bool> Visit(OrExpr *v) const {
    for (const auto &n : v->inners) {
      if (GET_OR_RET(Transform(n.get()))) return true;
    }

    return false;
  }

  StatusOr<bool> Visit(NotExpr *v) const { return !GET_OR_RET(Transform(v->inner.get())); }

  StatusOr<bool> Visit(TagContainExpr *v) const {
    auto val = GET_OR_RET(ctx->Retrieve(row, v->field->info));
    auto meta = v->field->info->MetadataAs<redis::TagFieldMetadata>();

    auto split = util::Split(val, std::string(1, meta->separator));
    return std::find(split.begin(), split.end(), v->tag->val) != split.end();
  }

  StatusOr<bool> Visit(NumericCompareExpr *v) const {
    auto l_str = GET_OR_RET(ctx->Retrieve(row, v->field->info));

    // TODO: reconsider how to handle failure case here
    auto l = GET_OR_RET(ParseFloat(l_str));
    auto r = v->num->val;

    switch (v->op) {
      case NumericCompareExpr::EQ:
        return l == r;
      case NumericCompareExpr::NE:
        return l != r;
      case NumericCompareExpr::LT:
        return l < r;
      case NumericCompareExpr::LET:
        return l <= r;
      case NumericCompareExpr::GT:
        return l > r;
      case NumericCompareExpr::GET:
        return l >= r;
      default:
        CHECK(false) << "unreachable";
        __builtin_unreachable();
    }
  }
};

struct FilterExecutor : ExecutorNode {
  Filter *filter;

  FilterExecutor(ExecutorContext *ctx, Filter *filter) : ExecutorNode(ctx), filter(filter) {}

  StatusOr<Result> Next() override {
    while (true) {
      auto v = GET_OR_RET(ctx->Get(filter->source)->Next());

      if (std::holds_alternative<End>(v)) return end;

      QueryExprEvaluator eval{ctx, std::get<RowType>(v)};

      bool res = GET_OR_RET(eval.Transform(filter->filter_expr.get()));

      if (res) {
        return v;
      }
    }
  }
};

}  // namespace kqir
