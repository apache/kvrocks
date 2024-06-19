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
#include <variant>

#include "parse_util.h"
#include "search/plan_executor.h"

namespace kqir {

struct TopNSortExecutor : ExecutorNode {
  TopNSort *sort;

  struct ComparedRow {
    RowType row;
    double val;

    ComparedRow(RowType row, double val) : row(std::move(row)), val(val) {}

    friend bool operator<(const ComparedRow &l, const ComparedRow &r) { return l.val < r.val; }
  };

  std::vector<ComparedRow> rows;
  decltype(rows)::iterator rows_iter;
  bool initialized = false;

  TopNSortExecutor(ExecutorContext *ctx, TopNSort *sort) : ExecutorNode(ctx), sort(sort) {}

  StatusOr<Result> Next() override {
    if (!initialized) {
      auto total = sort->limit->offset + sort->limit->count;
      if (total == 0) return end;

      auto v = GET_OR_RET(ctx->Get(sort->op)->Next());

      while (!std::holds_alternative<End>(v)) {
        auto &row = std::get<RowType>(v);

        auto get_order = [this](RowType &row) -> StatusOr<double> {
          auto order_val = GET_OR_RET(ctx->Retrieve(row, sort->order->field->info));
          CHECK(order_val.Is<kqir::Numeric>());
          return order_val.Get<kqir::Numeric>();
        };

        if (rows.size() == total) {
          std::make_heap(rows.begin(), rows.end());
        }

        if (rows.size() < total) {
          auto order = GET_OR_RET(get_order(row));
          rows.emplace_back(row, order);
        } else {
          auto order = GET_OR_RET(get_order(row));

          if (order < rows[0].val) {
            std::pop_heap(rows.begin(), rows.end());
            rows.back() = ComparedRow{row, order};
            std::push_heap(rows.begin(), rows.end());
          }
        }

        v = GET_OR_RET(ctx->Get(sort->op)->Next());
      }

      if (rows.size() <= sort->limit->offset) {
        return end;
      }

      std::sort(rows.begin(), rows.end());
      rows_iter = rows.begin() + static_cast<std::ptrdiff_t>(sort->limit->offset);
      initialized = true;
    }

    if (rows_iter == rows.end()) {
      return end;
    }

    auto res = rows_iter->row;
    rows_iter++;
    return res;
  }
};

}  // namespace kqir
