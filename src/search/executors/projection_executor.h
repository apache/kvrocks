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

#include "search/plan_executor.h"

namespace kqir {

struct ProjectionExecutor : ExecutorNode {
  Projection *proj;

  ProjectionExecutor(ExecutorContext *ctx, Projection *proj) : ExecutorNode(ctx), proj(proj) {}

  StatusOr<Result> Next() override {
    auto v = GET_OR_RET(ctx->Get(proj->source)->Next());

    if (std::holds_alternative<End>(v)) return end;

    auto &row = std::get<RowType>(v);
    if (proj->select->fields.empty()) {
      for (const auto &field : row.index->fields) {
        GET_OR_RET(ctx->Retrieve(row, &field.second));
      }
    } else {
      std::map<const FieldInfo *, ValueType> res;

      for (const auto &field : proj->select->fields) {
        auto r = GET_OR_RET(ctx->Retrieve(row, field->info));
        res.emplace(field->info, std::move(r));
      }

      return RowType{row.key, res, row.index};
    }

    return v;
  }
};

}  // namespace kqir
