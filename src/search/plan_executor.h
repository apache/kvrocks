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

#include "ir_plan.h"
#include "search/index_info.h"
#include "search/value.h"
#include "storage/storage.h"
#include "string_util.h"

namespace kqir {

struct ExecutorContext;

struct ExecutorNode {
  using KeyType = std::string;
  using ValueType = kqir::Value;
  struct RowType {
    KeyType key;
    std::map<const FieldInfo *, ValueType> fields;
    const IndexInfo *index;

    bool operator==(const RowType &another) const {
      return key == another.key && fields == another.fields && index == another.index;
    }

    bool operator!=(const RowType &another) const { return !(*this == another); }

    // for debug purpose
    friend std::ostream &operator<<(std::ostream &os, const RowType &row) {
      if (row.index) {
        os << row.key << "@" << row.index->name;
      } else {
        os << row.key;
      }
      return os << " {" << util::StringJoin(row.fields, [](const auto &v) {
               return v.first->name + ": " + v.second.ToString();
             }) << "}";
    }
  };

  static constexpr inline const struct End {
  } end{};
  friend constexpr bool operator==(End, End) noexcept { return true; }
  friend constexpr bool operator!=(End, End) noexcept { return false; }

  using Result = std::variant<End, RowType>;

  ExecutorContext *ctx;
  explicit ExecutorNode(ExecutorContext *ctx) : ctx(ctx) {}

  virtual StatusOr<Result> Next() = 0;
  virtual ~ExecutorNode() = default;
};

struct ExecutorContext {
  std::map<PlanOperator *, std::unique_ptr<ExecutorNode>> nodes;
  PlanOperator *root;
  engine::Storage *storage;
  engine::Context db_ctx;

  using Result = ExecutorNode::Result;
  using RowType = ExecutorNode::RowType;
  using KeyType = ExecutorNode::KeyType;
  using ValueType = ExecutorNode::ValueType;

  explicit ExecutorContext(PlanOperator *op);
  explicit ExecutorContext(PlanOperator *op, engine::Storage *storage);

  ExecutorNode *Get(PlanOperator *op) {
    if (auto iter = nodes.find(op); iter != nodes.end()) {
      return iter->second.get();
    }

    return nullptr;
  }

  ExecutorNode *Get(const std::unique_ptr<PlanOperator> &op) { return Get(op.get()); }

  StatusOr<Result> Next() { return Get(root)->Next(); }
  StatusOr<ValueType> Retrieve(engine::Context &ctx, RowType &row, const FieldInfo *field) const;
};

}  // namespace kqir
