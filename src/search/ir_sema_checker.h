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

#include <map>
#include <memory>

#include "ir.h"
#include "search_encoding.h"
#include "storage/redis_metadata.h"

namespace kqir {

struct IndexInfo;

struct FieldInfo {
  std::string name;
  IndexInfo *index;
  std::unique_ptr<redis::SearchFieldMetadata> metadata;
};

struct IndexInfo {
  std::string name;
  SearchMetadata metadata;
  std::map<std::string, std::unique_ptr<FieldInfo>> fields;
};

using IndexMap = std::map<std::string, IndexInfo>;

struct SemaChecker {
  const IndexMap index_map;

  const IndexInfo *current_index = nullptr;

  using Result = std::map<const Node *, std::variant<const FieldInfo *, const IndexInfo *>>;
  Result result;

  explicit SemaChecker(IndexMap index_map) : index_map(std::move(index_map)) {}

  Status Check(Node *node) {
    if (auto v = dynamic_cast<SearchStmt *>(node)) {
      auto index_name = v->index->name;
      if (auto iter = index_map.find(index_name); iter != index_map.end()) {
        current_index = &iter->second;
        result.emplace(v->index.get(), current_index);

        GET_OR_RET(Check(v->select_expr.get()));
        if (v->query_expr) GET_OR_RET(Check(v->query_expr.get()));
        if (v->limit) GET_OR_RET(Check(v->limit.get()));
        if (v->sort_by) GET_OR_RET(Check(v->sort_by.get()));
      } else {
        return {Status::NotOK, fmt::format("index `{}` not found", index_name)};
      }
    } else if (auto v = dynamic_cast<Limit *>(node)) {
      return Status::OK();
    } else if (auto v = dynamic_cast<SortBy *>(node)) {
      if (auto iter = current_index->fields.find(v->field->name); iter == current_index->fields.end()) {
        return {Status::NotOK, fmt::format("field `{}` not found", v->field->name)};
      } else {
        result.emplace(v->field.get(), iter->second.get());
      }
    } else if (auto v = dynamic_cast<AndExpr *>(node)) {
      for (const auto &n : v->inners) {
        GET_OR_RET(Check(n.get()));
      }
    } else if (auto v = dynamic_cast<OrExpr *>(node)) {
      for (const auto &n : v->inners) {
        GET_OR_RET(Check(n.get()));
      }
    } else if (auto v = dynamic_cast<NotExpr *>(node)) {
      GET_OR_RET(Check(v->inner.get()));
    } else if (auto v = dynamic_cast<TagContainExpr *>(node)) {
      if (auto iter = current_index->fields.find(v->field->name); iter == current_index->fields.end()) {
        return {Status::NotOK, fmt::format("field `{}` not found", v->field->name)};
      } else if (!dynamic_cast<redis::SearchTagFieldMetadata *>(iter->second->metadata.get())) {
        return {Status::NotOK, "field `{}` is not a tag field"};
      } else {
        result.emplace(v->field.get(), iter->second.get());
      }
    } else if (auto v = dynamic_cast<NumericCompareExpr *>(node)) {
      if (auto iter = current_index->fields.find(v->field->name); iter == current_index->fields.end()) {
        return {Status::NotOK, fmt::format("field `{}` not found", v->field->name)};
      } else if (!dynamic_cast<redis::SearchNumericFieldMetadata *>(iter->second->metadata.get())) {
        return {Status::NotOK, "field `{}` is not a numeric field"};
      } else {
        result.emplace(v->field.get(), iter->second.get());
      }
    } else if (auto v = dynamic_cast<SelectExpr *>(node)) {
      for (const auto &n : v->fields) {
        if (auto iter = current_index->fields.find(n->name); iter == current_index->fields.end()) {
          return {Status::NotOK, fmt::format("field `{}` not found", n->name)};
        } else {
          result.emplace(n.get(), iter->second.get());
        }
      }
    } else if (auto v = dynamic_cast<BoolLiteral *>(node)) {
      return Status::OK();
    } else {
      return {Status::NotOK, fmt::format("unexpected IR node type: {}", node->Name())};
    }

    return Status::OK();
  }
};

}  // namespace kqir
