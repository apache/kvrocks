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
  IndexInfo *index = nullptr;
  std::unique_ptr<redis::SearchFieldMetadata> metadata;

  FieldInfo(std::string name, std::unique_ptr<redis::SearchFieldMetadata> &&metadata)
      : name(std::move(name)), metadata(std::move(metadata)) {}
};

struct IndexInfo {
  using FieldMap = std::map<std::string, FieldInfo>;

  std::string name;
  SearchMetadata metadata;
  FieldMap fields;

  IndexInfo(std::string name, SearchMetadata metadata) : name(std::move(name)), metadata(std::move(metadata)) {}

  void Add(FieldInfo &&field) {
    const auto &name = field.name;
    field.index = this;
    fields.emplace(name, std::move(field));
  }
};

using IndexMap = std::map<std::string, IndexInfo>;

struct SemaChecker {
  const IndexMap &index_map;

  const IndexInfo *current_index = nullptr;

  using Result = std::map<const Node *, std::variant<const FieldInfo *, const IndexInfo *>>;
  Result result;

  explicit SemaChecker(const IndexMap &index_map) : index_map(index_map) {}

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
    } else if (auto v [[maybe_unused]] = dynamic_cast<LimitClause *>(node)) {
      return Status::OK();
    } else if (auto v = dynamic_cast<SortByClause *>(node)) {
      if (auto iter = current_index->fields.find(v->field->name); iter == current_index->fields.end()) {
        return {Status::NotOK, fmt::format("field `{}` not found in index `{}`", v->field->name, current_index->name)};
      } else {
        result.emplace(v->field.get(), &iter->second);
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
        return {Status::NotOK, fmt::format("field `{}` not found in index `{}`", v->field->name)};
      } else if (auto meta = dynamic_cast<redis::SearchTagFieldMetadata *>(iter->second.metadata.get()); !meta) {
        return {Status::NotOK, fmt::format("field `{}` is not a tag field", v->field->name)};
      } else {
        result.emplace(v->field.get(), &iter->second);

        if (v->tag->val.empty()) {
          return {Status::NotOK, "tag cannot be an empty string"};
        }

        if (v->tag->val.find(meta->separator) != std::string::npos) {
          return {Status::NotOK, fmt::format("tag cannot contain the separator `{}`", meta->separator)};
        }
      }
    } else if (auto v = dynamic_cast<NumericCompareExpr *>(node)) {
      if (auto iter = current_index->fields.find(v->field->name); iter == current_index->fields.end()) {
        return {Status::NotOK, fmt::format("field `{}` not found in index `{}`", v->field->name, current_index->name)};
      } else if (!dynamic_cast<redis::SearchNumericFieldMetadata *>(iter->second.metadata.get())) {
        return {Status::NotOK, fmt::format("field `{}` is not a numeric field", v->field->name)};
      } else {
        result.emplace(v->field.get(), &iter->second);
      }
    } else if (auto v = dynamic_cast<SelectExpr *>(node)) {
      for (const auto &n : v->fields) {
        if (auto iter = current_index->fields.find(n->name); iter == current_index->fields.end()) {
          return {Status::NotOK, fmt::format("field `{}` not found in index `{}`", n->name, current_index->name)};
        } else {
          result.emplace(n.get(), &iter->second);
        }
      }
    } else if (auto v [[maybe_unused]] = dynamic_cast<BoolLiteral *>(node)) {
      return Status::OK();
    } else {
      return {Status::NotOK, fmt::format("unexpected IR node type: {}", node->Name())};
    }

    return Status::OK();
  }
};

}  // namespace kqir
