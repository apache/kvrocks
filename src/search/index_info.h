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
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "search_encoding.h"
#include "storage/redis_metadata.h"

namespace kqir {

struct IndexInfo;

struct FieldInfo {
  std::string name;
  IndexInfo *index = nullptr;
  std::unique_ptr<redis::IndexFieldMetadata> metadata;

  FieldInfo(std::string name, std::unique_ptr<redis::IndexFieldMetadata> &&metadata)
      : name(std::move(name)), metadata(std::move(metadata)) {}

  bool IsSortable() const { return metadata->IsSortable(); }
  bool HasIndex() const { return !metadata->noindex; }

  template <typename T>
  const T *MetadataAs() const {
    return dynamic_cast<const T *>(metadata.get());
  }
};

struct IndexInfo {
  using FieldMap = std::map<std::string, FieldInfo>;
  using MutexMap = std::map<std::string, std::mutex>;

  std::string name;
  redis::IndexMetadata metadata;
  FieldMap fields;
  mutable MutexMap field_mutexes;
  redis::IndexPrefixes prefixes;
  std::string ns;

  IndexInfo(std::string name, redis::IndexMetadata metadata, std::string ns)
      : name(std::move(name)), metadata(std::move(metadata)), ns(std::move(ns)) {}

  void Add(FieldInfo &&field) {
    auto name = field.name;
    field.index = this;
    fields.emplace(name, std::move(field));
    field_mutexes.emplace(std::piecewise_construct, std::make_tuple(name), std::make_tuple());
  }

  void LockField(const std::string &field_name) const { field_mutexes.at(field_name).lock(); }

  void UnLockField(const std::string &field_name) const { field_mutexes.at(field_name).unlock(); }
};

struct IndexMap : std::map<std::string, std::unique_ptr<IndexInfo>> {
  auto Insert(std::unique_ptr<IndexInfo> index_info) {
    auto key = ComposeNamespaceKey(index_info->ns, index_info->name, false);
    return emplace(key, std::move(index_info));
  }

  auto Find(std::string_view index, std::string_view ns) const { return find(ComposeNamespaceKey(ns, index, false)); }
};

}  // namespace kqir
