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
#include <string>

#include "search_encoding.h"

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

  std::string name;
  redis::IndexMetadata metadata;
  FieldMap fields;
  redis::IndexPrefixes prefixes;
  std::string ns;

  IndexInfo(std::string name, redis::IndexMetadata metadata) : name(std::move(name)), metadata(std::move(metadata)) {}

  void Add(FieldInfo &&field) {
    const auto &name = field.name;
    field.index = this;
    fields.emplace(name, std::move(field));
  }
};

using IndexMap = std::map<std::string, std::unique_ptr<IndexInfo>>;

}  // namespace kqir
