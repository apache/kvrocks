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

#include "db_util.h"
#include "encoding.h"
#include "search/index_info.h"
#include "search/indexer.h"
#include "search/search_encoding.h"
#include "status.h"
#include "storage/storage.h"
#include "string_util.h"

namespace redis {

struct IndexManager {
  kqir::IndexMap index_map;
  GlobalIndexer *indexer;
  engine::Storage *storage;

  IndexManager(GlobalIndexer *indexer, engine::Storage *storage) : indexer(indexer), storage(storage) {}

  Status Load(const std::string &ns) {
    // currently index cannot work in cluster mode
    if (storage->GetConfig()->cluster_enabled) {
      return Status::OK();
    }

    util::UniqueIterator iter(storage, storage->DefaultScanOptions(), ColumnFamilyID::Search);
    auto begin = SearchKey{ns, ""}.ConstructIndexMeta();

    for (iter->Seek(begin); iter->Valid(); iter->Next()) {
      auto key = iter->key();

      uint8_t ns_size = 0;
      if (!GetFixed8(&key, &ns_size)) break;
      if (ns_size != ns.size()) break;
      if (!key.starts_with(ns)) break;
      key.remove_prefix(ns_size);

      uint8_t subkey_type = 0;
      if (!GetFixed8(&key, &subkey_type)) break;
      if (subkey_type != (uint8_t)SearchSubkeyType::INDEX_META) break;

      Slice index_name;
      if (!GetSizedString(&key, &index_name)) break;

      IndexMetadata metadata;
      auto index_meta_value = iter->value();
      if (auto s = metadata.Decode(&index_meta_value); !s.ok()) {
        return {Status::NotOK, fmt::format("fail to decode index metadata for index: {}", index_name)};
      }

      auto index_key = SearchKey(ns, index_name.ToStringView());
      std::string prefix_value;
      if (auto s = storage->Get(storage->DefaultMultiGetOptions(), storage->GetCFHandle(ColumnFamilyID::Search),
                                index_key.ConstructIndexPrefixes(), &prefix_value);
          !s.ok()) {
        return {Status::NotOK, fmt::format("fail to find index prefixes for index: {}", index_name)};
      }

      IndexPrefixes prefixes;
      Slice prefix_slice = prefix_value;
      if (auto s = prefixes.Decode(&prefix_slice); !s.ok()) {
        return {Status::NotOK, fmt::format("fail to decode index prefixes for index: {}", index_name)};
      }

      auto info = std::make_unique<kqir::IndexInfo>(index_name.ToString(), metadata, ns);
      info->prefixes = prefixes;

      util::UniqueIterator field_iter(storage, storage->DefaultScanOptions(), ColumnFamilyID::Search);
      auto field_begin = index_key.ConstructFieldMeta();

      for (field_iter->Seek(field_begin); field_iter->Valid(); field_iter->Next()) {
        auto key = field_iter->key();

        uint8_t ns_size = 0;
        if (!GetFixed8(&key, &ns_size)) break;
        if (ns_size != ns.size()) break;
        if (!key.starts_with(ns)) break;
        key.remove_prefix(ns_size);

        uint8_t subkey_type = 0;
        if (!GetFixed8(&key, &subkey_type)) break;
        if (subkey_type != (uint8_t)SearchSubkeyType::FIELD_META) break;

        Slice value;
        if (!GetSizedString(&key, &value)) break;
        if (value != index_name) break;

        if (!GetSizedString(&key, &value)) break;

        auto field_name = value;
        auto field_value = iter->value();

        std::unique_ptr<IndexFieldMetadata> field_meta;
        if (auto s = IndexFieldMetadata::Decode(&field_value, field_meta); !s.ok()) {
          return {Status::NotOK, fmt::format("fail to decode index field metadata for index {}, field {}: {}",
                                             index_name, field_name, s.ToString())};
        }

        info->Add(kqir::FieldInfo(field_name.ToString(), std::move(field_meta)));
      }

      IndexUpdater updater(info.get());
      indexer->Add(updater);
      index_map.Insert(std::move(info));
    }

    return Status::OK();
  }

  Status Create(std::unique_ptr<kqir::IndexInfo> info) {
    if (storage->GetConfig()->cluster_enabled) {
      return {Status::NotOK, "currently index cannot work in cluster mode"};
    }

    if (auto iter = index_map.Find(info->name, info->ns); iter != index_map.end()) {
      return {Status::NotOK, "index already exists"};
    }

    SearchKey index_key(info->ns, info->name);
    auto cf = storage->GetCFHandle(ColumnFamilyID::Search);

    auto batch = storage->GetWriteBatchBase();

    std::string meta_val;
    info->metadata.Encode(&meta_val);
    batch->Put(cf, index_key.ConstructIndexMeta(), meta_val);

    std::string prefix_val;
    info->prefixes.Encode(&prefix_val);
    batch->Put(cf, index_key.ConstructIndexPrefixes(), prefix_val);

    for (const auto &[_, field_info] : info->fields) {
      SearchKey field_key(info->ns, info->name, field_info.name);

      std::string field_val;
      field_info.metadata->Encode(&field_val);

      batch->Put(cf, field_key.ConstructFieldMeta(), field_val);
    }

    if (auto s = storage->Write(storage->DefaultWriteOptions(), batch->GetWriteBatch()); !s.ok()) {
      return {Status::NotOK, "failed to write index metadata"};
    }

    IndexUpdater updater(info.get());
    indexer->Add(updater);
    index_map.Insert(std::move(info));

    for (auto updater : indexer->updater_list) {
      GET_OR_RET(updater.Build());
    }

    return Status::OK();
  }
};

}  // namespace redis
