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
#include "search/ir.h"
#include "search/ir_sema_checker.h"
#include "search/passes/manager.h"
#include "search/plan_executor.h"
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
    auto no_txn_ctx = engine::Context::NoTransactionContext(storage);
    util::UniqueIterator iter(no_txn_ctx, no_txn_ctx.DefaultScanOptions(), ColumnFamilyID::Search);
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
        return {Status::NotOK, fmt::format("fail to decode index metadata for index {}: {}", index_name, s.ToString())};
      }

      auto index_key = SearchKey(ns, index_name.ToStringView());
      std::string prefix_value;
      if (auto s = storage->Get(no_txn_ctx, no_txn_ctx.DefaultMultiGetOptions(),
                                storage->GetCFHandle(ColumnFamilyID::Search), index_key.ConstructIndexPrefixes(),
                                &prefix_value);
          !s.ok()) {
        return {Status::NotOK, fmt::format("fail to find index prefixes for index {}: {}", index_name, s.ToString())};
      }

      IndexPrefixes prefixes;
      Slice prefix_slice = prefix_value;
      if (auto s = prefixes.Decode(&prefix_slice); !s.ok()) {
        return {Status::NotOK, fmt::format("fail to decode index prefixes for index {}: {}", index_name, s.ToString())};
      }

      auto info = std::make_unique<kqir::IndexInfo>(index_name.ToString(), metadata, ns);
      info->prefixes = prefixes;

      auto alias_iter = util::UniqueIterator(no_txn_ctx, no_txn_ctx.DefaultScanOptions(), ColumnFamilyID::Search);
      auto alias_prefix = index_key.ConstructAliasKey("");

      for (alias_iter->Seek(alias_prefix); alias_iter->Valid(); alias_iter->Next()) {
        auto key = alias_iter->key();

        uint8_t ns_size = 0;
        if (!GetFixed8(&key, &ns_size)) break;
        if (ns_size != ns.size()) break;
        if (!key.starts_with(ns)) break;
        key.remove_prefix(ns_size);

        uint8_t subkey_type = 0;
        if (!GetFixed8(&key, &subkey_type)) break;
        if (subkey_type != (uint8_t)SearchSubkeyType::FIELD_ALIAS) break;

        Slice alias_name_slice;
        if (!GetSizedString(&key, &alias_name_slice)) break;

        Slice index_name_slice;
        if (!GetSizedString(&key, &index_name_slice)) break;

        info->aliases.emplace_back(alias_name_slice.ToStringView());
      }

      if (auto s = alias_iter->status(); !s.ok()) {
        return {Status::NotOK, fmt::format("fail to load aliases: {}", s.ToString())};
      }

      util::UniqueIterator field_iter(no_txn_ctx, no_txn_ctx.DefaultScanOptions(), ColumnFamilyID::Search);
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
        auto field_value = field_iter->value();

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

    if (auto s = iter->status(); !s.ok()) {
      return {Status::NotOK, fmt::format("fail to load index metadata: {}", s.ToString())};
    }

    return Status::OK();
  }

  Status Create(engine::Context &ctx, std::unique_ptr<kqir::IndexInfo> info) {
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
    auto s = batch->Put(cf, index_key.ConstructIndexMeta(), meta_val);
    if (!s.ok()) {
      return {Status::NotOK, s.ToString()};
    }

    std::string prefix_val;
    info->prefixes.Encode(&prefix_val);
    s = batch->Put(cf, index_key.ConstructIndexPrefixes(), prefix_val);
    if (!s.ok()) {
      return {Status::NotOK, s.ToString()};
    }

    for (const auto &[_, field_info] : info->fields) {
      SearchKey field_key(info->ns, info->name, field_info.name);

      std::string field_val;
      field_info.metadata->Encode(&field_val);

      s = batch->Put(cf, field_key.ConstructFieldMeta(), field_val);
      if (!s.ok()) {
        return {Status::NotOK, s.ToString()};
      }
    }

    if (auto s = storage->Write(ctx, storage->DefaultWriteOptions(), batch->GetWriteBatch()); !s.ok()) {
      return {Status::NotOK, fmt::format("failed to write index metadata: {}", s.ToString())};
    }

    IndexUpdater updater(info.get());
    indexer->Add(updater);
    index_map.Insert(std::move(info));

    for (auto updater : indexer->updater_list) {
      GET_OR_RET(updater.Build(ctx));
    }

    return Status::OK();
  }

  StatusOr<std::unique_ptr<kqir::PlanOperator>> GeneratePlan(std::unique_ptr<kqir::Node> ir,
                                                             const std::string &ns) const {
    kqir::SemaChecker sema_checker(index_map);
    sema_checker.ns = ns;

    GET_OR_RET(sema_checker.Check(ir.get()));

    auto plan_ir = kqir::PassManager::Execute(kqir::PassManager::Default(), std::move(ir));
    std::unique_ptr<kqir::PlanOperator> plan_op;
    if (plan_op = kqir::Node::As<kqir::PlanOperator>(std::move(plan_ir)); !plan_op) {
      return {Status::NotOK, "failed to convert the query to plan operators"};
    }

    return plan_op;
  }

  StatusOr<std::vector<kqir::ExecutorContext::RowType>> Search(std::unique_ptr<kqir::Node> ir,
                                                               const std::string &ns) const {
    auto plan_op = GET_OR_RET(GeneratePlan(std::move(ir), ns));

    kqir::ExecutorContext executor_ctx(plan_op.get(), storage);

    std::vector<kqir::ExecutorContext::RowType> results;

    auto iter_res = GET_OR_RET(executor_ctx.Next());
    while (!std::holds_alternative<kqir::ExecutorNode::End>(iter_res)) {
      results.push_back(std::get<kqir::ExecutorContext::RowType>(iter_res));

      iter_res = GET_OR_RET(executor_ctx.Next());
    }

    return results;
  }

  Status Drop(engine::Context &ctx, std::string_view index_name, const std::string &ns) {
    auto iter = index_map.Find(index_name, ns);
    if (iter == index_map.end()) {
      return {Status::NotOK, "index not found"};
    }

    auto info = iter->second.get();

    SearchKey index_key(info->ns, info->name);
    auto cf = storage->GetCFHandle(ColumnFamilyID::Search);

    auto batch = storage->GetWriteBatchBase();

    std::vector<std::string> aliases_copy = info->aliases;

    for (const auto &alias_name : aliases_copy) {
      auto s = DelAlias(ctx, alias_name, ns);
      if (!s.IsOK()) {
        return Status::NotOK;
      }
    }
    indexer->Remove(info);

    auto s = batch->Delete(cf, index_key.ConstructIndexMeta());
    if (!s.ok()) {
      return {Status::NotOK, s.ToString()};
    }
    s = batch->Delete(cf, index_key.ConstructIndexPrefixes());
    if (!s.ok()) {
      return {Status::NotOK, s.ToString()};
    }

    auto begin = index_key.ConstructAllFieldMetaBegin();
    auto end = index_key.ConstructAllFieldMetaEnd();
    s = batch->DeleteRange(cf, begin, end);
    if (!s.ok()) {
      return {Status::NotOK, s.ToString()};
    }

    begin = index_key.ConstructAllFieldDataBegin();
    end = index_key.ConstructAllFieldDataEnd();
    s = batch->DeleteRange(cf, begin, end);
    if (!s.ok()) {
      return {Status::NotOK, s.ToString()};
    }

    auto no_txn_ctx = engine::Context::NoTransactionContext(storage);
    if (auto s = storage->Write(no_txn_ctx, storage->DefaultWriteOptions(), batch->GetWriteBatch()); !s.ok()) {
      return {Status::NotOK, fmt::format("failed to delete index metadata and data: {}", s.ToString())};
    }

    index_map.erase(iter);

    return Status::OK();
  }

  Status AddAlias(engine::Context &ctx, std::string_view alias_name, std::string_view index_name,
                  const std::string &ns) {
    auto iter = index_map.Find(index_name, ns);
    if (iter == index_map.end()) {
      return {Status::NotOK, "Target index not found"};
    }

    auto info = iter->second.get();

    // Checks if the alias already exists in the index
    auto it = std::find(info->aliases.begin(), info->aliases.end(), alias_name);
    if (it != info->aliases.end()) {
      return {Status::NotOK, fmt::format("Alias already exists inside index")};
    }

    auto cf = storage->GetCFHandle(ColumnFamilyID::Search);
    auto batch = storage->GetWriteBatchBase();
    SearchKey alias_key(ns, "");

    auto no_txn_ctx = engine::Context::NoTransactionContext(storage);

    std::string retrieve_index;
    auto s = storage->Get(no_txn_ctx, no_txn_ctx.DefaultMultiGetOptions(), cf, alias_key.ConstructAliasKey(alias_name),
                          &retrieve_index);
    if (s.ok()) {
      return {Status::NotOK, fmt::format("Alias already exists")};
    }

    s = batch->Put(cf, alias_key.ConstructAliasKey(alias_name), std::string(index_name));
    if (!s.ok()) {
      return {Status::NotOK, s.ToString()};
    }

    s = storage->Write(ctx, storage->DefaultWriteOptions(), batch->GetWriteBatch());
    if (!s.ok()) {
      return {Status::NotOK, fmt::format("Failed to add alias metadata: {}", s.ToString())};
    }

    info->aliases.emplace_back(alias_name);

    return Status::OK();
  }

  Status DelAlias(engine::Context &ctx, std::string_view alias_name, const std::string &ns) {
    auto cf = storage->GetCFHandle(ColumnFamilyID::Search);
    auto batch = storage->GetWriteBatchBase();

    auto no_txn_ctx = engine::Context::NoTransactionContext(storage);

    std::string retrieve_index;
    SearchKey alias_key(ns, "");

    auto s = storage->Get(no_txn_ctx, no_txn_ctx.DefaultMultiGetOptions(), cf, alias_key.ConstructAliasKey(alias_name),
                          &retrieve_index);
    if (!s.ok() || retrieve_index == "") {
      return {Status::NotOK, fmt::format("Alias does not exist")};
    }
    LOG(INFO) << retrieve_index;

    s = batch->Delete(cf, alias_key.ConstructAliasKey(alias_name));
    if (!s.ok()) {
      return {Status::NotOK, s.ToString()};
    }

    s = storage->Write(ctx, storage->DefaultWriteOptions(), batch->GetWriteBatch());
    if (!s.ok()) {
      return {Status::NotOK, fmt::format("Failed to delete alias metadata: {}", s.ToString())};
    }

    auto iter = index_map.Find(retrieve_index, ns);
    if (iter == index_map.end()) {
      return {Status::NotOK, "index not found"};
    }

    auto info = iter->second.get();

    auto it = std::find(info->aliases.begin(), info->aliases.end(), alias_name);
    if (it == info->aliases.end()) {
      return Status::NotOK;
    }
    info->aliases.erase(it);

    return Status::OK();
  }

  Status UpdateAlias(engine::Context &ctx, std::string_view alias_name, std::string_view index_name,
                     const std::string &ns) {
    auto iter = index_map.Find(index_name, ns);
    if (iter == index_map.end()) {
      return {Status::NotOK, "index not found"};
    }
    auto info = iter->second.get();
    auto it = std::find(info->aliases.begin(), info->aliases.end(), alias_name);
    if (it != info->aliases.end()) {
      return {Status::NotOK, fmt::format("Alias already in index.")};
    }

    auto cf = storage->GetCFHandle(ColumnFamilyID::Search);
    auto batch = storage->GetWriteBatchBase();

    SearchKey alias_key(ns, "");

    auto no_txn_ctx = engine::Context::NoTransactionContext(storage);

    std::string retrieve_index;
    auto s = storage->Get(no_txn_ctx, no_txn_ctx.DefaultMultiGetOptions(), cf, alias_key.ConstructAliasKey(alias_name),
                          &retrieve_index);
    if (s.ok()) {
      auto s = DelAlias(ctx, alias_name, ns);
      if (!s.IsOK()) {
        return Status::NotOK;
      }
    }
    if (!s.ok()) {
      return {Status::NotOK, fmt::format("Alias does not exist")};
    }

    s = batch->Put(cf, alias_key.ConstructAliasKey(alias_name), std::string(index_name));
    if (!s.ok()) {
      return {Status::NotOK, s.ToString()};
    }

    s = storage->Write(ctx, storage->DefaultWriteOptions(), batch->GetWriteBatch());
    if (!s.ok()) {
      return {Status::NotOK, fmt::format("Failed to update alias metadata: {}", s.ToString())};
    }
    info->aliases.emplace_back(alias_name);

    return Status::OK();
  }
};

}  // namespace redis
