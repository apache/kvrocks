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

#include "indexer.h"

#include <variant>

#include "storage/redis_metadata.h"
#include "types/redis_hash.h"

namespace redis {

StatusOr<FieldValueRetriever> FieldValueRetriever::Create(SearchOnDataType type, std::string_view key,
                                                          engine::Storage *storage, const std::string &ns) {
  if (type == SearchOnDataType::HASH) {
    Hash db(storage, ns);
    std::string ns_key = db.AppendNamespacePrefix(key);
    HashMetadata metadata(false);
    auto s = db.GetMetadata(ns_key, &metadata);
    if (!s.ok()) return {Status::NotOK, s.ToString()};
    return FieldValueRetriever(db, metadata, key);
  } else if (type == SearchOnDataType::JSON) {
    Json db(storage, ns);
    std::string ns_key = db.AppendNamespacePrefix(key);
    JsonMetadata metadata(false);
    JsonValue value;
    auto s = db.read(ns_key, &metadata, &value);
    if (!s.ok()) return {Status::NotOK, s.ToString()};
    return FieldValueRetriever(value);
  } else {
    assert(false && "unreachable code: unexpected SearchOnDataType");
    __builtin_unreachable();
  }
}

rocksdb::Status FieldValueRetriever::Retrieve(std::string_view field, std::string *output) {
  if (std::holds_alternative<HashData>(db)) {
    auto &[hash, metadata, key] = std::get<HashData>(db);
    std::string ns_key = hash.AppendNamespacePrefix(key);
    LatestSnapShot ss(hash.storage_);
    rocksdb::ReadOptions read_options;
    read_options.snapshot = ss.GetSnapShot();
    std::string sub_key = InternalKey(ns_key, field, metadata.version, hash.storage_->IsSlotIdEncoded()).Encode();
    return hash.storage_->Get(read_options, sub_key, output);
  } else if (std::holds_alternative<JsonData>(db)) {
    auto &value = std::get<JsonData>(db);
    auto s = value.Get(field);
    if (!s.IsOK()) return rocksdb::Status::Corruption(s.Msg());
    if (s->value.size() != 1)
      return rocksdb::Status::NotFound("json value specified by the field (json path) should exist and be unique");
    *output = s->value[0].as_string();
    return rocksdb::Status::OK();
  } else {
    __builtin_unreachable();
  }
}

StatusOr<IndexUpdater::FieldValues> IndexUpdater::Record(std::string_view key, Connection *conn) {
  Database db(indexer->storage, conn->GetNamespace());

  RedisType type = kRedisNone;
  auto s = db.Type(key, &type);
  if (!s.ok()) return {Status::NotOK, s.ToString()};

  if (type != static_cast<RedisType>(on_data_type)) {
    // not the expected type, stop record
    return {Status::NotOK, "this data type cannot be indexed"};
  }

  auto retriever = GET_OR_RET(FieldValueRetriever::Create(on_data_type, key, indexer->storage, conn->GetNamespace()));

  FieldValues values;
  for (const auto &[field, info] : fields) {
    std::string value;
    auto s = retriever.Retrieve(field, &value);
    if (s.IsNotFound()) continue;
    if (!s.ok()) return {Status::NotOK, s.ToString()};

    values.emplace(field, value);
  }

  return values;
}

StatusOr<IndexUpdater::FieldValues> GlobalIndexer::Record(std::string_view key, Connection *conn) {
  auto iter = prefix_map.longest_prefix(key);
  if (iter != prefix_map.end()) {
    return iter.value()->Record(key, conn);
  }

  return {Status::NoPrefixMatched};
}

}  // namespace redis
