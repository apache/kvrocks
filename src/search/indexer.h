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

#include <tsl/htrie_map.h>

#include <deque>
#include <map>
#include <utility>
#include <variant>

#include "commands/commander.h"
#include "config/config.h"
#include "indexer.h"
#include "search/search_encoding.h"
#include "server/server.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"
#include "types/redis_hash.h"
#include "types/redis_json.h"

namespace redis {

struct GlobalIndexer;

struct FieldValueRetriever {
  struct HashData {
    Hash hash;
    HashMetadata metadata;
    std::string_view key;

    HashData(Hash hash, HashMetadata metadata, std::string_view key)
        : hash(std::move(hash)), metadata(std::move(metadata)), key(key) {}
  };
  using JsonData = JsonValue;

  using Variant = std::variant<HashData, JsonData>;
  Variant db;

  static StatusOr<FieldValueRetriever> Create(SearchOnDataType type, std::string_view key, engine::Storage *storage,
                                              const std::string &ns);

  explicit FieldValueRetriever(Hash hash, HashMetadata metadata, std::string_view key)
      : db(std::in_place_type<HashData>, std::move(hash), std::move(metadata), key) {}

  explicit FieldValueRetriever(JsonValue json) : db(std::in_place_type<JsonData>, std::move(json)) {}

  rocksdb::Status Retrieve(std::string_view field, std::string *output);
};

struct IndexUpdater {
  using FieldValues = std::map<std::string, std::string>;

  SearchOnDataType on_data_type;
  std::vector<std::string> prefixes;
  std::map<std::string, std::unique_ptr<SearchFieldMetadata>> fields;
  GlobalIndexer *indexer = nullptr;

  StatusOr<FieldValues> Record(std::string_view key, Connection *conn);
};

struct GlobalIndexer {
  std::deque<IndexUpdater> updaters;
  tsl::htrie_map<char, IndexUpdater *> prefix_map;

  engine::Storage *storage = nullptr;

  explicit GlobalIndexer(engine::Storage *storage) : storage(storage) {}
  void Add(IndexUpdater updater);
  StatusOr<IndexUpdater::FieldValues> Record(std::string_view key, Connection *conn);
};

}  // namespace redis
