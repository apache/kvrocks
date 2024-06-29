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

#include <string>

#include "db_util.h"
#include "encoding.h"
#include "search/plan_executor.h"
#include "search/search_encoding.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"
#include "string_util.h"

namespace kqir {

struct TagFieldScanExecutor : ExecutorNode {
  TagFieldScan *scan;
  redis::LatestSnapShot ss;
  util::UniqueIterator iter{nullptr};

  IndexInfo *index;
  std::string index_key;
  bool case_sensitive;

  TagFieldScanExecutor(ExecutorContext *ctx, TagFieldScan *scan)
      : ExecutorNode(ctx),
        scan(scan),
        ss(ctx->storage),
        index(scan->field->info->index),
        index_key(redis::SearchKey(index->ns, index->name, scan->field->name).ConstructTagFieldData(scan->tag, {})),
        case_sensitive(scan->field->info->MetadataAs<redis::TagFieldMetadata>()->case_sensitive) {}

  bool InRangeDecode(Slice key, Slice *user_key) const {
    uint8_t ns_size = 0;
    if (!GetFixed8(&key, &ns_size)) return false;
    if (ns_size != index->ns.size()) return false;
    if (!key.starts_with(index->ns)) return false;
    key.remove_prefix(ns_size);

    uint8_t subkey_type = 0;
    if (!GetFixed8(&key, &subkey_type)) return false;
    if (subkey_type != (uint8_t)redis::SearchSubkeyType::FIELD) return false;

    Slice value;
    if (!GetSizedString(&key, &value)) return false;
    if (value != index->name) return false;

    if (!GetSizedString(&key, &value)) return false;
    if (value != scan->field->name) return false;

    if (!GetSizedString(&key, &value)) return false;
    if (case_sensitive ? value != scan->tag : !util::EqualICase(value.ToStringView(), scan->tag)) return false;

    if (!GetSizedString(&key, user_key)) return false;

    return true;
  }

  StatusOr<Result> Next() override {
    if (!iter) {
      rocksdb::ReadOptions read_options = ctx->storage->DefaultScanOptions();
      read_options.snapshot = ss.GetSnapShot();

      iter = util::UniqueIterator(ctx->storage, read_options, ctx->storage->GetCFHandle(ColumnFamilyID::Search));
      iter->Seek(index_key);
    }

    if (!iter->Valid()) {
      return end;
    }

    Slice user_key;
    if (!InRangeDecode(iter->key(), &user_key)) {
      return end;
    }

    auto key_str = user_key.ToString();

    iter->Next();
    return RowType{key_str, {}, scan->field->info->index};
  }
};

}  // namespace kqir
