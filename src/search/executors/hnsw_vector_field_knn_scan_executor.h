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
#include "search/hnsw_indexer.h"
#include "search/plan_executor.h"
#include "search/search_encoding.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"
#include "string_util.h"

namespace kqir {

struct HnswVectorFieldKnnScanExecutor : ExecutorNode {
  HnswVectorFieldKnnScan *scan;
  redis::LatestSnapShot ss;
  bool initialized = false;

  IndexInfo *index;
  redis::SearchKey search_key;
  redis::HnswVectorFieldMetadata field_metadata;
  std::vector<std::string> row_keys;
  decltype(row_keys)::iterator row_keys_iter;

  HnswVectorFieldKnnScanExecutor(ExecutorContext *ctx, HnswVectorFieldKnnScan *scan)
      : ExecutorNode(ctx),
        scan(scan),
        ss(ctx->storage),
        index(scan->field->info->index),
        search_key(index->ns, index->name, scan->field->name),
        field_metadata(*(scan->field->info->MetadataAs<redis::HnswVectorFieldMetadata>())) {}

  StatusOr<Result> Next() override {
    if (!initialized) {
      // TODO(Beihao): Consider pass snapshot as well
      auto hnsw_index = redis::HnswIndex(search_key, &field_metadata, ctx->storage);
      row_keys = GET_OR_RET(hnsw_index.KnnSearch(scan->vector, scan->k));
      row_keys_iter = row_keys.begin();
      initialized = true;
    }

    if (row_keys_iter == row_keys.end()) {
      return end;
    }

    auto key_str = *row_keys_iter;
    row_keys_iter++;
    return RowType{key_str, {}, scan->field->info->index};
  }
};

}  // namespace kqir
