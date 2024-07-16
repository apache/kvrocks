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

struct HnswVectorFieldKnnScanExecutor : ExecutorNode {
  HnswVectorFieldKnnScan *scan;
  redis::LatestSnapShot ss;
  // util::UniqueIterator iter{nullptr};
  bool initialized = false;

  IndexInfo *index;
  redis::SearchKey search_key;

  HnswVectorFieldKnnScanExecutor(ExecutorContext *ctx, HnswVectorFieldKnnScan *scan)
      : ExecutorNode(ctx),
        scan(scan),
        ss(ctx->storage),
        index(scan->field->info->index),
        search_key(index->ns, index->name, scan->field->name) {}

  StatusOr<Result> Next() override {
    // Not initialized
    if (!initialized) {
      rocksdb::ReadOptions read_options = ctx->storage->DefaultScanOptions();
      read_options.snapshot = ss.GetSnapShot();

      // snapshot read
      initialized = true;
    }

    // Invalid

    // search
    return RowType{"placeholder", {}, scan->field->info->index};
  }
};

}  // namespace kqir
