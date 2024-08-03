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
#include "search/plan_executor.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"

namespace kqir {

struct FullIndexScanExecutor : ExecutorNode {
  FullIndexScan *scan;
  util::UniqueIterator iter{nullptr};
  const std::string *prefix_iter;

  FullIndexScanExecutor(ExecutorContext *ctx, FullIndexScan *scan)
      : ExecutorNode(ctx), scan(scan), prefix_iter(scan->index->info->prefixes.begin()) {}

  std::string NSKey(const std::string &user_key) {
    return ComposeNamespaceKey(scan->index->info->ns, user_key, ctx->storage->IsSlotIdEncoded());
  }

  StatusOr<Result> Next() override {
    if (prefix_iter == scan->index->info->prefixes.end()) {
      return end;
    }

    auto ns_key = NSKey(*prefix_iter);
    if (!iter) {
      iter = util::UniqueIterator(ctx->db_ctx, ctx->db_ctx.DefaultScanOptions(),
                                  ctx->storage->GetCFHandle(ColumnFamilyID::Metadata));
      iter->Seek(ns_key);
    }

    while (!iter->Valid() || !iter->key().starts_with(ns_key)) {
      prefix_iter++;
      if (prefix_iter == scan->index->info->prefixes.end()) {
        return end;
      }

      ns_key = NSKey(*prefix_iter);
      iter->Seek(ns_key);
    }

    auto [_, key] = ExtractNamespaceKey(iter->key(), ctx->storage->IsSlotIdEncoded());
    auto key_str = key.ToString();

    iter->Next();
    return RowType{key_str, {}, scan->index->info};
  }
};

}  // namespace kqir
