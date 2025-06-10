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

#include "compaction_checker.h"

#include "logging.h"
#include "parse_util.h"
#include "storage.h"
#include "time_util.h"

void CompactionChecker::CompactPropagateAndPubSubFiles() {
  rocksdb::CompactRangeOptions compact_opts;
  // See https://github.com/facebook/rocksdb/issues/13671
  // change_level doesn't work well with level_compaction_dynamic_level_bytes
  compact_opts.change_level = !storage_->GetConfig()->rocks_db.level_compaction_dynamic_level_bytes;
  for (const auto &cf :
       {engine::ColumnFamilyConfigs::PubSubColumnFamily(), engine::ColumnFamilyConfigs::PropagateColumnFamily()}) {
    info("[compaction checker] Start to compact the column family: {}", cf.Name());
    auto cf_handle = storage_->GetCFHandle(cf.Id());
    auto s = storage_->GetDB()->CompactRange(compact_opts, cf_handle, nullptr, nullptr);
    info("[compaction checker] Compact the column family: {} finished, result: {}", cf.Name(), s.ToString());
  }
}

void CompactionChecker::PickCompactionFilesForCf(const engine::ColumnFamilyConfig &column_family_config) {
  rocksdb::TablePropertiesCollection props;
  rocksdb::ColumnFamilyHandle *cf = storage_->GetCFHandle(column_family_config.Id());
  auto s = storage_->GetDB()->GetPropertiesOfAllTables(cf, &props);
  if (!s.ok()) {
    warn("[compaction checker] Failed to get table properties, {}", s.ToString());
    return;
  }
  // The main goal of compaction was reclaimed the disk space and removed
  // the tombstone. It seems that compaction checker was unnecessary here when
  // the live files was too few, Hard code to 1 here.
  if (props.size() <= 1) return;

  size_t max_files_to_compact = 1;
  if (props.size() / 360 > max_files_to_compact) {
    max_files_to_compact = props.size() / 360;
  }
  int64_t now = util::GetTimeStamp();

  auto force_compact_file_age = storage_->GetConfig()->force_compact_file_age;
  auto force_compact_min_ratio =
      static_cast<double>(storage_->GetConfig()->force_compact_file_min_deleted_percentage) / 100.0;

  std::string best_filename;
  double best_delete_ratio = 0;
  int64_t total_keys = 0, deleted_keys = 0;
  rocksdb::Slice start_key, stop_key, best_start_key, best_stop_key;
  for (const auto &iter : props) {
    if (max_files_to_compact == 0) return;

    uint64_t file_creation_time = iter.second->file_creation_time;
    if (file_creation_time == 0) {
      // Fallback to the file Modification time to prevent repeatedly compacting the same file,
      // file_creation_time is 0 which means the unknown condition in rocksdb
      s = rocksdb::Env::Default()->GetFileModificationTime(iter.first, &file_creation_time);
      if (!s.ok()) {
        info("[compaction checker] Failed to get the file creation time: {}, err: {}", iter.first, s.ToString());
        continue;
      }
    }

    for (const auto &property_iter : iter.second->user_collected_properties) {
      if (property_iter.first == "total_keys") {
        auto parse_result = ParseInt<int>(property_iter.second, 10);
        if (!parse_result) {
          error("[compaction checker] Parse total_keys error: {}", parse_result.Msg());
          continue;
        }
        total_keys = *parse_result;
      }
      if (property_iter.first == "deleted_keys") {
        auto parse_result = ParseInt<int>(property_iter.second, 10);
        if (!parse_result) {
          error("[compaction checker] Parse deleted_keys error: {}", parse_result.Msg());
          continue;
        }
        deleted_keys = *parse_result;
      }
      if (property_iter.first == "start_key") {
        start_key = property_iter.second;
      }
      if (property_iter.first == "stop_key") {
        stop_key = property_iter.second;
      }
    }

    if (start_key.empty() || stop_key.empty()) continue;
    double delete_ratio = static_cast<double>(deleted_keys) / static_cast<double>(total_keys);

    // pick the file according to force compact policy
    if (file_creation_time < static_cast<uint64_t>(now - force_compact_file_age) &&
        delete_ratio >= force_compact_min_ratio) {
      info("[compaction checker] Going to compact the key in file (force compact policy): {}", iter.first);
      auto s = storage_->Compact(cf, &start_key, &stop_key);
      info("[compaction checker] Compact the key in file (force compact policy): {} finished, result: {}", iter.first,
           s.ToString());
      max_files_to_compact--;
      continue;
    }

    // don't compact the SST created in 1 hour
    if (file_creation_time > static_cast<uint64_t>(now - 3600)) continue;

    // pick the file which has highest delete ratio
    if (total_keys != 0 && delete_ratio > best_delete_ratio) {
      best_delete_ratio = delete_ratio;
      best_filename = iter.first;
      best_start_key = start_key;
      start_key.clear();
      best_stop_key = stop_key;
      stop_key.clear();
    }
  }
  if (best_delete_ratio > 0.1 && !best_start_key.empty() && !best_stop_key.empty()) {
    info("[compaction checker] Going to compact the key in file: {}, delete ratio: {}", best_filename,
         best_delete_ratio);
    auto s = storage_->Compact(cf, &best_start_key, &best_stop_key);
    if (!s.ok()) {
      error("[compaction checker] Failed to do compaction: {}", s.ToString());
    }
  }
}
