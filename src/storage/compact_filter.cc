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

#include "compact_filter.h"

#include <string>
#include <utility>

#include "db_util.h"
#include "encoding.h"
#include "logging.h"
#include "search/search_encoding.h"
#include "storage/redis_metadata.h"
#include "time_util.h"
#include "types/redis_bitmap.h"
#include "types/redis_timeseries.h"

namespace engine {

using rocksdb::Slice;

bool MetadataFilter::Filter([[maybe_unused]] int level, const Slice &key, const Slice &value,
                            [[maybe_unused]] std::string *new_value, [[maybe_unused]] bool *modified) const {
  Metadata metadata(kRedisNone, false);
  rocksdb::Status s = metadata.Decode(value);
  auto [ns, user_key] = ExtractNamespaceKey(key, stor_->IsSlotIdEncoded());
  if (!s.ok()) {
    warn("[compact_filter/metadata] Failed to decode, namespace: {}, key: {}, err: {}", ns, user_key, s.ToString());
    return false;
  }
  debug("[compact_filter/metadata] namespace: {}, key: {}, result: {}", ns, user_key,
        (metadata.Expired() ? "deleted" : "reserved"));
  return metadata.Expired();
}

Status SubKeyFilter::GetMetadata(const InternalKey &ikey, Metadata *metadata) const {
  auto db = stor_->GetDB();
  // storage close the would delete the column family handler and DB
  if (!db || stor_->GetCFHandles()->size() < 2) return {Status::NotOK, "storage is closed"};
  std::string metadata_key = ComposeNamespaceKey(ikey.GetNamespace(), ikey.GetKey(), stor_->IsSlotIdEncoded());

  if (cached_key_.empty() || metadata_key != cached_key_) {
    std::string bytes;
    rocksdb::Status s =
        db->Get(rocksdb::ReadOptions(), stor_->GetCFHandle(ColumnFamilyID::Metadata), metadata_key, &bytes);
    cached_key_ = std::move(metadata_key);
    if (s.ok()) {
      cached_metadata_ = std::move(bytes);
    } else if (s.IsNotFound()) {
      // metadata was deleted (perhaps through compaction or manually)
      // so here we clear the metadata
      cached_metadata_.clear();
      return {Status::NotFound, "metadata is not found"};
    } else {
      cached_key_.clear();
      cached_metadata_.clear();
      return {Status::NotOK, "fetch error: " + s.ToString()};
    }
  }
  // the metadata was not found
  if (cached_metadata_.empty()) return {Status::NotFound, "metadata is not found"};
  // the metadata is cached
  rocksdb::Status s = metadata->Decode(cached_metadata_);
  if (!s.ok()) {
    cached_key_.clear();
    return {Status::NotOK, "decode error: " + s.ToString()};
  }
  return Status::OK();
}

bool SubKeyFilter::IsMetadataExpired(const InternalKey &ikey, const Metadata &metadata) {
  // lazy delete to avoid race condition between command Expire and subkey Compaction
  // Related issue:https://github.com/apache/kvrocks/issues/1298
  //
  // `util::GetTimeStampMS() - 300000` means extending 5 minutes for expired items,
  // to prevent them from being recycled once they reach the expiration time.
  uint64_t lazy_expired_ts = util::GetTimeStampMS() - 300000;
  return metadata.IsSingleKVType()  // metadata key was overwrite by set command
         || metadata.ExpireAt(lazy_expired_ts) || ikey.GetVersion() != metadata.version;
}

rocksdb::CompactionFilter::Decision SubKeyFilter::FilterBlobByKey([[maybe_unused]] int level, const Slice &key,
                                                                  [[maybe_unused]] std::string *new_value,
                                                                  [[maybe_unused]] std::string *skip_until) const {
  InternalKey ikey(key, stor_->IsSlotIdEncoded());
  Metadata metadata(kRedisNone, false);
  Status s = GetMetadata(ikey, &metadata);
  if (s.Is<Status::NotFound>()) {
    return rocksdb::CompactionFilter::Decision::kRemove;
  }
  if (!s.IsOK()) {
    error("[compact_filter/subkey] Failed to get metadata, namespace: {}, key: {}, err: {}", ikey.GetNamespace(),
          ikey.GetKey(), s.Msg());
    return rocksdb::CompactionFilter::Decision::kKeep;
  }
  // bitmap and timeseries will be checked in Filter
  if (metadata.Type() == kRedisBitmap || metadata.Type() == kRedisTimeSeries) {
    return rocksdb::CompactionFilter::Decision::kUndetermined;
  }

  bool result = IsMetadataExpired(ikey, metadata);
  return result ? rocksdb::CompactionFilter::Decision::kRemove : rocksdb::CompactionFilter::Decision::kKeep;
}

bool SubKeyFilter::Filter([[maybe_unused]] int level, const Slice &key, const Slice &value,
                          [[maybe_unused]] std::string *new_value, [[maybe_unused]] bool *modified) const {
  InternalKey ikey(key, stor_->IsSlotIdEncoded());
  Metadata metadata(kRedisNone, false);
  Status s = GetMetadata(ikey, &metadata);
  if (s.Is<Status::NotFound>()) {
    return true;
  }
  if (!s.IsOK()) {
    error("[compact_filter/subkey] Failed to get metadata, namespace: {}, key: {}, err: {}", ikey.GetNamespace(),
          ikey.GetKey(), s.Msg());
    return false;
  }

  if (metadata.Type() == kRedisTimeSeries && redis::TimeSeries::IsTSChunkKey(ikey)) {
    TimeSeriesMetadata ts_metadata(false);
    Slice input(cached_metadata_);
    auto s = ts_metadata.Decode(&input);
    if (!s.ok()) {
      error("[compact_filter/subkey] Failed to decode timeseries metadata, namespace: {}, key: {}, err: {}",
            ikey.GetNamespace(), ikey.GetKey(), s.ToString());
      return false;
    }
    return redis::TimeSeries::IsChunkExpired(ts_metadata, value);
  }

  return IsMetadataExpired(ikey, metadata) || (metadata.Type() == kRedisBitmap && redis::Bitmap::IsEmptySegment(value));
}

bool SearchFilter::Filter([[maybe_unused]] int level, const Slice &key, [[maybe_unused]] const Slice &value,
                          [[maybe_unused]] std::string *new_value, [[maybe_unused]] bool *modified) const {
  auto db = stor_->GetDB();
  // It would delete the column family handler and DB when closing.
  if (!db || stor_->GetCFHandles()->size() < 2) return false;

  auto [ns, rest_key] = ExtractNamespaceKey(key, false);
  redis::SearchSubkeyType subkey_type = redis::SearchSubkeyType::INDEX_META;
  if (!GetFixed8(&rest_key, (std::uint8_t *)&subkey_type)) return false;
  if (subkey_type != redis::SearchSubkeyType::FIELD) return false;

  Slice index_name;
  if (!GetSizedString(&rest_key, &index_name)) return false;
  Slice field_name;
  if (!GetSizedString(&rest_key, &field_name)) return false;
  auto field_meta_key =
      redis::SearchKey(ns.ToStringView(), index_name.ToStringView(), field_name.ToStringView()).ConstructFieldMeta();

  std::string field_meta_value;
  auto s =
      db->Get(rocksdb::ReadOptions(), stor_->GetCFHandle(ColumnFamilyID::Search), field_meta_key, &field_meta_value);
  if (s.IsNotFound()) {
    // metadata of this field is not found, so we can remove the field data
    return true;
  } else if (!s.ok()) {
    error("[compact_filter/search] Failed to get field metadata, namespace: {}, index: {}, field: {}, err: {}", ns,
          index_name, field_name, s.ToString());
    return false;
  }

  std::unique_ptr<redis::IndexFieldMetadata> field_meta;
  Slice field_meta_slice(field_meta_value);
  if (auto s = redis::IndexFieldMetadata::Decode(&field_meta_slice, field_meta); !s.ok()) {
    error("[compact_filter/search] Failed to decode field metadata, namespace: {}, index: {}, field: {}, err: {}", ns,
          index_name, field_name, s.ToString());
    return false;
  }

  Slice user_key;
  if (field_meta->type == redis::IndexFieldType::TAG) {
    Slice tag_value;
    if (!GetSizedString(&rest_key, &tag_value)) return false;
    if (!GetSizedString(&rest_key, &user_key)) return false;
  } else if (field_meta->type == redis::IndexFieldType::NUMERIC) {
    double numeric_value = 0;
    if (!GetDouble(&rest_key, &numeric_value)) return false;
    if (!GetSizedString(&rest_key, &user_key)) return false;
  } else if (field_meta->type == redis::IndexFieldType::VECTOR) {
    // TODO(twice): handle vector field
    return false;
  } else {
    // unsupported field type, just keep it
    return false;
  }

  auto ns_key = ComposeNamespaceKey(ns, user_key, stor_->IsSlotIdEncoded());
  std::string metadata_value;
  s = db->Get(rocksdb::ReadOptions(), stor_->GetCFHandle(ColumnFamilyID::Metadata), ns_key, &metadata_value);
  if (s.IsNotFound()) {
    // metadata of this key is not found, so we can remove the field data
    return true;
  } else if (!s.ok()) {
    error("[compact_filter/search] Failed to get metadata, namespace: {}, key: {}, err: {}", ns, user_key,
          s.ToString());
    return false;
  }

  Metadata metadata(kRedisNone, false);
  if (auto s = metadata.Decode(metadata_value); !s.ok()) {
    error("[compact_filter/search] Failed to decode metadata, namespace: {}, key: {}, err: {}", ns, user_key,
          s.ToString());
  }

  if (metadata.Expired()) {
    // metadata is expired, so we can remove the field data
    return true;  // NOLINT
  }

  return false;
}

bool IndexFilter::Filter([[maybe_unused]] int level, const Slice &key, [[maybe_unused]] const Slice &value,
                         [[maybe_unused]] std::string *new_value, [[maybe_unused]] bool *modified) const {
  auto db = stor_->GetDB();

  auto index_key = redis::IndexInternalKey(key);
  if (index_key.type != redis::IndexKeyType::TS_LABEL) {
    // Only handle time series index for now
    return false;
  }
  auto rev_key = redis::TSRevLabelKey(key);
  auto ns = rev_key.ns;
  auto user_key = rev_key.user_key;
  auto ns_key = ComposeNamespaceKey(ns, user_key, stor_->IsSlotIdEncoded());
  std::string metadata_value;
  auto s = db->Get(rocksdb::ReadOptions(), stor_->GetCFHandle(ColumnFamilyID::Metadata), ns_key, &metadata_value);
  if (s.IsNotFound()) {
    // metadata of this key is not found, so we can remove the index
    return true;
  } else if (!s.ok()) {
    error("[compact_filter/index] Failed to get metadata, namespace: {}, key: {}, err: {}", ns, user_key, s.ToString());
    return false;
  }

  Metadata metadata(kRedisNone, false);
  if (auto s = metadata.Decode(metadata_value); !s.ok()) {
    error("[compact_filter/index] Failed to decode metadata, namespace: {}, key: {}, err: {}", ns, user_key,
          s.ToString());
  }

  if (metadata.Expired()) {
    return true;  // NOLINT
  }
  return false;
}

}  // namespace engine
