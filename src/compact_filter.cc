#include "compact_filter.h"
#include <string>
#include <utility>
#include <glog/logging.h>
#include "redis_bitmap.h"

namespace Engine {
using rocksdb::Slice;

bool MetadataFilter::Filter(int level,
                                    const Slice &key,
                                    const Slice &value,
                                    std::string *new_value,
                                    bool *modified) const {
  std::string ns, user_key, bytes = value.ToString();
  Metadata metadata(kRedisNone, false);
  rocksdb::Status s = metadata.Decode(bytes);
  ExtractNamespaceKey(key, &ns, &user_key, stor_->IsSlotIdEncoded());
  if (!s.ok()) {
    LOG(WARNING) << "[compact_filter/metadata] Failed to decode,"
                 << ", namespace: " << ns
                 << ", key: " << user_key
                 << ", err: " << s.ToString();
    return false;
  }

  bool is_filter = metadata.Expired();

  if (is_filter && metadata.Type() != kRedisString) {
    is_filter = stor_->IsDBNoExpire() ? false : IfRemoveExpiredRecord(key, metadata.Type());
  }

  DLOG(INFO) << "[compact_filter/metadata] "
             << "namespace: " << ns
             << ", key: " << user_key
             << ", result: " << (is_filter ? "deleted" : "reserved");

  return is_filter;
}

bool MetadataFilter::IfRemoveExpiredRecord(const Slice &ns_key, RedisType type) const {
  auto exipre_guard = stor_->ReadExpireLockGuard();
  if (stor_->IsDBNoExpire()) {
    /* Slave must not be allowed to expire meta record of complex key */
    return false;
  }

  std::string value;
  auto db = stor_->GetDB();
  LockGuard rw_guard(stor_->GetLockManager(), ns_key);
  rocksdb::Status s = db->Get(rocksdb::ReadOptions(), stor_->GetCFHandle(kMetadataColumnFamilyName), ns_key, &value);
  if (s.IsNotFound()) {
    /* The key has been deleted */
    return true;
  } else if (!s.ok()) {
    /* Read DB err, keep the record */
    return false;
  }

  Metadata metadata(kRedisNone, false);
  metadata.Decode(value);
  if (type != kRedisNone && type != metadata.Type()) {
    /* The key has gone and was rewritten with other type */
    return true;
  }

  if (!metadata.Expired()) {
    /* TTL of the key was extended */
    return true;
  }

  stor_->ExpdelSpeedLimit(ns_key.size());

  s = stor_->Delete(rocksdb::WriteOptions(), stor_->GetCFHandle(kMetadataColumnFamilyName), ns_key);
  return s.ok() ? true : false;
}

bool SubKeyFilter::IsKeyExpired(const InternalKey &ikey, const Slice &value) const {
  std::string metadata_key;

  auto db = stor_->GetDB();
  const auto cf_handles = stor_->GetCFHandles();
  // storage close the would delete the column familiy handler and DB
  if (!db || cf_handles->size() < 2)  return false;

  ComposeNamespaceKey(ikey.GetNamespace(), ikey.GetKey(), &metadata_key, stor_->IsSlotIdEncoded());

  if (cached_key_.empty() || metadata_key != cached_key_) {
    std::string bytes;
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), (*cf_handles)[1], metadata_key, &bytes);
    cached_key_ = std::move(metadata_key);
    if (s.ok()) {
      cached_metadata_ = std::move(bytes);
    } else if (s.IsNotFound()) {
      // metadata was deleted(perhaps compaction or manual)
      // clear the metadata
      cached_metadata_.clear();
      return true;
    } else {
      LOG(ERROR) << "[compact_filter/subkey] Failed to fetch metadata"
                 << ", namespace: " << ikey.GetNamespace().ToString()
                 << ", key: " << ikey.GetKey().ToString()
                 << ", err: " << s.ToString();
      cached_key_.clear();
      cached_metadata_.clear();
      return false;
    }
  }
  // the metadata was not found
  if (cached_metadata_.empty()) return true;
  // the metadata is cached
  Metadata metadata(kRedisNone, false);
  rocksdb::Status s = metadata.Decode(cached_metadata_);
  if (!s.ok()) {
    cached_key_.clear();
    LOG(ERROR) << "[compact_filter/subkey] Failed to decode metadata"
               << ", namespace: " << ikey.GetNamespace().ToString()
               << ", key: " << ikey.GetKey().ToString()
               << ", err: " << s.ToString();
    return false;
  }
  if (metadata.Type() == kRedisString  // metadata key was overwrite by set command
      || metadata.Empty()
      || ikey.GetVersion() != metadata.version) {
    return true;
  }
  return metadata.Type() == kRedisBitmap && Redis::Bitmap::IsEmptySegment(value);
}

rocksdb::CompactionFilter::Decision SubKeyFilter::FilterV2(int level,
                                                                  const Slice& key,
                                                                  ValueType value_type,
                                                                  const Slice& value,
                                                                  std::string* new_value,
                                                                  std::string* skip_until) const {
  InternalKey ikey(key, stor_->IsSlotIdEncoded());
  bool result = IsKeyExpired(ikey, value);
  DLOG(INFO) << "[compact_filter/subkey] "
             << "namespace: " << ikey.GetNamespace().ToString()
             << ", metadata key: " << ikey.GetKey().ToString()
             << ", subkey: " << ikey.GetSubKey().ToString()
             << ", verison: " << ikey.GetVersion()
             << ", result: " << (result ? "kRemoveAndSkipUntil" : "kKeep");
  if (!result) {
    return Decision::kKeep;
  }

  std::string ns_key;
  ComposeNamespaceKey(ikey.GetNamespace(), ikey.GetKey(), &ns_key, stor_->IsSlotIdEncoded());
  InternalKey(ns_key, "", ikey.GetVersion() + 1, stor_->IsSlotIdEncoded()).Encode(skip_until);
  return Decision::kRemoveAndSkipUntil;
}
}  // namespace Engine
