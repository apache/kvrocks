#include "compact_filter.h"
#include <glog/logging.h>
#include <string>
#include <utility>

#include "redis_bitmap.h"

namespace Engine {
using rocksdb::Slice;

bool MetadataFilter::Filter(int level,
                                    const Slice &key,
                                    const Slice &value,
                                    std::string *new_value,
                                    bool *modified) const {
  std::string ns, user_key, bytes = value.ToString();
  Metadata metadata(kRedisNone);
  rocksdb::Status s = metadata.Decode(bytes);
  ExtractNamespaceKey(key, &ns, &user_key);
  if (!s.ok()) {
    LOG(WARNING) << "[compact_filter/metadata] Failed to decode,"
                 << ", namespace: " << ns
                 << ", key: " << user_key
                 << ", err: " << s.ToString();
    return false;
  }
  DLOG(INFO) << "[compact_filter/metadata] "
             << "namespace: " << ns
             << ", key: " << user_key
             << ", result: " << (metadata.Expired() ? "deleted" : "reserved");
  return metadata.Expired();
}

bool SubKeyFilter::IsKeyExpired(const InternalKey &ikey, const Slice &value) const {
  std::string metadata_key;

  if (cf_handles_->size() < 2)  return false;  // DB recovery may trigger compaction, and cf_handlers_ would be empty
  ComposeNamespaceKey(ikey.GetNamespace(), ikey.GetKey(), &metadata_key);
  if (cached_key_.empty() || metadata_key != cached_key_) {
    std::string bytes;
    rocksdb::Status s = (*db_)->Get(rocksdb::ReadOptions(), (*cf_handles_)[1], metadata_key, &bytes);
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
  Metadata metadata(kRedisNone);
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
      || metadata.Expired()
      || ikey.GetVersion() != metadata.version) {
    return true;
  }
  return metadata.Type() == kRedisBitmap && Redis::Bitmap::IsEmptySegment(value);
}

bool SubKeyFilter::Filter(int level,
                                  const Slice &key,
                                  const Slice &value,
                                  std::string *new_value,
                                  bool *modified) const {
  InternalKey ikey(key);
  bool result = IsKeyExpired(ikey, value);
  DLOG(INFO) << "[compact_filter/subkey] "
             << "namespace: " << ikey.GetNamespace().ToString()
             << ", metadata key: " << ikey.GetKey().ToString()
             << ", subkey: " << ikey.GetSubKey().ToString()
             << ", verison: " << ikey.GetVersion()
             << ", result: " << (result ? "deleted" : "reserved");
  return result;
}
}  // namespace Engine
