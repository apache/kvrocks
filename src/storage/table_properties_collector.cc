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

#include "table_properties_collector.h"

#include <memory>
#include <utility>

#include "encoding.h"
#include "redis_metadata.h"
#include "server/server.h"

rocksdb::Status CompactOnExpiredCollector::AddUserKey(const rocksdb::Slice &key, const rocksdb::Slice &value,
                                                      rocksdb::EntryType entry_type, rocksdb::SequenceNumber,
                                                      uint64_t) {
  if (start_key_.empty()) {
    start_key_ = key.ToString();
  }
  stop_key_ = key.ToString();
  total_keys_ += 1;
  if (entry_type == rocksdb::kEntryDelete) {
    deleted_keys_ += 1;
    return rocksdb::Status::OK();
  }

  if (cf_name_ != "metadata") {
    return rocksdb::Status::OK();
  }

  if (entry_type != rocksdb::kEntryPut || value.size() < 5) {
    return rocksdb::Status::OK();
  }

  Metadata metadata(RedisType::kRedisNone);
  auto s = metadata.Decode(value);
  if (!s.ok()) return rocksdb::Status::OK();

  total_keys_ += metadata.size;
  if (metadata.ExpireAt(Server::GetCachedUnixTime() * 1000)) {
    deleted_keys_ += metadata.size + 1;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status CompactOnExpiredCollector::Finish(rocksdb::UserCollectedProperties *properties) {
  properties->emplace("total_keys", std::to_string(total_keys_));
  properties->emplace("deleted_keys", std::to_string(deleted_keys_));
  properties->emplace("start_key", start_key_);
  properties->emplace("stop_key", stop_key_);
  return rocksdb::Status::OK();
}

rocksdb::UserCollectedProperties CompactOnExpiredCollector::GetReadableProperties() const {
  rocksdb::UserCollectedProperties properties;
  properties.emplace("total_keys", std::to_string(total_keys_));
  properties.emplace("deleted_keys", std::to_string(deleted_keys_));
  return properties;
}

bool CompactOnExpiredCollector::NeedCompact() const {
  if (total_keys_ == 0) return false;
  return static_cast<float>(deleted_keys_) / static_cast<float>(total_keys_) > trigger_threshold_;
}

rocksdb::TablePropertiesCollector *CompactOnExpiredTableCollectorFactory::CreateTablePropertiesCollector(
    rocksdb::TablePropertiesCollectorFactory::Context context) {
  return new CompactOnExpiredCollector(cf_name_, trigger_threshold_);
}

std::shared_ptr<CompactOnExpiredTableCollectorFactory> NewCompactOnExpiredTableCollectorFactory(
    const std::string &cf_name, float trigger_threshold) {
  return std::make_shared<CompactOnExpiredTableCollectorFactory>(cf_name, trigger_threshold);
}
