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

#include "cluster/migrate_iterator.h"

#include "db_util.h"
#include "storage/redis_db.h"

MigrateIterator::MigrateIterator(engine::Storage *storage, const rocksdb::ReadOptions &read_options)
    : metadata_cf_(storage->GetCFHandle(engine::kMetadataColumnFamilyName)),
      subkey_cf_(storage->GetCFHandle(engine::kSubkeyColumnFamilyName)),
      zset_score_cf_(storage->GetCFHandle(engine::kZSetScoreColumnFamilyName)),
      metadata_iter_(util::UniqueIterator(storage, read_options, metadata_cf_)),
      subdata_iter_(util::UniqueIterator(storage, read_options, subkey_cf_)),
      valid_(false),
      metadata_(RedisType::kRedisNone, false) {}

bool MigrateIterator::Valid() const { return valid_; }

void MigrateIterator::Seek(const rocksdb::Slice &target) {
  items_.clear();
  log_data_.clear();
  metadata_iter_->Reset();
  subdata_iter_->Reset();
  metakey_prefix_.clear();

  metadata_iter_->Seek(target);
  valid_ = metadata_iter_->Valid() && metadata_iter_->key().starts_with(target);
  if (valid_) {
    metakey_prefix_ = target.ToString();
    findMetaData();
  }
}

void MigrateIterator::Next() {
  assert(valid_);
  valid_ = false;
  items_.clear();

  if (subdata_iter_->Valid()) {
    subdata_iter_->Next();
    valid_ = subdata_iter_->Valid() && subdata_iter_->key().starts_with(subkey_prefix_);
    if (valid_) {
      findSubData();
    } else {
      subdata_iter_->Reset();
    }
  }

  if (!valid_ && metadata_iter_->Valid()) {
    metadata_iter_->Next();
    valid_ = metadata_iter_->Valid() && metadata_iter_->key().starts_with(metakey_prefix_);
    if (valid_) {
      findMetaData();
    }
  }
}

const std::vector<MigrateItem> &MigrateIterator::GetItems() const {
  assert(valid_);
  return items_;
}

std::string MigrateIterator::GetLogData() const {
  assert(valid_);
  return log_data_;
}

void MigrateIterator::findMetaData() {
  assert(metadata_iter_->Valid());
  Metadata metadata(kRedisNone, false /* generate_version */);
  metadata.Decode(metadata_iter_->value().ToString());
  RedisType redis_type = metadata.Type();
  metadata_ = metadata;

  redis::WriteBatchLogData log_data(redis_type);
  if (redis_type == RedisType::kRedisList) {
    log_data.SetArguments({std::to_string(RedisCommand::kRedisCmdRPush)});
  }

  log_data_ = log_data.Encode();

  items_.push_back(MigrateItem{metadata_cf_, metadata_iter_->key().ToString(), metadata_iter_->value().ToString()});

  subdata_iter_->Reset();

  if (redis_type != RedisType::kRedisNone && redis_type != RedisType::kRedisString && metadata.size > 0) {
    initSubData();
  }
}

void MigrateIterator::initSubData() {
  std::string prefix_subkey;
  InternalKey(metadata_iter_->key(), "", metadata_.version, true /* slot_id_encoded */).Encode(&prefix_subkey);
  subdata_iter_->Seek(prefix_subkey);
  subkey_prefix_ = prefix_subkey;
  if (subdata_iter_->Valid() && subdata_iter_->key().starts_with(subkey_prefix_)) {
    findSubData();
  }
}

void MigrateIterator::findSubData() {
  assert(subdata_iter_->Valid());

  items_.push_back(MigrateItem{subkey_cf_, subdata_iter_->key().ToString(), subdata_iter_->value().ToString()});
  if (metadata_.Type() == RedisType::kRedisZSet) {
    InternalKey inkey(subdata_iter_->key(), true /* slot_id_encoded */);
    std::string score_member, new_score_key;
    score_member.append(subdata_iter_->value().ToString());
    score_member.append(inkey.GetSubKey().ToString());
    std::string ns, user_key;

    ExtractNamespaceKey(metadata_iter_->key(), &ns, &user_key, true /* slot_id_encoded */);
    InternalKey(metadata_iter_->key(), score_member, metadata_.version, true /* cluster_enabled */)
        .Encode(&new_score_key);
    items_.push_back(MigrateItem{zset_score_cf_, new_score_key, std::string()});
  }
}
