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

#include "iterator.h"

#include <cluster/redis_slot.h>

#include "db_util.h"

namespace engine {
DBIterator::DBIterator(Storage* storage, rocksdb::ReadOptions read_options, int slot)
    : storage_(storage), read_options_(std::move(read_options)), slot_(slot) {
  metadata_cf_handle_ = storage_->GetCFHandle(kMetadataColumnFamilyName);
  metadata_iter_ = std::move(util::UniqueIterator(storage_->NewIterator(read_options_, metadata_cf_handle_)));
}

void DBIterator::Next() {
  metadata_iter_->Next();
  nextUntilValid();
}

void DBIterator::nextUntilValid() {
  // slot_ != -1 means we would like to iterate all keys in the slot
  // so we can skip the afterwards keys if the slot id doesn't match
  if (slot_ != -1 && metadata_iter_->Valid()) {
    auto [_, user_key] = ExtractNamespaceKey(metadata_iter_->key(), storage_->IsSlotIdEncoded());
    // Release the iterator if the slot id doesn't match
    if (GetSlotIdFromKey(user_key.ToString()) != slot_) {
      Reset();
      return;
    }
  }

  while (metadata_iter_->Valid()) {
    Metadata metadata(kRedisNone, false);
    // Skip the metadata if it's expired
    if (metadata.Decode(metadata_iter_->value()).ok() && !metadata.Expired()) {
      metadata_ = metadata;
      break;
    }
    metadata_iter_->Next();
  }
}

bool DBIterator::Valid() const { return metadata_iter_ && metadata_iter_->Valid(); }

Slice DBIterator::Key() const { return Valid() ? metadata_iter_->key() : Slice(); }

std::tuple<Slice, Slice> DBIterator::UserKey() const {
  if (!Valid()) {
    return {};
  }
  return ExtractNamespaceKey(metadata_iter_->key(), slot_ != -1);
}

Slice DBIterator::Value() const { return Valid() ? metadata_iter_->value() : Slice(); }

RedisType DBIterator::Type() const { return Valid() ? metadata_.Type() : kRedisNone; }

void DBIterator::Reset() {
  if (metadata_iter_) metadata_iter_.reset();
}

void DBIterator::Seek(const std::string& target) {
  metadata_iter_->Reset();

  // Iterate with the slot id but storage didn't enable slot id encoding
  if (slot_ != -1 && !storage_->IsSlotIdEncoded()) {
    Reset();
    return;
  }
  std::string prefix = target;
  if (slot_ != -1) {
    // Use the slot id as the prefix if it's specified
    prefix = ComposeSlotKeyPrefix(kDefaultNamespace, slot_) + target;
  }

  metadata_iter_->Seek(prefix);
  nextUntilValid();
}

std::unique_ptr<SubKeyIterator> DBIterator::GetSubKeyIterator() const {
  if (!Valid()) {
    return nullptr;
  }

  // The string/json type doesn't have sub keys
  RedisType type = metadata_.Type();
  if (type == kRedisNone || type == kRedisString || type == kRedisJson) {
    return nullptr;
  }

  auto prefix = InternalKey(Key(), "", metadata_.version, storage_->IsSlotIdEncoded()).Encode();
  return std::make_unique<SubKeyIterator>(storage_, read_options_, type, prefix);
}

SubKeyIterator::SubKeyIterator(Storage* storage, rocksdb::ReadOptions read_options, RedisType type, std::string prefix)
    : storage_(storage), read_options_(std::move(read_options)), type_(type), prefix_(std::move(prefix)) {
  if (type_ == kRedisStream) {
    cf_handle_ = storage_->GetCFHandle(kStreamColumnFamilyName);
  } else {
    cf_handle_ = storage_->GetCFHandle(kSubkeyColumnFamilyName);
  }
  iter_ = std::move(util::UniqueIterator(storage_->NewIterator(read_options_, cf_handle_)));
}

void SubKeyIterator::Next() {
  iter_->Next();

  if (!Valid()) return;

  if (!iter_->key().starts_with(prefix_)) {
    Reset();
  }
}

bool SubKeyIterator::Valid() const { return iter_ && iter_->Valid(); }

Slice SubKeyIterator::Key() const { return Valid() ? iter_->key() : Slice(); }

Slice SubKeyIterator::UserKey() const {
  if (!Valid()) return {};

  const InternalKey internal_key(iter_->key(), storage_->IsSlotIdEncoded());
  return internal_key.GetSubKey();
}

Slice SubKeyIterator::Value() const { return Valid() ? iter_->value() : Slice(); }

void SubKeyIterator::Seek() {
  iter_->Reset();
  iter_->Seek(prefix_);

  if (!iter_->Valid()) return;
  // For the subkey iterator, it MUST contain the prefix key itself
  if (!iter_->key().starts_with(prefix_)) {
    Reset();
  }
}

void SubKeyIterator::Reset() {
  if (iter_) iter_.reset();
}

}  // namespace engine
