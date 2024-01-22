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
DBIterator::DBIterator(Storage *storage, rocksdb::ReadOptions read_options, int slot)
    : storage_(storage), read_options_(std::move(read_options)), slot_(slot) {
  metadata_cf_handle_ = storage_->GetCFHandle(kMetadataColumnFamilyName);
  metadata_iter_ = util::UniqueIterator(storage_->NewIterator(read_options_, metadata_cf_handle_));
}

void DBIterator::Next() {
  if (!Valid()) return;

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

void DBIterator::Seek(const std::string &target) {
  if (!metadata_iter_) return;

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

  RedisType type = metadata_.Type();
  if (type == kRedisNone || metadata_.IsSingleKVType()) {
    return nullptr;
  }

  auto prefix = InternalKey(Key(), "", metadata_.version, storage_->IsSlotIdEncoded()).Encode();
  return std::make_unique<SubKeyIterator>(storage_, read_options_, type, std::move(prefix));
}

SubKeyIterator::SubKeyIterator(Storage *storage, rocksdb::ReadOptions read_options, RedisType type, std::string prefix)
    : storage_(storage), read_options_(std::move(read_options)), type_(type), prefix_(std::move(prefix)) {
  if (type_ == kRedisStream) {
    cf_handle_ = storage_->GetCFHandle(kStreamColumnFamilyName);
  } else {
    cf_handle_ = storage_->GetCFHandle(kSubkeyColumnFamilyName);
  }
  iter_ = util::UniqueIterator(storage_->NewIterator(read_options_, cf_handle_));
}

void SubKeyIterator::Next() {
  if (!Valid()) return;

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
  if (!iter_) return;

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

rocksdb::Status WALBatchExtractor::PutCF(uint32_t column_family_id, const Slice &key, const Slice &value) {
  if (slot_ != -1) {
    if (slot_ != ExtractSlotId(key)) {
      return rocksdb::Status::OK();
    }
  }
  items_.emplace_back(WALItem::Type::kTypePut, column_family_id, key.ToString(), value.ToString());
  return rocksdb::Status::OK();
}

rocksdb::Status WALBatchExtractor::DeleteCF(uint32_t column_family_id, const rocksdb::Slice &key) {
  if (slot_ != -1) {
    if (slot_ != ExtractSlotId(key)) {
      return rocksdb::Status::OK();
    }
  }
  items_.emplace_back(WALItem::Type::kTypeDelete, column_family_id, key.ToString(), std::string{});
  return rocksdb::Status::OK();
}

rocksdb::Status WALBatchExtractor::DeleteRangeCF(uint32_t column_family_id, const rocksdb::Slice &begin_key,
                                                 const rocksdb::Slice &end_key) {
  items_.emplace_back(WALItem::Type::kTypeDeleteRange, column_family_id, begin_key.ToString(), end_key.ToString());
  return rocksdb::Status::OK();
}

void WALBatchExtractor::LogData(const rocksdb::Slice &blob) {
  items_.emplace_back(WALItem::Type::kTypeLogData, 0, blob.ToString(), std::string{});
};

void WALBatchExtractor::Clear() { items_.clear(); }

WALBatchExtractor::Iter WALBatchExtractor::GetIter() { return Iter(&items_); }

bool WALBatchExtractor::Iter::Valid() { return items_ && cur_ < items_->size(); }

void WALBatchExtractor::Iter::Next() { cur_++; }

WALItem WALBatchExtractor::Iter::Value() { return (*items_)[cur_]; }

void WALIterator::Reset() {
  if (iter_) {
    iter_.reset();
  }
  if (batch_iter_) {
    batch_iter_.reset();
  }
  extractor_.Clear();
  next_batch_seq_ = 0;
}

bool WALIterator::Valid() const { return (batch_iter_ && batch_iter_->Valid()) || (iter_ && iter_->Valid()); }

void WALIterator::nextBatch() {
  if (!iter_ || !iter_->Valid()) {
    Reset();
    return;
  }

  auto batch = iter_->GetBatch();
  if (batch.sequence != next_batch_seq_ || !batch.writeBatchPtr) {
    Reset();
    return;
  }

  extractor_.Clear();

  auto s = batch.writeBatchPtr->Iterate(&extractor_);
  if (!s.ok()) {
    Reset();
    return;
  }

  next_batch_seq_ += batch.writeBatchPtr->Count();
  batch_iter_ = std::make_unique<WALBatchExtractor::Iter>(extractor_.GetIter());
}

void WALIterator::Seek(rocksdb::SequenceNumber seq) {
  if (slot_ != -1 && !storage_->IsSlotIdEncoded()) {
    Reset();
    return;
  }

  auto s = storage_->GetWALIter(seq, &iter_);
  if (!s.IsOK()) {
    Reset();
    return;
  }

  next_batch_seq_ = seq;

  nextBatch();
}

WALItem WALIterator::Item() {
  if (batch_iter_ && batch_iter_->Valid()) {
    return batch_iter_->Value();
  }
  return {};
}

rocksdb::SequenceNumber WALIterator::NextSequenceNumber() { return next_batch_seq_; }

void WALIterator::Next() {
  if (!Valid()) {
    Reset();
    return;
  }

  if (batch_iter_ && batch_iter_->Valid()) {
    batch_iter_->Next();
    if (batch_iter_->Valid()) {
      return;
    }
  }

  iter_->Next();
  nextBatch();
}

}  // namespace engine
