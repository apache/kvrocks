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

#include <rocksdb/iterator.h>
#include <rocksdb/options.h>

#include "storage.h"

namespace engine {

class SubKeyIterator {
 public:
  explicit SubKeyIterator(Storage *storage, rocksdb::ReadOptions read_options, RedisType type, std::string prefix);
  ~SubKeyIterator() = default;
  bool Valid() const;
  void Seek();
  void Next();
  // return the raw key in rocksdb
  Slice Key() const;
  // return the user key without prefix
  Slice UserKey() const;
  Slice Value() const;
  void Reset();

 private:
  Storage *storage_;
  rocksdb::ReadOptions read_options_;
  RedisType type_;
  std::string prefix_;
  std::unique_ptr<rocksdb::Iterator> iter_;
  rocksdb::ColumnFamilyHandle *cf_handle_ = nullptr;
};

class DBIterator {
 public:
  explicit DBIterator(Storage *storage, rocksdb::ReadOptions read_options, int slot = -1);
  ~DBIterator() = default;

  bool Valid() const;
  void Seek(const std::string &target = "");
  void Next();
  // return the raw key in rocksdb
  Slice Key() const;
  // return the namespace and user key without prefix
  std::tuple<Slice, Slice> UserKey() const;
  Slice Value() const;
  RedisType Type() const;
  void Reset();
  std::unique_ptr<SubKeyIterator> GetSubKeyIterator() const;

 private:
  void nextUntilValid();

  Storage *storage_;
  rocksdb::ReadOptions read_options_;
  int slot_ = -1;
  Metadata metadata_ = Metadata(kRedisNone, false);

  rocksdb::ColumnFamilyHandle *metadata_cf_handle_ = nullptr;
  std::unique_ptr<rocksdb::Iterator> metadata_iter_;
  std::unique_ptr<SubKeyIterator> subkey_iter_;
};

struct WALItem {
  enum class Type : uint8_t {
    kTypeInvalid = 0,
    kTypeLogData = 1,
    kTypePut = 2,
    kTypeDelete = 3,
    kTypeDeleteRange = 4,
  };

  WALItem() = default;
  WALItem(WALItem::Type t, uint32_t cf_id, std::string k, std::string v)
      : type(t), column_family_id(cf_id), key(std::move(k)), value(std::move(v)) {}

  WALItem::Type type = WALItem::Type::kTypeInvalid;
  uint32_t column_family_id = 0;
  std::string key;
  std::string value;
};

class WALBatchExtractor : public rocksdb::WriteBatch::Handler {
 public:
  // If set slot, storage must enable slot id encoding
  explicit WALBatchExtractor(int slot = -1) : slot_(slot) {}

  rocksdb::Status PutCF(uint32_t column_family_id, const Slice &key, const Slice &value) override;

  rocksdb::Status DeleteCF(uint32_t column_family_id, const rocksdb::Slice &key) override;

  rocksdb::Status DeleteRangeCF(uint32_t column_family_id, const rocksdb::Slice &begin_key,
                                const rocksdb::Slice &end_key) override;

  void LogData(const rocksdb::Slice &blob) override;

  void Clear();

  class Iter {
    friend class WALBatchExtractor;

   public:
    bool Valid();
    void Next();
    WALItem Value();

   private:
    explicit Iter(std::vector<WALItem> *items) : items_(items), cur_(0) {}
    std::vector<WALItem> *items_;
    size_t cur_;
  };

  WALBatchExtractor::Iter GetIter();

 private:
  std::vector<WALItem> items_;
  int slot_;
};

class WALIterator {
 public:
  explicit WALIterator(engine::Storage *storage, int slot = -1) : storage_(storage), slot_(slot){};
  ~WALIterator() = default;

  bool Valid() const;
  void Seek(rocksdb::SequenceNumber seq);
  void Next();
  WALItem Item();

  rocksdb::SequenceNumber NextSequenceNumber();
  void Reset();

 private:
  void nextBatch();

  engine::Storage *storage_;
  int slot_;

  std::unique_ptr<rocksdb::TransactionLogIterator> iter_;
  WALBatchExtractor extractor_;
  std::unique_ptr<WALBatchExtractor::Iter> batch_iter_;
  rocksdb::SequenceNumber next_batch_seq_;
};

}  // namespace engine
