//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// The file is copy and modified from rocksdb:
// https://github.com/facebook/rocksdb/blob/5cf6ab6f315e2506171aad2504638a7da9af7d1e/db/write_batch_test.cc#L251

#pragma once

#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include <string>
#include <vector>

#include "storage.h"

// WriteBatchIndexer traverses the operations in WriteBatch and append to the specified WriteBatchWithIndex
// TODO: unit test
class WriteBatchIndexer : public rocksdb::WriteBatch::Handler {
 public:
  explicit WriteBatchIndexer(engine::Storage* storage, rocksdb::WriteBatchWithIndex* dest_batch)
      : storage_(storage), dest_batch_(dest_batch) {}

  rocksdb::Status PutCF(uint32_t column_family_id, const rocksdb::Slice& key, const rocksdb::Slice& value) override {
    return dest_batch_->Put(storage_->GetCFHandle(static_cast<ColumnFamilyID>(column_family_id)), key, value);
  }

  void Put(const rocksdb::Slice& key, const rocksdb::Slice& value) override { dest_batch_->Put(key, value); }

  rocksdb::Status DeleteCF(uint32_t column_family_id, const rocksdb::Slice& key) override {
    return dest_batch_->Delete(storage_->GetCFHandle(static_cast<ColumnFamilyID>(column_family_id)), key);
  }

  void Delete(const rocksdb::Slice& key) override { dest_batch_->Delete(key); }

  rocksdb::Status SingleDeleteCF(uint32_t column_family_id, const rocksdb::Slice& key) override {
    return dest_batch_->SingleDelete(storage_->GetCFHandle(static_cast<ColumnFamilyID>(column_family_id)), key);
  }

  void SingleDelete(const rocksdb::Slice& key) override { dest_batch_->SingleDelete(key); }

  rocksdb::Status DeleteRangeCF(uint32_t column_family_id, const rocksdb::Slice& begin_key,
                                const rocksdb::Slice& end_key) override {
    rocksdb::ReadOptions read_options;  // TODO: snapshot?
    auto cf_handle = storage_->GetCFHandle(static_cast<ColumnFamilyID>(column_family_id));
    std::unique_ptr<rocksdb::Iterator> it(storage_->NewIterator(read_options, cf_handle));
    for (it->Seek(begin_key); it->Valid() && it->key().compare(end_key) < 0; it->Next()) {
      auto s = dest_batch_->Delete(cf_handle, it->key());
      if (!s.ok()) {
        return s;
      }
    }
    return rocksdb::Status::OK();
  }

  rocksdb::Status MergeCF(uint32_t column_family_id, const rocksdb::Slice& key, const rocksdb::Slice& value) override {
    return dest_batch_->Merge(storage_->GetCFHandle(static_cast<ColumnFamilyID>(column_family_id)), key, value);
  }

  void Merge(const rocksdb::Slice& key, const rocksdb::Slice& value) override { dest_batch_->Merge(key, value); }

  void LogData(const rocksdb::Slice& blob) override { dest_batch_->PutLogData(blob); }

 private:
  engine::Storage* storage_;
  rocksdb::WriteBatchWithIndex* dest_batch_;
};