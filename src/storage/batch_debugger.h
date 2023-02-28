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

#include <string>
#include <vector>

/// WriteBatchInspector can be used to Iterate and print rocksdb's WriteBatch,
///  kvrocks' replication serialize WriteBatch in WAL, but WriteBatch is not
///  human-readable. WriteBatchInspector can be used to trace and print the
///  content of the WriteBatch for debugging.
///
/// Usage:
///
/// ```
/// WriteBatchInspector inspector;
/// status = write_batch.Iterate(&inspector);
/// LOG(INFO) << inspector.seen << ", cnt: " << inspector.cnt;
/// ```
struct WriteBatchInspector : public rocksdb::WriteBatch::Handler {
  std::string seen;
  int cnt = 0;
  rocksdb::Status PutCF(uint32_t column_family_id, const rocksdb::Slice& key, const rocksdb::Slice& value) override {
    ++cnt;
    if (column_family_id == 0) {
      seen += "Put(" + key.ToString() + ", " + value.ToString() + ")";
    } else {
      seen += "PutCF(" + std::to_string(column_family_id) + ", " + key.ToString() + ", " + value.ToString() + ")";
    }
    return rocksdb::Status::OK();
  }
  rocksdb::Status DeleteCF(uint32_t column_family_id, const rocksdb::Slice& key) override {
    ++cnt;
    if (column_family_id == 0) {
      seen += "Delete(" + key.ToString() + ")";
    } else {
      seen += "DeleteCF(" + std::to_string(column_family_id) + ", " + key.ToString() + ")";
    }
    return rocksdb::Status::OK();
  }
  rocksdb::Status SingleDeleteCF(uint32_t column_family_id, const rocksdb::Slice& key) override {
    ++cnt;
    if (column_family_id == 0) {
      seen += "SingleDelete(" + key.ToString() + ")";
    } else {
      seen += "SingleDeleteCF(" + std::to_string(column_family_id) + ", " + key.ToString() + ")";
    }
    return rocksdb::Status::OK();
  }
  rocksdb::Status DeleteRangeCF(uint32_t column_family_id, const rocksdb::Slice& begin_key,
                                const rocksdb::Slice& end_key) override {
    ++cnt;
    if (column_family_id == 0) {
      seen += "DeleteRange(" + begin_key.ToString() + ", " + end_key.ToString() + ")";
    } else {
      seen += "DeleteRangeCF(" + std::to_string(column_family_id) + ", " + begin_key.ToString() + ", " +
              end_key.ToString() + ")";
    }
    return rocksdb::Status::OK();
  }
  rocksdb::Status MergeCF(uint32_t column_family_id, const rocksdb::Slice& key, const rocksdb::Slice& value) override {
    ++cnt;
    if (column_family_id == 0) {
      seen += "Merge(" + key.ToString() + ", " + value.ToString() + ")";
    } else {
      seen += "MergeCF(" + std::to_string(column_family_id) + ", " + key.ToString() + ", " + value.ToString() + ")";
    }
    return rocksdb::Status::OK();
  }
  void LogData(const rocksdb::Slice& blob) override {
    ++cnt;
    seen += "LogData(" + blob.ToString() + ")";
  }
  rocksdb::Status MarkBeginPrepare(bool unprepare) override {
    ++cnt;
    seen += "MarkBeginPrepare(" + std::string(unprepare ? "true" : "false") + ")";
    return rocksdb::Status::OK();
  }
  rocksdb::Status MarkEndPrepare(const rocksdb::Slice& xid) override {
    ++cnt;
    seen += "MarkEndPrepare(" + xid.ToString() + ")";
    return rocksdb::Status::OK();
  }
  rocksdb::Status MarkNoop(bool empty_batch) override {
    ++cnt;
    seen += "MarkNoop(" + std::string(empty_batch ? "true" : "false") + ")";
    return rocksdb::Status::OK();
  }
  rocksdb::Status MarkCommit(const rocksdb::Slice& xid) override {
    ++cnt;
    seen += "MarkCommit(" + xid.ToString() + ")";
    return rocksdb::Status::OK();
  }
  rocksdb::Status MarkCommitWithTimestamp(const rocksdb::Slice& xid, const rocksdb::Slice& ts) override {
    ++cnt;
    seen += "MarkCommitWithTimestamp(" + xid.ToString() + ", " + ts.ToString(true) + ")";
    return rocksdb::Status::OK();
  }
  rocksdb::Status MarkRollback(const rocksdb::Slice& xid) override {
    ++cnt;
    seen += "MarkRollback(" + xid.ToString() + ")";
    return rocksdb::Status::OK();
  }
};
