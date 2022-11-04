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

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <rocksdb/db.h>
#include <rocksdb/slice.h>

#include <string>
#include <vector>

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
