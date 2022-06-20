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

#include <string>
#include <map>
#include <vector>

#include "../../src/redis_db.h"
#include "../../src/status.h"
#include "../../src/storage.h"
#include "../../src/redis_metadata.h"
#include "../../src/batch_extractor.h"

#include "config.h"
#include "writer.h"

class LatestSnapShot {
 public:
  explicit LatestSnapShot(rocksdb::DB *db) : db_(db) {
    snapshot_ = db_->GetSnapshot();
  }
  ~LatestSnapShot() {
    db_->ReleaseSnapshot(snapshot_);
  }
  const rocksdb::Snapshot *GetSnapShot() { return snapshot_; }
 private:
  rocksdb::DB *db_ = nullptr;
  const rocksdb::Snapshot *snapshot_ = nullptr;
};

class Parser {
 public:
  explicit Parser(Engine::Storage *storage, Writer *writer)
      : storage_(storage), writer_(writer) {
    lastest_snapshot_ = new LatestSnapShot(storage->GetDB());
    is_slotid_encoded_ = storage_->IsSlotIdEncoded();
  }
  ~Parser() {
    delete lastest_snapshot_;
    lastest_snapshot_ = nullptr;
  }

  Status ParseFullDB();
  rocksdb::Status ParseWriteBatch(const std::string &batch_string);

 protected:
  Engine::Storage *storage_ = nullptr;
  Writer *writer_ = nullptr;
  LatestSnapShot *lastest_snapshot_ = nullptr;
  bool is_slotid_encoded_ = false;

  Status parseSimpleKV(const Slice &ns_key, const Slice &value, int expire);
  Status parseComplexKV(const Slice &ns_key, const Metadata &metadata);
  Status parseBitmapSegment(const Slice &ns, const Slice &user_key, int index, const Slice &bitmap);
};
