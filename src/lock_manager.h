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

#include <mutex>
#include <vector>

#include <rocksdb/db.h>

class LockManager {
 public:
  explicit LockManager(int hash_power);
  ~LockManager();

  unsigned Size();
  void Lock(const rocksdb::Slice &key);
  void UnLock(const rocksdb::Slice &key);

 private:
  int hash_power_;
  int hash_mask_;
  std::vector<std::mutex*> mutex_pool_;
  unsigned hash(const rocksdb::Slice &key);
};

class LockGuard {
 public:
  explicit LockGuard(LockManager *lock_mgr, rocksdb::Slice key):
      lock_mgr_(lock_mgr),
      key_(key) {
    lock_mgr->Lock(key_);
  }
  ~LockGuard() {
    lock_mgr_->UnLock(key_);
  }
 private:
  LockManager *lock_mgr_ = nullptr;
  rocksdb::Slice key_;
};
