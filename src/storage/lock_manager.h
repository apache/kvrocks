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

#include <rocksdb/db.h>

#include <functional>
#include <mutex>
#include <string>
#include <vector>

class LockManager {
 public:
  explicit LockManager(int hash_power);
  ~LockManager() = default;

  LockManager(const LockManager &) = delete;
  LockManager &operator=(const LockManager &) = delete;

  unsigned Size() const;
  void Lock(const rocksdb::Slice &key);
  void UnLock(const rocksdb::Slice &key);
  std::vector<std::mutex *> MultiGet(const std::vector<std::string> &keys);

 private:
  int hash_power_;
  unsigned hash_mask_;
  std::vector<std::unique_ptr<std::mutex>> mutex_pool_;

  unsigned hash(const rocksdb::Slice &key) const;
};

class LockGuard {
 public:
  explicit LockGuard(LockManager *lock_mgr, rocksdb::Slice key) : lock_mgr_(lock_mgr), key_(key) {
    lock_mgr->Lock(key_);
  }
  ~LockGuard() { lock_mgr_->UnLock(key_); }

  LockGuard(const LockGuard &) = delete;
  LockGuard &operator=(const LockGuard &) = delete;

 private:
  LockManager *lock_mgr_ = nullptr;
  rocksdb::Slice key_;
};

class MultiLockGuard {
 public:
  explicit MultiLockGuard(LockManager *lock_mgr, const std::vector<std::string> &keys) : lock_mgr_(lock_mgr) {
    locks_ = lock_mgr_->MultiGet(keys);
    for (const auto &iter : locks_) {
      iter->lock();
    }
  }

  ~MultiLockGuard() {
    // Lock with order `A B C` and unlock should be `C B A`
    for (auto iter = locks_.rbegin(); iter != locks_.rend(); ++iter) {
      (*iter)->unlock();
    }
  }

  MultiLockGuard(const MultiLockGuard &) = delete;
  MultiLockGuard &operator=(const MultiLockGuard &) = delete;

 private:
  LockManager *lock_mgr_ = nullptr;
  std::vector<std::mutex *> locks_;
};
