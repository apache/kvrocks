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

#include "lock_manager.h"

#include <thread>
#include <string>

LockManager::LockManager(int hash_power): hash_power_(hash_power) {
  hash_mask_ = (1U << hash_power) - 1;
  for (unsigned i = 0; i < Size(); i++) {
    mutex_pool_.emplace_back(new std::mutex());
  }
}

LockManager::~LockManager() {
  for (const auto &mu : mutex_pool_) {
    delete mu;
  }
}

unsigned LockManager::hash(const rocksdb::Slice &key) {
  return static_cast<unsigned>(std::hash<std::string>{}(key.ToString()) & hash_mask_);
}

unsigned LockManager::Size() {
  return (1U << hash_power_);
}

void LockManager::Lock(const rocksdb::Slice &key) {
  mutex_pool_[hash(key)]->lock();
}

void LockManager::UnLock(const rocksdb::Slice &key) {
  mutex_pool_[hash(key)]->unlock();
}
