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

// This file is modified from Bing-a-ling/ReadWriteLock.
// See the original code at https://github.com/Bing-a-ling/ReadWriteLock.

// A implementation of RAII write-first read-write lock using C++11.

#pragma once
#include <mutex>
#include <condition_variable>

namespace RWLock {

class ReadWriteLock {
 public:
  ReadWriteLock() : read_count_(0), write_count_(0), is_writing_(false) {}
  virtual ~ReadWriteLock() = default;

  void LockWrite() {
    std::unique_lock<std::mutex> guard(lock_);
    ++write_count_;
    write_condition_.wait(guard, [=] {
        return read_count_ == 0 && !is_writing_;
    });
    is_writing_ = true;
  }

  void UnLockWrite() {
    std::lock_guard<std::mutex> guard(lock_);
    is_writing_ = false;
    if (--write_count_ == 0) {
        read_condition_.notify_all();
    } else {
        write_condition_.notify_one();
    }
  }

  void LockRead() {
    std::unique_lock<std::mutex> guard(lock_);
    read_condition_.wait(guard, [=] {
        return write_count_ == 0;
    });
    ++read_count_;
  }

  void UnLockRead() {
    std::lock_guard<std::mutex> guard(lock_);
    if (--read_count_ == 0 && write_count_ > 0) {
        write_condition_.notify_one();
    }
  }

 private:
  int read_count_;
  int write_count_;
  bool is_writing_;
  std::mutex lock_;
  std::condition_variable read_condition_;
  std::condition_variable write_condition_;
};

class WriteLock {
 public:
  explicit WriteLock(ReadWriteLock& wlock) : wlock_(wlock) {
    wlock_.LockWrite();
  }

  virtual ~WriteLock() {
    wlock_.UnLockWrite();
  }

 private:
  ReadWriteLock& wlock_;
};

class ReadLock {
 public:
  explicit ReadLock(ReadWriteLock& rlock) : rlock_(rlock) {
    rlock_.LockRead();
  }

  virtual ~ReadLock() {
    rlock_.UnLockRead();
  }

 private:
  ReadWriteLock& rlock_;
};

}  // namespace RWLock
