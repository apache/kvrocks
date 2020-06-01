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
