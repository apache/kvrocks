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