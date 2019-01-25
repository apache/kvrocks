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
