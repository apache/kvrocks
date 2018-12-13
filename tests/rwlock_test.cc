#include "lock_manager.h"
#include <thread>
#include <gtest/gtest.h>

TEST(LockManager, LockKey) {
  LockManager locks(8);
  std::vector<rocksdb::Slice> keys = {"abc", "123", "456", "abc", "123"};
  for (const auto key : keys) {
    locks.Lock(key);
    locks.UnLock(key);
  }
}