#include "lock_manager.h"
#include "rw_lock.h"
#include <thread>
#include <memory>
#include <gtest/gtest.h>

TEST(LockManager, LockKey) {
  LockManager locks(8);
  std::vector<rocksdb::Slice> keys = {"abc", "123", "456", "abc", "123"};
  for (const auto key : keys) {
    locks.Lock(key);
    locks.UnLock(key);
  }
}

TEST(ReadWriteLock, ReadLockGurad) {
  RWLock::ReadWriteLock rwlock;
  int val = 1;

  std::thread ths[10];
  for(int i = 0; i < 10; i++) {
    ths[i] = std::thread([&rwlock, &val]() {
      RWLock::ReadLock rlock(rwlock);
      ASSERT_EQ(1, val);
    });
  }

  for (int i = 0; i < 10; i++) {
    ths[i].join();
  }
}

TEST(ReadWriteLock, WriteLockGurad) {
  RWLock::ReadWriteLock rwlock;
  int val = 0;

  std::thread ths[10];
  for(int i = 0; i < 10; i++) {
    ths[i] = std::thread([&rwlock, &val]() {
        RWLock::WriteLock wlock(rwlock);
        for (int i = 0; i < 100000000; i++) {
          val++;
      }
    });
  }

  for (int i = 0; i < 10; i++) {
    ths[i].join();
  }
  ASSERT_EQ(1000000000, val);
}

TEST(ReadWriteLock, unique_ptr_for_WriteLockGuard) {
  RWLock::ReadWriteLock rwlock;
  int val = 0;

  std::thread ths[10];
  for(int i = 0; i < 10; i++) {
    ths[i] = std::thread([&rwlock, &val]() {
      auto ptr = std::unique_ptr<RWLock::WriteLock>(new RWLock::WriteLock(rwlock));
      for (int i = 0; i < 10000000; i++) {
        val++;
      }
    });
  }

  for (int i = 0; i < 10; i++) {
    ths[i].join();
  }
  ASSERT_EQ(100000000, val);
}
