#include "rwlock.h"
#include <thread>
#include <gtest/gtest.h>

TEST(RWLock, Lock) {
  RWLock lock;
  for (int i = 0; i < 1000; i++) {
    lock.RLock();
  }
  for (int i = 0; i < 1000; i++) {
    lock.RUnLock();
  }
  for (int i = 0; i < 1000; i++) {
    lock.Lock();
    lock.UnLock();
  }
}

TEST(RWLock, MultiThreadCounter) {
  int threads = 10;
  int count = 0;
  int expected = threads * 100000;
  RWLock lock;
  std::vector<std::thread> counters;
  for (int i = 0; i < threads; i++) {
    counters.emplace_back(std::thread([&lock,&count](){
      for (int i = 0; i < 100000; i++) {
        lock.Lock();
        count++;
        lock.UnLock();
      }
    }));
  }
  for (int i = 0; i < threads; i++) {
    counters[i].join();
  }
  ASSERT_EQ(expected, count);
}

TEST(RWLocks, LockKey) {
  RWLocks locks(8);
  std::vector<std::string> keys = {"abc", "123", "456", "abc", "123"};
  for (const auto key : keys) {
    locks.Lock(key);
    locks.UnLock(key);
    locks.RLock(key);
    locks.RUnLock(key);
  }
}