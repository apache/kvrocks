#include "t_mutex.h"
#include <thread>
#include <gtest/gtest.h>

TEST(RWLock, Lock) {
  RWLock lock;
  for (int i = 0; i < 1000; i++) {
    lock.RLock();
  }
  for (int i = 0; i < 1000; i++) {
    lock.UnLock();
  }
  for (int i = 0; i < 1000; i++) {
    lock.Lock();
    lock.UnLock();
  }
}

TEST(RWLock, MultiThreadCounter) {
  int threads = 10;
  int count = 0;
  int expected = threads * 10000;
  RWLock lock;
  std::vector<std::thread> counters;
  for (int i = 0; i < threads; i++) {
    counters.emplace_back(std::thread([&lock,&count](){
      for (int i = 0; i < 10000; i++) {
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