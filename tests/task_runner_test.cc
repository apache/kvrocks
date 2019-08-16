#include <gtest/gtest.h>
#include <atomic>
#include "task_runner.h"

TEST(TaskRunner, PublishOverflow) {
  TaskRunner tr(2, 3);
  Task t;
  Status s;
  for(int i = 0; i < 5; i++) {
    s = tr.Publish(t);
    if (i < 3) {
      ASSERT_TRUE(s.IsOK());
    } else {
      std::cout << "i:" << i <<std::endl;
      ASSERT_FALSE(s.IsOK());
    }
  }
}

TEST(TaskRunner, PublishToStopQueue) {
  TaskRunner tr(2, 3);
  tr.Stop();

  Task t;
  Status s;
  for(int i = 0; i < 5; i++) {
    s = tr.Publish(t);
    ASSERT_FALSE(s.IsOK());
  }
}

TEST(TaskRunner, Run) {
  std::atomic<int> counter = {0};
  TaskRunner tr(3, 1024);
  tr.Start();

  Status s;
  Task t;
  for(int i = 0; i < 100; i++) {
    t.callback = [](void *arg){auto ptr = (std::atomic<int>*)arg; ptr->fetch_add(1);};
    t.arg = (void*) &counter;
    s = tr.Publish(t);
    ASSERT_TRUE(s.IsOK());
  }
  sleep(1);
  ASSERT_EQ(100, counter);
  tr.Stop();
  tr.Join();
}