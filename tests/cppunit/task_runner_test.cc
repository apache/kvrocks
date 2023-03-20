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

#include "task_runner.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "time_util.h"

TEST(TaskRunner, PublishOverflow) {
  TaskRunner tr(2, 3);
  Task t;
  for (int i = 0; i < 5; i++) {
    auto s = tr.TryPublish(t);
    if (i < 3) {
      ASSERT_TRUE(s.IsOK());
    } else {
      ASSERT_FALSE(s.IsOK());
    }
  }
}

using namespace std::chrono_literals;

TEST(TaskRunner, Counter) {
  std::atomic<int> counter = 0;
  TaskRunner tr(3, 1024);

  for (int i = 0; i < 100; i++) {
    Task t = [&counter] { counter.fetch_add(1); };
    auto s = tr.TryPublish(t);
    ASSERT_TRUE(s.IsOK());
  }

  auto _ = tr.Start();
  std::this_thread::sleep_for(1s);
  ASSERT_EQ(100, counter);

  tr.Cancel();
  _ = tr.Join();
}

TEST(TaskRunner, Sleep) {
  TaskRunner tr(3, 1024);

  for (int i = 0; i < 100; i++) {
    Task t = [i] { std::this_thread::sleep_for(i * 100ms); };
    tr.Publish(t);
  }

  ASSERT_EQ(tr.Size(), 100);

  auto _ = tr.Start();

  std::this_thread::sleep_for(1s);
  ASSERT_NEAR(tr.Size(), 90, 1);

  auto begin = Util::GetTimeStampMS();
  tr.Cancel();
  _ = tr.Join();
  ASSERT_LE(Util::GetTimeStampMS() - begin, 1500);
}

TEST(TaskRunner, PublishAfterStart) {
  std::atomic<int> counter = 0;
  TaskRunner tr(3, 1024);
  auto _ = tr.Start();

  std::this_thread::sleep_for(0.1s);

  for (int i = 0; i < 10; i++) {
    tr.Publish([&counter] { counter.fetch_add(1); });
  }

  std::this_thread::sleep_for(0.1s);

  ASSERT_EQ(counter, 10);

  for (int i = 0; i < 10; i++) {
    tr.Publish([&counter] { counter.fetch_add(1); });
  }

  std::this_thread::sleep_for(0.1s);

  ASSERT_EQ(counter, 20);
}
