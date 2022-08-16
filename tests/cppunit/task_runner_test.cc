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
    t = [&counter]{counter.fetch_add(1);};
    s = tr.Publish(t);
    ASSERT_TRUE(s.IsOK());
  }
  sleep(1);
  ASSERT_EQ(100, counter);
  tr.Stop();
  tr.Join();
}
