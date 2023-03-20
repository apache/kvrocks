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

#include "observer_or_unique.h"

#include <gtest/gtest.h>
struct Counter {  // NOLINT
  explicit Counter(int* i) : i_(i) { ++*i_; }
  ~Counter() { --*i_; }

  int* i_;
};

TEST(ObserverOrUniquePtr, Unique) {
  int v = 0;
  {
    ObserverOrUniquePtr<Counter> unique(new Counter{&v}, ObserverOrUnique::Unique);
    ASSERT_EQ(v, 1);
  }
  ASSERT_EQ(v, 0);
}

TEST(ObserverOrUniquePtr, Observer) {
  int v = 0;
  std::unique_ptr<Counter> c = nullptr;
  {
    ObserverOrUniquePtr<Counter> observer(new Counter{&v}, ObserverOrUnique::Observer);
    ASSERT_EQ(v, 1);

    c.reset(observer.Get());
  }
  ASSERT_EQ(v, 1);
  c.reset();
  ASSERT_EQ(v, 0);
}
