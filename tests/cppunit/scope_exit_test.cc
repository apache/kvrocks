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
 */

#include "common/scope_exit.h"

#include <gtest/gtest.h>

#include <utility>

TEST(ScopeExit, MoveConstructionRunsCleanupOnce) {
  int cleanup_count = 0;
  {
    auto source = MakeScopeExit([&cleanup_count] { ++cleanup_count; });
    {
      auto target = std::move(source);
      EXPECT_TRUE(target.enabled);
    }
    EXPECT_EQ(cleanup_count, 1);
  }
  EXPECT_EQ(cleanup_count, 1);
}
