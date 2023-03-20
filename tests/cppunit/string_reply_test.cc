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

#include "server/redis_reply.h"

class StringReplyTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    for (int i = 0; i < 100000; i++) {
      values.emplace_back("values" + std::to_string(i));
    }
  }
  static void TearDownTestCase() { values.clear(); }
  static std::vector<std::string> values;

  void SetUp() override {}
  void TearDown() override {}
};

std::vector<std::string> StringReplyTest::values;

TEST_F(StringReplyTest, MultiBulkString) {
  std::string result = Redis::MultiBulkString(values);
  ASSERT_EQ(result.length(), 13 * 10 + 14 * 90 + 15 * 900 + 17 * 9000 + 18 * 90000 + 9);
}

TEST_F(StringReplyTest, BulkString) {
  std::string result = "*" + std::to_string(values.size()) + CRLF;
  for (const auto &v : values) {
    result += Redis::BulkString(v);
  }

  ASSERT_EQ(result.length(), 13 * 10 + 14 * 90 + 15 * 900 + 17 * 9000 + 18 * 90000 + 9);
}
