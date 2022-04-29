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
#include <map>
#include "util.h"

TEST(StringUtil, ToLower) {
  std::map<std::string, std::string> cases {
          {"ABC", "abc"},
          {"AbC", "abc"},
          {"abc", "abc"},
  };
  for (auto iter = cases.begin(); iter != cases.end(); iter++) {
    std::string input = iter->first;
    input = Util::ToLower(input);
    ASSERT_EQ(input, iter->second);
  }
}

TEST(StringUtil, Trim) {
  std::map<std::string, std::string> cases {
          {"abc", "abc"},
          {"   abc    ", "abc"},
          {"\t\tabc\t\t", "abc"},
          {"\t\tabc\n\n", "abc"},
          {"\n\nabc\n\n", "abc"},
  };
  for (auto iter = cases.begin(); iter != cases.end(); iter++) {
    std::string input = iter->first;
    std::string output;
    Util::Trim(input, " \t\n", &output);
    ASSERT_EQ(output, iter->second);
  }
}

TEST(StringUtil, Split) {
  std::vector<std::string> array;
  std::vector<std::string> expected = {"a", "b", "c", "d"};
  Util::Split("a,b,c,d", ",", &array);
  ASSERT_EQ(expected, array);
  Util::Split("a,b,,c,d,", ",", &array);
  ASSERT_EQ(expected, array);
  Util::Split(",a,b,c,d,", ",", &array);
  ASSERT_EQ(expected, array);
  Util::Split("a     b  c  d   ", " ", &array);
  ASSERT_EQ(expected, array);
  Util::Split("a\tb\nc\t\nd   ", " \t\n", &array);
  ASSERT_EQ(expected, array);
}

TEST(StringUtil, TokenizeRedisProtocol) {
  std::vector<std::string> array;
  std::vector<std::string> expected = {"this", "is", "a", "test"};
  Util::TokenizeRedisProtocol("*4\r\n$4\r\nthis\r\n$2\r\nis\r\n$1\r\na\r\n$4\r\ntest\r\n", &array);
  ASSERT_EQ(expected, array);
}

TEST(StringUtil, HasPrefix) {
  ASSERT_TRUE(Util::HasPrefix("has_prefix_is_true", "has_prefix"));
  ASSERT_FALSE(Util::HasPrefix("has_prefix_is_false", "_has_prefix"));
  ASSERT_TRUE(Util::HasPrefix("has_prefix", "has_prefix"));
  ASSERT_FALSE(Util::HasPrefix("has", "has_prefix"));
}
