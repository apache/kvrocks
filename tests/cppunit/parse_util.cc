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

#include <parse_util.h>

TEST(ParseUtil, TryParseInt) {
  long long v;
  const char *str = "12345hellooo", *end;
  
  std::tie(v, end) = *TryParseInt(str);
  ASSERT_EQ(v, 12345);
  ASSERT_EQ(end - str, 5);

  ASSERT_EQ(TryParseInt("hello").Msg(), "not started as an integer");
  ASSERT_FALSE(TryParseInt("9999999999999999999999999999999999"));
  ASSERT_FALSE(TryParseInt("1", 100));
}

TEST(ParseUtil, ParseInt) {
  ASSERT_EQ(*ParseInt("-2333"), -2333);
  ASSERT_EQ(*ParseInt("0x1a"), 26);
  ASSERT_EQ(*ParseInt("011"), 9);
  ASSERT_EQ(*ParseInt("111", 2), 7);
  ASSERT_EQ(*ParseInt("11", 010), 9);
  ASSERT_EQ(*ParseInt("11", 10), 11);
  ASSERT_EQ(*ParseInt("11", 0x10), 17);

  ASSERT_EQ(ParseInt("hello").Msg(), "not started as an integer");
  ASSERT_EQ(ParseInt("123hello").Msg(), "encounter non-integer characters");
  ASSERT_FALSE(ParseInt("9999999999999999999999999999999999"));
  ASSERT_EQ(ParseInt<short>("99999").Msg(), "out of range of integer type");
  ASSERT_EQ(*ParseInt<short>("30000"), 30000);
  ASSERT_EQ(*ParseInt<int>("99999"), 99999);
  ASSERT_EQ(ParseInt<int>("3000000000").Msg(), "out of range of integer type");

  ASSERT_EQ(*ParseInt("123", {0, 123}), 123);
  ASSERT_EQ(ParseInt("124", {0, 123}).Msg(), "out of numeric range");
}
