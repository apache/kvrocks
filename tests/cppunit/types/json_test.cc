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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <types/redis_json.h>

#include "test_base.h"

class RedisJsonTest : public TestBase {
 protected:
  explicit RedisJsonTest() : json_(std::make_unique<redis::Json>(storage_, "json_ns")) {}
  ~RedisJsonTest() override = default;

  void SetUp() override { key_ = "test_json_key"; }
  void TearDown() override {}

  std::unique_ptr<redis::Json> json_;
  JsonValue json_val_;
};

using ::testing::MatchesRegex;

TEST_F(RedisJsonTest, Set) {
  ASSERT_THAT(json_->Set(key_, "$[0]", "1").ToString(), MatchesRegex(".*created at the root"));
  ASSERT_THAT(json_->Set(key_, "$.a", "1").ToString(), MatchesRegex(".*created at the root"));

  ASSERT_THAT(json_->Set(key_, "$", "invalid").ToString(), MatchesRegex(".*syntax_error.*"));
  ASSERT_THAT(json_->Set(key_, "$", "{").ToString(), MatchesRegex(".*Unexpected end of file.*"));

  ASSERT_TRUE(json_->Set(key_, "$", "  \t{\n  }  ").ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), "{}");

  ASSERT_TRUE(json_->Set(key_, "$", "1").ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), "1");

  ASSERT_TRUE(json_->Set(key_, "$", "[1, 2, 3]").ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), "[1,2,3]");

  ASSERT_TRUE(json_->Set(key_, "$[1]", "233").ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), "[1,233,3]");

  ASSERT_TRUE(json_->Set(key_, "$", "[[1,2],[3,4],[5,6]]").ok());
  ASSERT_TRUE(json_->Set(key_, "$[*][1]", R"("x")").ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"([[1,"x"],[3,"x"],[5,"x"]])");

  ASSERT_TRUE(json_->Set(key_, "$", R"({"x":1,"y":2, "z":3})").ok());
  ASSERT_TRUE(json_->Set(key_, "$.x", "[1,2,3]").ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"x":[1,2,3],"y":2,"z":3})");

  ASSERT_TRUE(json_->Set(key_, "$.y", R"({"a":"xxx","x":2})").ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"x":[1,2,3],"y":{"a":"xxx","x":2},"z":3})");

  ASSERT_TRUE(json_->Set(key_, "$..x", "true").ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"x":true,"y":{"a":"xxx","x":true},"z":3})");

  ASSERT_THAT(json_->Set(key_, "...", "1").ToString(), MatchesRegex("Invalid.*"));
  ASSERT_THAT(json_->Set(key_, "[", "1").ToString(), MatchesRegex("Invalid.*"));

  ASSERT_TRUE(json_->Set(key_, "$", "[[1,2],[[5,6],4]] ").ok());
  ASSERT_TRUE(json_->Set(key_, "$..[0]", "{}").ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"([{},[{},4]])");

  ASSERT_TRUE(json_->Del(key_).ok());
  ASSERT_TRUE(json_->Set(key_, "$", "[{ }, [ ]]").ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), "[{},[]]");
  ASSERT_THAT(json_->Set(key_, "$[1]", "invalid").ToString(), MatchesRegex(".*syntax_error.*"));
  ASSERT_TRUE(json_->Del(key_).ok());
}

TEST_F(RedisJsonTest, Get) {
  ASSERT_TRUE(json_->Set(key_, "$", R"({"x":[1,2,{"z":3}],"y":[]})").ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"x":[1,2,{"z":3}],"y":[]})");
  ASSERT_TRUE(json_->Get(key_, {"$"}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"([{"x":[1,2,{"z":3}],"y":[]}])");
  ASSERT_TRUE(json_->Get(key_, {"$.y"}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"([[]])");
  ASSERT_TRUE(json_->Get(key_, {"$.y[0]"}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"([])");
  ASSERT_TRUE(json_->Get(key_, {"$.z"}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"([])");
  ASSERT_THAT(json_->Get(key_, {"[[["}, &json_val_).ToString(), MatchesRegex("Invalid.*"));

  ASSERT_TRUE(json_->Set(key_, "$", R"([[[1,2],[3]],[4,5]])").ok());
  ASSERT_TRUE(json_->Get(key_, {"$..[0]"}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"([[[1,2],[3]],[1,2],1,3,4])");
  ASSERT_TRUE(json_->Get(key_, {"$[0][1][0]", "$[1][1]"}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"$[0][1][0]":[3],"$[1][1]":[5]})");

  ASSERT_TRUE(json_->Set(key_, "$", R"({"x":{"y":1},"y":[2,{"z":3}],"z":{"a":{"x":4}}})").ok());
  ASSERT_TRUE(json_->Get(key_, {"$..x", "$..y", "$..z"}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"$..x":[{"y":1},4],"$..y":[[2,{"z":3}],1],"$..z":[{"a":{"x":4}},3]})");
}

TEST_F(RedisJsonTest, Print) {
  auto json = *JsonValue::FromString("[1,2,3]");
  ASSERT_EQ(json.Print().GetValue(), "[1,2,3]");
  ASSERT_EQ(json.Print(1).GetValue(), "[ 1, 2, 3]");
  ASSERT_EQ(json.Print(0, true).GetValue(), "[1,2,3]");
  ASSERT_EQ(json.Print(0, false, std::string("\n")).GetValue(), "[\n1,\n2,\n3\n]");
  ASSERT_EQ(json.Print(1, false, std::string("\n")).GetValue(), "[\n 1,\n 2,\n 3\n]");
  ASSERT_EQ(json.Print(1, true, std::string("\n")).GetValue(), "[\n 1,\n 2,\n 3\n]");

  json = *JsonValue::FromString(R"({"a":1      ,"b":2})");
  ASSERT_EQ(json.Print().GetValue(), R"({"a":1,"b":2})");
  ASSERT_EQ(json.Print(1).GetValue(), R"({ "a":1, "b":2})");
  ASSERT_EQ(json.Print(0, true).GetValue(), R"({"a": 1,"b": 2})");
  ASSERT_EQ(json.Print(0, false, std::string("\n")).GetValue(), "{\n\"a\":1,\n\"b\":2\n}");
  ASSERT_EQ(json.Print(1, false, std::string("\n")).GetValue(), "{\n \"a\":1,\n \"b\":2\n}");
  ASSERT_EQ(json.Print(1, true, std::string("\n")).GetValue(), "{\n \"a\": 1,\n \"b\": 2\n}");
}

TEST_F(RedisJsonTest, ArrAppend) {
  std::vector<uint64_t> res;

  ASSERT_FALSE(json_->ArrAppend(key_, "$", {"1"}, &res).ok());

  ASSERT_TRUE(json_->Set(key_, "$", R"({"x":1,"y":[]})").ok());
  ASSERT_TRUE(json_->ArrAppend(key_, "$.x", {"1"}, &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_EQ(res[0], 0);
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump(), R"({"x":1,"y":[]})");
  res.clear();

  ASSERT_TRUE(json_->Set(key_, "$", R"({"x":[1,2,{"z":3}],"y":[]})").ok());
  ASSERT_TRUE(json_->ArrAppend(key_, "$.x", {"1"}, &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_EQ(res[0], 4);
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump(), R"({"x":[1,2,{"z":3},1],"y":[]})");
  res.clear();

  ASSERT_TRUE(json_->ArrAppend(key_, "$..y", {"1", "2", "3"}, &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_EQ(res[0], 3);
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump(), R"({"x":[1,2,{"z":3},1],"y":[1,2,3]})");
  res.clear();

  ASSERT_TRUE(json_->Set(key_, "$.x[2]", R"({"x":[1,2,{"z":3,"y":[]}],"y":[{"y":1}]})").ok());
  ASSERT_TRUE(json_->ArrAppend(key_, "$..y", {"1", "2", "3"}, &res).ok());
  ASSERT_EQ(res.size(), 4);
  std::sort(res.begin(), res.end());
  ASSERT_EQ(res[0], 0);
  ASSERT_EQ(res[1], 3);
  ASSERT_EQ(res[2], 4);
  ASSERT_EQ(res[3], 6);
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump(), R"({"x":[1,2,{"x":[1,2,{"y":[1,2,3],"z":3}],"y":[{"y":1},1,2,3]},1],"y":[1,2,3,1,2,3]})");
  res.clear();
}
