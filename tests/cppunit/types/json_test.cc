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

#include <cstddef>

#include "test_base.h"

class RedisJsonTest : public TestBase {
 protected:
  explicit RedisJsonTest() : json_(std::make_unique<redis::Json>(storage_.get(), "json_ns")) {}
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

  size_t result = 0;
  ASSERT_TRUE(json_->Del(key_, "$", &result).ok());
  ASSERT_TRUE(json_->Set(key_, "$", "[{ }, [ ]]").ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), "[{},[]]");
  ASSERT_THAT(json_->Set(key_, "$[1]", "invalid").ToString(), MatchesRegex(".*syntax_error.*"));
  ASSERT_TRUE(json_->Del(key_, "$", &result).ok());

  ASSERT_TRUE(json_->Set(key_, "$", R"({"a":1})").ok());
  ASSERT_TRUE(json_->Set(key_, "$.b", "2").ok());
  ASSERT_TRUE(json_->Set(key_, "$.c", R"({"x":3})").ok());
  ASSERT_TRUE(json_->Set(key_, "$.c.y", "4").ok());

  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.value, jsoncons::json::parse(R"({"a":1,"b":2,"c":{"x":3,"y":4}})"));
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
  Optionals<size_t> res;

  ASSERT_FALSE(json_->ArrAppend(key_, "$", {"1"}, &res).ok());

  ASSERT_TRUE(json_->Set(key_, "$", R"({"x":1,"y":[]})").ok());
  ASSERT_TRUE(json_->ArrAppend(key_, "$.x", {"1"}, &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_EQ(res[0], std::nullopt);
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"x":1,"y":[]})");
  res.clear();

  ASSERT_TRUE(json_->Set(key_, "$", R"({"x":[1,2,{"z":3}],"y":[]})").ok());
  ASSERT_TRUE(json_->ArrAppend(key_, "$.x", {"1"}, &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_EQ(res[0], 4);
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"x":[1,2,{"z":3},1],"y":[]})");
  res.clear();

  ASSERT_TRUE(json_->ArrAppend(key_, "$..y", {"1", "2", "3"}, &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_EQ(res[0], 3);
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"x":[1,2,{"z":3},1],"y":[1,2,3]})");
  res.clear();

  ASSERT_TRUE(json_->Set(key_, "$.x[2]", R"({"x":[1,2,{"z":3,"y":[]}],"y":[{"y":1}]})").ok());
  ASSERT_TRUE(json_->ArrAppend(key_, "$..y", {"1", "2", "3"}, &res).ok());
  ASSERT_EQ(res.size(), 4);
  std::sort(res.begin(), res.end());
  ASSERT_EQ(res[0], std::nullopt);
  ASSERT_EQ(res[1], 3);
  ASSERT_EQ(res[2], 4);
  ASSERT_EQ(res[3], 6);
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(),
            R"({"x":[1,2,{"x":[1,2,{"y":[1,2,3],"z":3}],"y":[{"y":1},1,2,3]},1],"y":[1,2,3,1,2,3]})");
  res.clear();
}

TEST_F(RedisJsonTest, Merge) {
  bool result = false;

  ASSERT_TRUE(json_->Set(key_, "$", R"({"a":2})").ok());
  ASSERT_TRUE(json_->Merge(key_, "$.a", "3", result).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), "{\"a\":3}");
  ASSERT_EQ(result, true);

  ASSERT_TRUE(json_->Set(key_, "$", R"({"a":2})").ok());
  ASSERT_TRUE(json_->Merge(key_, "$.b", "3", result).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), "{\"a\":2,\"b\":3}");
  ASSERT_EQ(result, true);

  ASSERT_TRUE(json_->Set(key_, "$", R"({"v": {"b": "cc"}})").ok());
  ASSERT_TRUE(json_->Merge(key_, "$.v.b", "null", result).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), "{\"v\":{}}");
  ASSERT_EQ(result, true);

  ASSERT_TRUE(json_->Set(key_, "$", R"({"arr":[2,4,6,8]})").ok());
  ASSERT_TRUE(json_->Merge(key_, "$.arr", "[10,12]", result).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), "{\"arr\":[10,12]}");
  ASSERT_EQ(result, true);

  ASSERT_TRUE(json_->Set(key_, "$", R"({"f1": {"a":1}, "f2":{"a":2}})").ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), "{\"f1\":{\"a\":1},\"f2\":{\"a\":2}}");
  ASSERT_TRUE(json_->Merge(key_, "$", R"({"f1": null, "f2":{"a":3, "b":4}, "f3":[2,4,6]})", result).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), "{\"f2\":{\"a\":3,\"b\":4},\"f3\":[2,4,6]}");
  ASSERT_EQ(result, true);
}

TEST_F(RedisJsonTest, Clear) {
  size_t result = 0;

  ASSERT_TRUE(
      json_
          ->Set(key_, "$",
                R"({"obj":{"a":1, "b":2}, "arr":[1,2,3], "str": "foo", "bool": true, "int": 42, "float": 3.14})")
          .ok());

  ASSERT_TRUE(json_->Clear(key_, "$", &result).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), "{}");
  ASSERT_EQ(result, 1);

  ASSERT_TRUE(
      json_
          ->Set(key_, "$",
                R"({"obj":{"a":1, "b":2}, "arr":[1,2,3], "str": "foo", "bool": true, "int": 42, "float": 3.14})")
          .ok());

  ASSERT_TRUE(json_->Clear(key_, "$.obj", &result).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"arr":[1,2,3],"bool":true,"float":3.14,"int":42,"obj":{},"str":"foo"})");
  ASSERT_EQ(result, 1);

  ASSERT_TRUE(json_->Clear(key_, "$.arr", &result).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"arr":[],"bool":true,"float":3.14,"int":42,"obj":{},"str":"foo"})");
  ASSERT_EQ(result, 1);

  ASSERT_TRUE(
      json_
          ->Set(key_, "$",
                R"({"obj":{"a":1, "b":2}, "arr":[1,2,3], "str": "foo", "bool": true, "int": 42, "float": 3.14})")
          .ok());
  ASSERT_TRUE(json_->Clear(key_, "$.*", &result).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"arr":[],"bool":true,"float":0,"int":0,"obj":{},"str":"foo"})");
  ASSERT_EQ(result, 4);

  ASSERT_TRUE(json_->Clear(key_, "$.some", &result).ok());
  ASSERT_EQ(result, 0);
}

TEST_F(RedisJsonTest, ArrLen) {
  ASSERT_TRUE(
      json_->Set(key_, "$", R"({"a1":[1,2],"a2":[[1,5,7],[8],[9]],"i":1,"d":1.0,"s":"string","o":{"a3":[1,1,1]}})")
          .ok());
  // 1. simple array
  Optionals<uint64_t> res;
  ASSERT_TRUE(json_->ArrLen(key_, "$.a1", &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_EQ(res[0], 2);
  res.clear();
  // 2. nested array
  ASSERT_TRUE(json_->ArrLen(key_, "$.a2", &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_EQ(res[0], 3);
  res.clear();
  ASSERT_TRUE(json_->ArrLen(key_, "$.a2[0]", &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_EQ(res[0], 3);
  res.clear();
  // 3.non-array type
  ASSERT_TRUE(json_->ArrLen(key_, "$.i", &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_EQ(res[0], std::nullopt);
  res.clear();
  ASSERT_TRUE(json_->ArrLen(key_, "$.d", &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_EQ(res[0], std::nullopt);
  res.clear();
  ASSERT_TRUE(json_->ArrLen(key_, "$.s", &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_EQ(res[0], std::nullopt);
  res.clear();
  // 4. object
  ASSERT_TRUE(json_->ArrLen(key_, "$.o", &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_EQ(res[0], std::nullopt);
  res.clear();
  ASSERT_TRUE(json_->ArrLen(key_, "$.o.a3", &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_EQ(res[0], 3);
  res.clear();
  // 5. key/path is not found
  ASSERT_FALSE(json_->ArrLen("not_exists", "$.*", &res).ok());
  ASSERT_TRUE(json_->ArrLen(key_, "$.not_exists", &res).ok());
  ASSERT_TRUE(res.empty());
}

TEST_F(RedisJsonTest, Toggle) {
  Optionals<bool> res;
  ASSERT_TRUE(json_->Set(key_, "$", "true").ok());
  ASSERT_TRUE(json_->Toggle(key_, "$", &res).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), "false");
  ASSERT_EQ(res.size(), 1);
  ASSERT_THAT(res, testing::ElementsAre(false));
  res.clear();

  ASSERT_TRUE(json_->Set(key_, "$", R"({"bool":true})").ok());
  ASSERT_TRUE(json_->Toggle(key_, "$.bool", &res).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"bool":false})");
  ASSERT_EQ(res.size(), 1);
  ASSERT_THAT(res, testing::ElementsAre(false));
  res.clear();

  ASSERT_TRUE(json_->Set(key_, "$", R"({"bool":true,"bools":{"bool":true}})").ok());
  ASSERT_TRUE(json_->Toggle(key_, "$.bool", &res).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"bool":false,"bools":{"bool":true}})");
  ASSERT_EQ(res.size(), 1);
  ASSERT_THAT(res, testing::ElementsAre(false));
  res.clear();

  ASSERT_TRUE(json_->Set(key_, "$", R"({"bool":true,"bools":{"bool":true}})").ok());
  ASSERT_TRUE(json_->Toggle(key_, "$..bool", &res).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"bool":false,"bools":{"bool":false}})");
  ASSERT_EQ(res.size(), 2);
  ASSERT_THAT(res, testing::ElementsAre(false, false));
  res.clear();

  ASSERT_TRUE(json_->Set(key_, "$", R"({"bool":false,"bools":{"bool":true}})").ok());
  ASSERT_TRUE(json_->Toggle(key_, "$..bool", &res).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"bool":true,"bools":{"bool":false}})");
  ASSERT_EQ(res.size(), 2);
  ASSERT_THAT(res, testing::ElementsAre(true, false));
  res.clear();

  ASSERT_TRUE(json_->Set(key_, "$", R"({"bool":false,"bools":{"bool":true},"incorrectbool":{"bool":88}})").ok());
  ASSERT_TRUE(json_->Toggle(key_, "$..bool", &res).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"bool":true,"bools":{"bool":false},"incorrectbool":{"bool":88}})");
  ASSERT_EQ(res.size(), 3);
  ASSERT_THAT(res, testing::ElementsAre(true, false, std::nullopt));
  res.clear();

  ASSERT_TRUE(json_->Set(key_, "$", "[true,true,99]").ok());
  ASSERT_TRUE(json_->Toggle(key_, "$..*", &res).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), "[false,false,99]");
  ASSERT_EQ(res.size(), 3);
  ASSERT_THAT(res, testing::ElementsAre(false, false, std::nullopt));
}

TEST_F(RedisJsonTest, ArrPop) {
  Optionals<JsonValue> res;

  // Array
  ASSERT_TRUE(json_->Set(key_, "$", R"([3,"str",2.1,{},[5,6]])").ok());
  ASSERT_TRUE(json_->ArrPop(key_, "$", -1, &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_TRUE(res[0].has_value());
  ASSERT_EQ(res[0]->Dump().GetValue(), "[5,6]");
  res.clear();
  ASSERT_TRUE(json_->ArrPop(key_, "$", -2, &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_TRUE(res[0].has_value());
  ASSERT_EQ(res[0]->Dump().GetValue(), "2.1");
  res.clear();
  ASSERT_TRUE(json_->ArrPop(key_, "$", 3, &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_TRUE(res[0].has_value());
  ASSERT_EQ(res[0]->Dump().GetValue(), "{}");
  res.clear();
  ASSERT_TRUE(json_->ArrPop(key_, "$", 1, &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_TRUE(res[0].has_value());
  ASSERT_EQ(res[0]->Dump().GetValue(), R"("str")");
  res.clear();
  ASSERT_TRUE(json_->ArrPop(key_, "$", 0, &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_TRUE(res[0].has_value());
  ASSERT_EQ(res[0]->Dump().GetValue(), "3");
  res.clear();
  ASSERT_TRUE(json_->ArrPop(key_, "$", -1, &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_FALSE(res[0].has_value());
  res.clear();

  // Non-array
  ASSERT_TRUE(json_->Set(key_, "$", R"({"o":{"x":1},"s":"str","i":1,"d":2.2})").ok());
  ASSERT_TRUE(json_->ArrPop(key_, "$.o", 1, &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_FALSE(res[0].has_value());
  res.clear();
  ASSERT_TRUE(json_->ArrPop(key_, "$.s", -1, &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_FALSE(res[0].has_value());
  res.clear();
  ASSERT_TRUE(json_->ArrPop(key_, "$.i", 0, &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_FALSE(res[0].has_value());
  res.clear();
  ASSERT_TRUE(json_->ArrPop(key_, "$.d", 2, &res).ok());
  ASSERT_EQ(res.size(), 1);
  ASSERT_FALSE(res[0].has_value());
  res.clear();

  // Multiple arrays
  ASSERT_TRUE(json_->Set(key_, "$", R"([[0,1],[3,{"x":2.0}],"str",[4,[5,"6"]]])").ok());
  ASSERT_TRUE(json_->ArrPop(key_, "$.*", -1, &res).ok());
  ASSERT_EQ(res.size(), 4);
  ASSERT_TRUE(res[0].has_value());
  ASSERT_EQ(res[0]->Dump().GetValue(), R"([5,"6"])");
  ASSERT_FALSE(res[1].has_value());
  ASSERT_TRUE(res[2].has_value());
  ASSERT_EQ(res[2]->Dump().GetValue(), R"({"x":2.0})");
  ASSERT_TRUE(res[3].has_value());
  ASSERT_EQ(res[3]->Dump().GetValue(), "1");
  res.clear();
}

TEST_F(RedisJsonTest, ArrIndex) {
  Optionals<ssize_t> res;
  int max_end = std::numeric_limits<int>::max();

  ASSERT_TRUE(json_->Set(key_, "$", R"({"arr":[0, 1, 2, 3, 2, 1, 0]})").ok());
  ASSERT_TRUE(json_->ArrIndex(key_, "$.arr", "0", 0, max_end, &res).ok() && res.size() == 1);
  ASSERT_EQ(res[0], 0);

  ASSERT_TRUE(json_->ArrIndex(key_, "$.arr", "3", 0, max_end, &res).ok() && res.size() == 1);
  ASSERT_EQ(res.size(), 1);
  ASSERT_EQ(res[0], 3);

  ASSERT_TRUE(json_->ArrIndex(key_, "$.arr", "4", 0, max_end, &res).ok() && res.size() == 1);
  ASSERT_EQ(res[0], -1);

  ASSERT_TRUE(json_->ArrIndex(key_, "$.arr", "0", 1, max_end, &res).ok() && res.size() == 1);
  ASSERT_EQ(res[0], 6);

  ASSERT_TRUE(json_->ArrIndex(key_, "$.arr", "0", -1, max_end, &res).ok() && res.size() == 1);
  ASSERT_EQ(res[0], 6);

  ASSERT_TRUE(json_->ArrIndex(key_, "$.arr", "0", 6, max_end, &res).ok() && res.size() == 1);
  ASSERT_EQ(res[0], 6);

  ASSERT_TRUE(json_->ArrIndex(key_, "$.arr", "0", 5, -1, &res).ok() && res.size() == 1);
  ASSERT_EQ(res[0], -1);

  ASSERT_TRUE(json_->ArrIndex(key_, "$.arr", "0", 5, 0, &res).ok() && res.size() == 1);
  ASSERT_EQ(res[0], 6);

  ASSERT_TRUE(json_->ArrIndex(key_, "$.arr", "2", -2, 6, &res).ok() && res.size() == 1);
  ASSERT_EQ(res[0], -1);

  ASSERT_TRUE(json_->ArrIndex(key_, "$.arr", "\"foo\"", 0, max_end, &res).ok() && res.size() == 1);
  ASSERT_EQ(res[0], -1);

  ASSERT_TRUE(json_->Set(key_, "$", R"({"arr":[0, 1, 2, 3, 4, 2, 1, 0]})").ok());

  ASSERT_TRUE(json_->ArrIndex(key_, "$.arr", "3", 0, max_end, &res).ok() && res.size() == 1);
  ASSERT_EQ(res[0], 3);

  ASSERT_TRUE(json_->ArrIndex(key_, "$.arr", "2", 3, max_end, &res).ok() && res.size() == 1);
  ASSERT_EQ(res[0], 5);

  ASSERT_TRUE(json_->ArrIndex(key_, "$.arr", "1", 0, max_end, &res).ok() && res.size() == 1);
  ASSERT_EQ(res[0], 1);

  ASSERT_TRUE(json_->ArrIndex(key_, "$.arr", "2", 1, 4, &res).ok() && res.size() == 1);
  ASSERT_EQ(res[0], 2);

  ASSERT_TRUE(json_->ArrIndex(key_, "$.arr", "6", 0, max_end, &res).ok() && res.size() == 1);
  ASSERT_EQ(res[0], -1);

  ASSERT_TRUE(json_->ArrIndex(key_, "$.arr", "3", 0, 2, &res).ok() && res.size() == 1);
  ASSERT_EQ(res[0], -1);
}

TEST_F(RedisJsonTest, Del) {
  size_t result = 0;

  ASSERT_TRUE(
      json_
          ->Set(key_, "$",
                R"({"obj":{"a":1, "b":2}, "arr":[1,2,3], "str": "foo", "bool": true, "int": 42, "float": 3.14})")
          .ok());

  ASSERT_TRUE(json_->Del(key_, "$", &result).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).IsNotFound());
  ASSERT_EQ(result, 1);

  ASSERT_TRUE(
      json_
          ->Set(key_, "$",
                R"({"obj":{"a":1, "b":2}, "arr":[1,2,3], "str": "foo", "bool": true, "int": 42, "float": 3.14})")
          .ok());

  ASSERT_TRUE(json_->Del(key_, "$.obj", &result).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"arr":[1,2,3],"bool":true,"float":3.14,"int":42,"str":"foo"})");
  ASSERT_EQ(result, 1);

  ASSERT_TRUE(json_->Del(key_, "$.arr", &result).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"bool":true,"float":3.14,"int":42,"str":"foo"})");
  ASSERT_EQ(result, 1);

  ASSERT_TRUE(
      json_
          ->Set(key_, "$",
                R"({"obj":{"a":1, "b":2}, "arr":[1,2,3], "str": "foo", "bool": true, "int": 42, "float": 3.14})")
          .ok());
  ASSERT_TRUE(json_->Del(key_, "$.*", &result).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({})");
  ASSERT_EQ(result, 6);

  ASSERT_TRUE(json_->Del(key_, "$.some", &result).ok());
  ASSERT_EQ(result, 0);

  ASSERT_TRUE(json_->Set(key_, "$", R"({"a": 1, "nested": {"a": 2, "b": 3}})").ok());
  ASSERT_TRUE(json_->Del(key_, "$..a", &result).ok());
  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"nested":{"b":3}})");
  ASSERT_EQ(result, 2);
}

TEST_F(RedisJsonTest, NumIncrBy) {
  ASSERT_TRUE(json_->Set(key_, "$", R"({ "foo": 0, "bar": "baz" })").ok());
  JsonValue res = JsonValue::FromString("[]").GetValue();
  ASSERT_TRUE(json_->NumIncrBy(key_, "$.foo", "1", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[1]");
  res.value.clear();
  ASSERT_TRUE(json_->NumIncrBy(key_, "$.foo", "2", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[3]");
  res.value.clear();
  ASSERT_TRUE(json_->NumIncrBy(key_, "$.foo", "0.5", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[3.5]");
  res.value.clear();

  ASSERT_TRUE(json_->NumIncrBy(key_, "$.bar", "1", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[null]");
  res.value.clear();

  ASSERT_TRUE(json_->NumIncrBy(key_, "$.fuzz", "1", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[]");
  res.value.clear();

  ASSERT_TRUE(json_->Set(key_, "$", "0").ok());
  ASSERT_TRUE(json_->NumIncrBy(key_, "$", "0", &res).ok());
  res.value.clear();
  ASSERT_TRUE(json_->NumIncrBy(key_, "$", "1", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[1]");
  res.value.clear();
  ASSERT_TRUE(json_->NumIncrBy(key_, "$", "1.5", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[2.5]");
  res.value.clear();

  // overflow case
  ASSERT_TRUE(json_->Set(key_, "$", "1.6350000000001313e+308").ok());
  ASSERT_TRUE(json_->NumIncrBy(key_, "$", "1", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[1.6350000000001313e+308]");
  res.value.clear();

  ASSERT_TRUE(json_->NumIncrBy(key_, "$", "2", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[1.6350000000001313e+308]");
  res.value.clear();

  // nested big_num object
  ASSERT_TRUE(json_->Set(key_, "$", R"({"l1":{"l2_a":1.6350000000001313e+308,"l2_b":2}})").ok());
  ASSERT_TRUE(json_->NumIncrBy(key_, "$.l1.l2_a", "1", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[1.6350000000001313e+308]");
  res.value.clear();

  ASSERT_TRUE(json_->NumIncrBy(key_, "$.l1.l2_a", "2", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[1.6350000000001313e+308]");
  res.value.clear();

  // nested big_num array
  ASSERT_TRUE(json_->Set(key_, "$", R"({"l1":{"l2":[0,1.6350000000001313e+308]}})").ok());
  ASSERT_FALSE(json_->NumIncrBy(key_, "$.l1.l2[1]", "1.6350000000001313e+308", &res).ok());
  res.value.clear();

  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"l1":{"l2":[0,1.6350000000001313e+308]}})");
}

TEST_F(RedisJsonTest, NumMultBy) {
  ASSERT_TRUE(json_->Set(key_, "$", R"({ "foo": 1, "bar": "baz" })").ok());
  JsonValue res = JsonValue::FromString("[]").GetValue();
  ASSERT_TRUE(json_->NumMultBy(key_, "$.foo", "1", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[1]");
  res.value.clear();
  ASSERT_TRUE(json_->NumMultBy(key_, "$.foo", "2", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[2]");
  res.value.clear();
  ASSERT_TRUE(json_->NumMultBy(key_, "$.foo", "0.5", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[1.0]");
  res.value.clear();

  ASSERT_TRUE(json_->NumMultBy(key_, "$.bar", "1", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[null]");
  res.value.clear();

  ASSERT_TRUE(json_->NumMultBy(key_, "$.fuzz", "1", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[]");
  res.value.clear();

  // num object
  ASSERT_TRUE(json_->Set(key_, "$", "1.0").ok());
  ASSERT_TRUE(json_->NumMultBy(key_, "$", "1", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[1.0]");
  res.value.clear();
  ASSERT_TRUE(json_->NumMultBy(key_, "$", "1.5", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[1.5]");
  res.value.clear();

  // overflow case
  ASSERT_TRUE(json_->Set(key_, "$", "1.6350000000001313e+308").ok());
  ASSERT_TRUE(json_->NumMultBy(key_, "$", "1", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[1.6350000000001313e+308]");
  res.value.clear();

  ASSERT_FALSE(json_->NumMultBy(key_, "$", "2", &res).ok());
  res.value.clear();

  // nested big_num object
  ASSERT_TRUE(json_->Set(key_, "$", R"({"l1":{"l2_a":1.6350000000001313e+308,"l2_b":2}})").ok());
  ASSERT_TRUE(json_->NumMultBy(key_, "$.l1.l2_a", "1", &res).ok());
  ASSERT_EQ(res.Print(0, true).GetValue(), "[1.6350000000001313e+308]");
  res.value.clear();

  ASSERT_FALSE(json_->NumMultBy(key_, "$.l1.l2_a", "2", &res).ok());
  res.value.clear();

  // nested big_num array
  ASSERT_TRUE(json_->Set(key_, "$", R"({"l1":{"l2":[0,1.6350000000001313e+308]}})").ok());
  ASSERT_FALSE(json_->NumMultBy(key_, "$.l1.l2[1]", "2", &res).ok());
  res.value.clear();

  ASSERT_TRUE(json_->Get(key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"l1":{"l2":[0,1.6350000000001313e+308]}})");
}

TEST_F(RedisJsonTest, StrAppend) {
  Optionals<uint64_t> results;
  ASSERT_TRUE(json_->Set(key_, "$", R"({"a":"foo", "nested": {"a": "hello"}, "nested2": {"a": 31}})").ok());
  ASSERT_TRUE(json_->StrAppend(key_, "$.a", "\"be\"", &results).ok());
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0], 5);

  results.clear();
  ASSERT_TRUE(json_->Set(key_, "$", R"({"a":"foo", "nested": {"a": "hello"}, "nested2": {"a": 31}})").ok());
  ASSERT_TRUE(json_->StrAppend(key_, "$..a", "\"be\"", &results).ok());
  ASSERT_EQ(results.size(), 3);
  Optionals<int64_t> result1 = {5, 7, std::nullopt};
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(results[i], result1[i]);
  }
}

TEST_F(RedisJsonTest, StrLen) {
  Optionals<uint64_t> results;
  ASSERT_TRUE(json_->Set(key_, "$", R"({"a":"foo", "nested": {"a": "hello"}, "nested2": {"a": 31}})").ok());
  ASSERT_TRUE(json_->StrLen(key_, "$.a", &results).ok());
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0], 3);

  results.clear();
  ASSERT_TRUE(json_->Set(key_, "$", R"({"a":"foo", "nested": {"a": "hello"}, "nested2": {"a": 31}})").ok());
  ASSERT_TRUE(json_->StrLen(key_, "$..a", &results).ok());
  ASSERT_EQ(results.size(), 3);
  Optionals<int64_t> result1 = {3, 5, std::nullopt};
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(results[i], result1[i]);
  }
}
