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

#include "search/ir_sema_checker.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "gtest/gtest.h"
#include "search/search_encoding.h"
#include "search/sql_transformer.h"
#include "storage/redis_metadata.h"

using namespace kqir;

static auto Parse(const std::string& in) { return sql::ParseToIR(peg::string_input(in, "test")); }

IndexMap MakeIndexMap() {
  auto f1 = FieldInfo("f1", std::make_unique<redis::SearchTagFieldMetadata>());
  auto f2 = FieldInfo("f2", std::make_unique<redis::SearchNumericFieldMetadata>());
  auto f3 = FieldInfo("f3", std::make_unique<redis::SearchNumericFieldMetadata>());
  auto ia = IndexInfo("ia", SearchMetadata());
  ia.Add(std::move(f1));
  ia.Add(std::move(f2));
  ia.Add(std::move(f3));

  auto& name = ia.name;
  IndexMap res;
  res.emplace(name, std::move(ia));
  return res;
}

using testing::MatchesRegex;

TEST(SemaCheckerTest, Simple) {
  auto index_map = MakeIndexMap();

  {
    SemaChecker checker(index_map);
    ASSERT_EQ(checker.Check(Parse("select a from b")->get()).Msg(), "index `b` not found");
    ASSERT_EQ(checker.Check(Parse("select a from ia")->get()).Msg(), "field `a` not found in index `ia`");
    ASSERT_EQ(checker.Check(Parse("select f1 from ia")->get()).Msg(), "ok");
    ASSERT_EQ(checker.Check(Parse("select f1 from ia where b = 1")->get()).Msg(), "field `b` not found in index `ia`");
    ASSERT_EQ(checker.Check(Parse("select f1 from ia where f1 = 1")->get()).Msg(), "field `f1` is not a numeric field");
    ASSERT_EQ(checker.Check(Parse("select f1 from ia where f2 hastag \"a\"")->get()).Msg(),
              "field `f2` is not a tag field");
    ASSERT_EQ(checker.Check(Parse("select f1 from ia where f1 hastag \"a\" and f2 = 1")->get()).Msg(), "ok");
    ASSERT_EQ(checker.Check(Parse("select f1 from ia where f1 hastag \"\"")->get()).Msg(),
              "tag cannot be an empty string");
    ASSERT_EQ(checker.Check(Parse("select f1 from ia where f1 hastag \",\"")->get()).Msg(),
              "tag cannot contain the separator `,`");
    ASSERT_EQ(checker.Check(Parse("select f1 from ia order by a")->get()).Msg(), "field `a` not found in index `ia`");
  }

  {
    SemaChecker checker(index_map);
    auto root = *Parse("select f1 from ia where f1 hastag \"a\" and f2 = 1 order by f3");

    ASSERT_EQ(checker.Check(root.get()).Msg(), "ok");
  }
}
