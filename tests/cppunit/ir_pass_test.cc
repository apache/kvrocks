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

#include "search/ir_pass.h"

#include "gtest/gtest.h"
#include "search/passes/simplify_boolean.h"
#include "search/sql_transformer.h"

using namespace kqir;

static auto Parse(const std::string& in) { return sql::ParseToIR(peg::string_input(in, "test")); }

TEST(IRPassTest, Simple) {
  auto ir = *Parse("select a from b where not c = 1 or d hastag \"x\" and 2 <= e order by e asc limit 0, 10");

  auto original = ir->Dump();

  Visitor visitor;
  auto ir2 = visitor.Transform(std::move(ir));
  ASSERT_EQ(original, ir2->Dump());
}

TEST(IRPassTest, SimplifyBoolean) {
  SimplifyBoolean sb;
  ASSERT_EQ(sb.Transform(*Parse("select a from b where not false"))->Dump(), "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where not not false"))->Dump(), "select a from b where false");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where true and true"))->Dump(), "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where true and false"))->Dump(), "select a from b where false");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where false and true"))->Dump(), "select a from b where false");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where true and false and true"))->Dump(),
            "select a from b where false");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where true and true and true"))->Dump(), "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where x > 1 and false"))->Dump(), "select a from b where false");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where x > 1 and true"))->Dump(), "select a from b where x > 1");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where x > 1 and true and y < 10"))->Dump(),
            "select a from b where (and x > 1, y < 10)");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where not (false and (not true))"))->Dump(),
            "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where true or true"))->Dump(), "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where true or false"))->Dump(), "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where false or true"))->Dump(), "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where true or false or true"))->Dump(), "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where true or false or true"))->Dump(), "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where not ((x < 1 or true) and (y > 2 and true))"))->Dump(),
            "select a from b where not y > 2");
}
