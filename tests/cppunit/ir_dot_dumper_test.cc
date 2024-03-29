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

#include "search/ir_dot_dumper.h"

#include <regex>
#include <sstream>

#include "gtest/gtest.h"
#include "search/sql_transformer.h"

using namespace kqir;

static auto Parse(const std::string& in) { return sql::ParseToIR(peg::string_input(in, "test")); }

TEST(DotDumperTest, Simple) {
  auto ir = *Parse("select a from b where c = 1 or d hastag \"x\" and 2 <= e order by e asc limit 0, 10");

  std::stringstream ss;
  DotDumper dumper{ss};

  dumper.Dump(ir.get());

  std::string dot = ss.str();
  std::smatch matches;

  std::regex_search(dot, matches, std::regex(R"((\w+) \[ label = "SearchStmt)"));
  auto search_stmt = matches[1].str();

  std::regex_search(dot, matches, std::regex(R"((\w+) \[ label = "OrExpr)"));
  auto or_expr = matches[1].str();

  std::regex_search(dot, matches, std::regex(R"((\w+) \[ label = "AndExpr)"));
  auto and_expr = matches[1].str();

  ASSERT_NE(dot.find(fmt::format("{} -> {}", search_stmt, or_expr)), std::string::npos);
  ASSERT_NE(dot.find(fmt::format("{} -> {}", or_expr, and_expr)), std::string::npos);
}
