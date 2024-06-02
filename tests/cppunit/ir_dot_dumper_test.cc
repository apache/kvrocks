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
#include "search/ir_plan.h"
#include "search/ir_sema_checker.h"
#include "search/passes/manager.h"
#include "search/search_encoding.h"
#include "search/sql_transformer.h"
#include "storage/redis_metadata.h"

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

static auto ParseS(SemaChecker& sc, const std::string& in) {
  auto ir = *Parse(in);
  EXPECT_EQ(sc.Check(ir.get()).Msg(), Status::ok_msg);
  return ir;
}

static IndexMap MakeIndexMap() {
  auto f1 = FieldInfo("t1", std::make_unique<redis::TagFieldMetadata>());
  auto f2 = FieldInfo("t2", std::make_unique<redis::TagFieldMetadata>());
  f2.metadata->noindex = true;
  auto f3 = FieldInfo("n1", std::make_unique<redis::NumericFieldMetadata>());
  auto f4 = FieldInfo("n2", std::make_unique<redis::NumericFieldMetadata>());
  auto f5 = FieldInfo("n3", std::make_unique<redis::NumericFieldMetadata>());
  f5.metadata->noindex = true;
  auto ia = std::make_unique<IndexInfo>("ia", redis::IndexMetadata(), "");
  ia->Add(std::move(f1));
  ia->Add(std::move(f2));
  ia->Add(std::move(f3));
  ia->Add(std::move(f4));
  ia->Add(std::move(f5));

  IndexMap res;
  res.Insert(std::move(ia));
  return res;
}

TEST(DotDumperTest, Plan) {
  auto index_map = MakeIndexMap();
  SemaChecker sc(index_map);
  auto plan = PassManager::Execute(
      PassManager::Default(),
      ParseS(
          sc,
          "select * from ia where (n1 < 2 or n1 >= 3) and (n1 >= 1 and n1 < 4) and not n3 != 1 and t2 hastag \"a\""));

  std::stringstream ss;
  DotDumper dd(ss);
  dd.Dump(plan.get());

  std::string dot = ss.str();
  std::smatch matches;

  std::regex_search(dot, matches, std::regex(R"((\w+) \[ label = "Filter)"));
  auto filter = matches[1].str();

  std::regex_search(dot, matches, std::regex(R"((\w+) \[ label = "Merge)"));
  auto merge = matches[1].str();

  std::regex_search(dot, matches, std::regex(R"((\w+) \[ label = "NumericFieldScan)"));
  auto scan = matches[1].str();

  ASSERT_NE(dot.find(fmt::format("{} -> {}", filter, merge)), std::string::npos);
  ASSERT_NE(dot.find(fmt::format("{} -> {}", merge, scan)), std::string::npos);
}
