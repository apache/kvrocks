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
#include "search/interval.h"
#include "search/ir_sema_checker.h"
#include "search/passes/interval_analysis.h"
#include "search/passes/lower_to_plan.h"
#include "search/passes/manager.h"
#include "search/passes/push_down_not_expr.h"
#include "search/passes/simplify_and_or_expr.h"
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

TEST(IRPassTest, SimplifyAndOrExpr) {
  SimplifyAndOrExpr saoe;

  ASSERT_EQ(Parse("select a from b where true and (false and true)").GetValue()->Dump(),
            "select a from b where (and true, (and false, true))");
  ASSERT_EQ(saoe.Transform(*Parse("select a from b where true and (false and true)"))->Dump(),
            "select a from b where (and true, false, true)");
  ASSERT_EQ(saoe.Transform(*Parse("select a from b where true or (false or true)"))->Dump(),
            "select a from b where (or true, false, true)");
  ASSERT_EQ(saoe.Transform(*Parse("select a from b where true and (false or true)"))->Dump(),
            "select a from b where (and true, (or false, true))");
  ASSERT_EQ(saoe.Transform(*Parse("select a from b where true or (false and true)"))->Dump(),
            "select a from b where (or true, (and false, true))");
  ASSERT_EQ(saoe.Transform(*Parse("select a from b where x > 1 or (y < 2 or z = 3)"))->Dump(),
            "select a from b where (or x > 1, y < 2, z = 3)");
}

TEST(IRPassTest, PushDownNotExpr) {
  PushDownNotExpr pdne;

  ASSERT_EQ(pdne.Transform(*Parse("select * from a where not a > 1"))->Dump(), "select * from a where a <= 1");
  ASSERT_EQ(pdne.Transform(*Parse("select * from a where not a hastag \"\""))->Dump(),
            "select * from a where not a hastag \"\"");
  ASSERT_EQ(pdne.Transform(*Parse("select * from a where not not a > 1"))->Dump(), "select * from a where a > 1");
  ASSERT_EQ(pdne.Transform(*Parse("select * from a where not (a > 1 and b <= 3)"))->Dump(),
            "select * from a where (or a <= 1, b > 3)");
  ASSERT_EQ(pdne.Transform(*Parse("select * from a where not (a > 1 or b <= 3)"))->Dump(),
            "select * from a where (and a <= 1, b > 3)");
  ASSERT_EQ(pdne.Transform(*Parse("select * from a where not (not a > 1 or (b < 3 and c hastag \"\"))"))->Dump(),
            "select * from a where (and a > 1, (or b >= 3, not c hastag \"\"))");
}

TEST(IRPassTest, Manager) {
  auto expr_passes = PassManager::ExprPasses();
  ASSERT_EQ(PassManager::Execute(expr_passes,
                                 *Parse("select * from a where not (x > 1 or (y < 2 or z = 3)) and (true or x = 1)"))
                ->Dump(),
            "select * from a where (and x <= 1, y >= 2, z != 3)");
}

TEST(IRPassTest, LowerToPlan) {
  LowerToPlan ltp;

  ASSERT_EQ(ltp.Transform(*Parse("select * from a"))->Dump(), "project *: full-scan a");
  ASSERT_EQ(ltp.Transform(*Parse("select * from a limit 1"))->Dump(), "project *: (limit 0, 1: full-scan a)");
  ASSERT_EQ(ltp.Transform(*Parse("select * from a where false"))->Dump(), "project *: noop");
  ASSERT_EQ(ltp.Transform(*Parse("select * from a where false limit 1"))->Dump(), "project *: noop");
  ASSERT_EQ(ltp.Transform(*Parse("select * from a where b > 1"))->Dump(), "project *: (filter b > 1: full-scan a)");
  ASSERT_EQ(ltp.Transform(*Parse("select a from b where c = 1 order by d"))->Dump(),
            "project a: (sort d, asc: (filter c = 1: full-scan b))");
  ASSERT_EQ(ltp.Transform(*Parse("select a from b where c = 1 limit 1"))->Dump(),
            "project a: (limit 0, 1: (filter c = 1: full-scan b))");
  ASSERT_EQ(ltp.Transform(*Parse("select a from b where c = 1 order by d limit 1"))->Dump(),
            "project a: (limit 0, 1: (sort d, asc: (filter c = 1: full-scan b)))");
}

TEST(IRPassTest, IntervalAnalysis) {
  auto ia_passes = PassManager::Create(IntervalAnalysis{true}, SimplifyAndOrExpr{}, SimplifyBoolean{});

  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a > 1 or a < 3"))->Dump(),
            "select * from a where true");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a < 1 and a > 3"))->Dump(),
            "select * from a where false");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where (a > 3 or a < 1) and a = 2"))->Dump(),
            "select * from a where false");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where b = 1 and (a = 1 or a != 1)"))->Dump(),
            "select * from a where b = 1");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a = 1 or b = 1 or a != 1"))->Dump(),
            "select * from a where true");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where (a < 3 or a > 1) and b >= 1"))->Dump(),
            "select * from a where b >= 1");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a != 1 or a != 2"))->Dump(),
            "select * from a where true");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a = 1 and a = 2"))->Dump(),
            "select * from a where false");

  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a < 1 and a < 3"))->Dump(),
            "select * from a where a < 1");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a < 1 or a < 3"))->Dump(),
            "select * from a where a < 3");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a = 1 and a < 3"))->Dump(),
            "select * from a where a = 1");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a = 1 or a < 3"))->Dump(),
            "select * from a where a < 3");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a = 1 or a = 3"))->Dump(),
            "select * from a where (or a = 1, a = 3)");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a != 1"))->Dump(),
            "select * from a where a != 1");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a != 1 and a != 2"))->Dump(),
            "select * from a where (and a != 1, a != 2)");
  ASSERT_EQ(
      PassManager::Execute(ia_passes, *Parse("select * from a where a >= 0 and a >= 1 and a < 4 and a != 2"))->Dump(),
      fmt::format("select * from a where (or (and a >= 1, a < 2), (and a >= {}, a < 4))", IntervalSet::NextNum(2)));
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a != 1 and b > 1 and b = 2"))->Dump(),
            "select * from a where (and a != 1, b = 2)");
}

static IndexMap MakeIndexMap() {
  auto f1 = FieldInfo("t1", std::make_unique<redis::SearchTagFieldMetadata>());
  auto f2 = FieldInfo("t2", std::make_unique<redis::SearchNumericFieldMetadata>());
  f2.metadata->noindex = true;
  auto f3 = FieldInfo("n1", std::make_unique<redis::SearchNumericFieldMetadata>());
  auto f4 = FieldInfo("n2", std::make_unique<redis::SearchNumericFieldMetadata>());
  auto f5 = FieldInfo("n3", std::make_unique<redis::SearchNumericFieldMetadata>());
  f5.metadata->noindex = true;
  auto ia = IndexInfo("ia", SearchMetadata());
  ia.Add(std::move(f1));
  ia.Add(std::move(f2));
  ia.Add(std::move(f3));
  ia.Add(std::move(f4));
  ia.Add(std::move(f5));

  auto& name = ia.name;
  IndexMap res;
  res.emplace(name, std::move(ia));
  return res;
}

std::unique_ptr<Node> ParseS(SemaChecker& sc, const std::string& in) {
  auto res = *Parse(in);
  EXPECT_EQ(sc.Check(res.get()).Msg(), Status::ok_msg);
  return res;
}

TEST(IRPassTest, IndexSelection) {
  auto index_map = MakeIndexMap();
  auto sc = SemaChecker(index_map);

  auto passes = PassManager::Default();
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia"))->Dump(), "project *: full-scan ia");
}
