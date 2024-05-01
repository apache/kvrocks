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
#include "search/plan_executor.h"

#include <gtest/gtest.h>

#include <memory>

#include "search/executors/mock_executor.h"
#include "search/ir.h"
#include "search/ir_plan.h"

using namespace kqir;

static auto exe_end = ExecutorNode::Result(ExecutorNode::end);

static IndexMap MakeIndexMap() {
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

static auto index_map = MakeIndexMap();

static auto NextRow(ExecutorContext& ctx) { return std::get<ExecutorNode::RowType>(ctx.Next().GetValue()); }

TEST(PlanExecutorTest, Mock) {
  auto op = std::make_unique<Mock>(std::vector<ExecutorNode::RowType>{});

  auto ctx = ExecutorContext(op.get());
  ASSERT_EQ(ctx.Next().GetValue(), exe_end);

  op = std::make_unique<Mock>(std::vector<ExecutorNode::RowType>{{"a"}, {"b"}, {"c"}});

  ctx = ExecutorContext(op.get());
  ASSERT_EQ(NextRow(ctx).key, "a");
  ASSERT_EQ(NextRow(ctx).key, "b");
  ASSERT_EQ(NextRow(ctx).key, "c");
  ASSERT_EQ(ctx.Next().GetValue(), exe_end);
}

static auto IndexI() -> const IndexInfo* { return &index_map.at("ia"); }
static auto FieldI(const std::string& f) -> const FieldInfo* { return &index_map.at("ia").fields.at(f); }

TEST(PlanExecutorTest, TopNSort) {
  std::vector<ExecutorNode::RowType> data{
      {"a", {{FieldI("f3"), "4"}}, IndexI()}, {"b", {{FieldI("f3"), "2"}}, IndexI()},
      {"c", {{FieldI("f3"), "7"}}, IndexI()}, {"d", {{FieldI("f3"), "3"}}, IndexI()},
      {"e", {{FieldI("f3"), "1"}}, IndexI()}, {"f", {{FieldI("f3"), "6"}}, IndexI()},
      {"g", {{FieldI("f3"), "8"}}, IndexI()},
  };
  {
    auto op = std::make_unique<TopNSort>(
        std::make_unique<Mock>(data),
        std::make_unique<SortByClause>(SortByClause::ASC, std::make_unique<FieldRef>("f3", FieldI("f3"))),
        std::make_unique<LimitClause>(0, 4));

    auto ctx = ExecutorContext(op.get());
    ASSERT_EQ(NextRow(ctx).key, "e");
    ASSERT_EQ(NextRow(ctx).key, "b");
    ASSERT_EQ(NextRow(ctx).key, "d");
    ASSERT_EQ(NextRow(ctx).key, "a");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }
  {
    auto op = std::make_unique<TopNSort>(
        std::make_unique<Mock>(data),
        std::make_unique<SortByClause>(SortByClause::ASC, std::make_unique<FieldRef>("f3", FieldI("f3"))),
        std::make_unique<LimitClause>(1, 4));

    auto ctx = ExecutorContext(op.get());
    ASSERT_EQ(NextRow(ctx).key, "b");
    ASSERT_EQ(NextRow(ctx).key, "d");
    ASSERT_EQ(NextRow(ctx).key, "a");
    ASSERT_EQ(NextRow(ctx).key, "f");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }
}
