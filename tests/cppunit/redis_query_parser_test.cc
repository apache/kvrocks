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
#include <search/redis_query_transformer.h>

#include "search/common_transformer.h"
#include "tao/pegtl/string_input.hpp"

using namespace kqir::redis_query;

static auto Parse(const std::string& in, const kqir::ParamMap& pm = {}) {
  return ParseToIR(string_input(in, "test"), pm);
}

#define AssertSyntaxError(node) ASSERT_EQ(node.Msg(), "invalid syntax");  // NOLINT

// NOLINTNEXTLINE
#define AssertIR(node, val)              \
  ASSERT_EQ(node.Msg(), Status::ok_msg); \
  ASSERT_EQ(node.GetValue()->Dump(), val);

TEST(RedisQueryParserTest, Simple) {
  AssertSyntaxError(Parse(""));
  AssertSyntaxError(Parse("a"));
  AssertSyntaxError(Parse("@a"));
  AssertSyntaxError(Parse("a:"));
  AssertSyntaxError(Parse("@a:"));
  AssertSyntaxError(Parse("@a:[]"));
  AssertSyntaxError(Parse("@a:[1 2"));
  AssertSyntaxError(Parse("@a:[(inf 1]"));
  AssertSyntaxError(Parse("@a:[((1 2]"));
  AssertSyntaxError(Parse("@a:[1]"));
  AssertSyntaxError(Parse("@a:[1 2 3]"));
  AssertSyntaxError(Parse("@a:{}"));
  AssertSyntaxError(Parse("@a:{x"));
  AssertSyntaxError(Parse("@a:{|}"));
  AssertSyntaxError(Parse("@a:{x|}"));
  AssertSyntaxError(Parse("@a:{|y}"));
  AssertSyntaxError(Parse("@a:{x|y|}"));
  AssertSyntaxError(Parse("@a:{x}|"));
  AssertSyntaxError(Parse("@a:{x} -"));
  AssertSyntaxError(Parse("@a:{x}|@a:{x}|"));

  AssertIR(Parse("@a:[1 2]"), "(and a >= 1, a <= 2)");
  AssertIR(Parse("@a : [1 2]"), "(and a >= 1, a <= 2)");
  AssertIR(Parse("@a:[(1 2]"), "(and a > 1, a <= 2)");
  AssertIR(Parse("@a:[1 (2]"), "(and a >= 1, a < 2)");
  AssertIR(Parse("@a:[(1 (2]"), "(and a > 1, a < 2)");
  AssertIR(Parse("@a:[inf 2]"), "a <= 2");
  AssertIR(Parse("@a:[-inf 2]"), "a <= 2");
  AssertIR(Parse("@a:[1 inf]"), "a >= 1");
  AssertIR(Parse("@a:[1 +inf]"), "a >= 1");
  AssertIR(Parse("@a:[(1 +inf]"), "a > 1");
  AssertIR(Parse("@a:[-inf +inf]"), "true");
  AssertIR(Parse("@a:{x}"), "a hastag \"x\"");
  AssertIR(Parse("@a:{x|y}"), R"((or a hastag "x", a hastag "y"))");
  AssertIR(Parse("@a:{x|y|z}"), R"((or a hastag "x", a hastag "y", a hastag "z"))");
  AssertIR(Parse(R"(@a:{"x"|y})"), R"((or a hastag "x", a hastag "y"))");
  AssertIR(Parse(R"(@a:{"x" | "y"})"), R"((or a hastag "x", a hastag "y"))");
  AssertIR(Parse("@a:{x} @b:[1 inf]"), "(and a hastag \"x\", b >= 1)");
  AssertIR(Parse("@a:{x} | @b:[1 inf]"), "(or a hastag \"x\", b >= 1)");
  AssertIR(Parse("@a:{x} @b:[1 inf] @c:{y}"), "(and a hastag \"x\", b >= 1, c hastag \"y\")");
  AssertIR(Parse("@a:{x}|@b:[1 inf] | @c:{y}"), "(or a hastag \"x\", b >= 1, c hastag \"y\")");
  AssertIR(Parse("@a:[1 inf] @b:[inf 2]| @c:[(3 inf]"), "(or (and a >= 1, b <= 2), c > 3)");
  AssertIR(Parse("@a:[1 inf] | @b:[inf 2] @c:[(3 inf]"), "(or a >= 1, (and b <= 2, c > 3))");
  AssertIR(Parse("(@a:[1 inf] @b:[inf 2])| @c:[(3 inf]"), "(or (and a >= 1, b <= 2), c > 3)");
  AssertIR(Parse("@a:[1 inf] | (@b:[inf 2] @c:[(3 inf])"), "(or a >= 1, (and b <= 2, c > 3))");
  AssertIR(Parse("@a:[1 inf] (@b:[inf 2]| @c:[(3 inf])"), "(and a >= 1, (or b <= 2, c > 3))");
  AssertIR(Parse("(@a:[1 inf] | @b:[inf 2]) @c:[(3 inf]"), "(and (or a >= 1, b <= 2), c > 3)");
  AssertIR(Parse("-@a:{x}"), "not a hastag \"x\"");
  AssertIR(Parse("-@a:[(1 +inf]"), "not a > 1");
  AssertIR(Parse("-@a:[1 inf] @b:[inf 2]| -@c:[(3 inf]"), "(or (and not a >= 1, b <= 2), not c > 3)");
  AssertIR(Parse("@a:[1 inf] -(@b:[inf 2]| @c:[(3 inf])"), "(and a >= 1, not (or b <= 2, c > 3))");
  AssertIR(Parse("*"), "true");
  AssertIR(Parse("* *"), "(and true, true)");
  AssertIR(Parse("*|*"), "(or true, true)");
}

TEST(RedisQueryParserTest, Params) {
  AssertIR(Parse("@c:[$left ($right]", {{"left", "1"}, {"right", "2"}}), "(and c >= 1, c < 2)");
  AssertIR(Parse("@c:[($x $x]", {{"x", "2"}}), "(and c > 2, c <= 2)");
  AssertIR(Parse("@c:{$y}", {{"y", "hello"}}), "c hastag \"hello\"");
  AssertIR(Parse("@c:{$y} @d:[$zzz inf]", {{"y", "hello"}, {"zzz", "3"}}), "(and c hastag \"hello\", d >= 3)");
  ASSERT_EQ(Parse("@c:{$y}", {{"z", "hello"}}).Msg(), "parameter with name `y` not found");
}

TEST(RedisQueryParserTest, Vector) {
  std::vector<double> vec = {1, 2, 3};
  std::string vec_str(reinterpret_cast<const char*>(vec.data()), vec.size() * sizeof(double));

  AssertSyntaxError(Parse("@field:[RANGE 10 $vector]", {{"vector", vec_str}}));
  AssertSyntaxError(Parse("@field:[VECTOR_RANGE 10 not_param"));
  AssertSyntaxError(Parse("@field:[VECTOR_RANGE $vector]", {{"vector", vec_str}}));
  AssertSyntaxError(Parse("@field:[VECTOR_RANGE $vector 10]", {{"vector", vec_str}}));
  AssertSyntaxError(Parse("* =>[knn 5 @field $BLOB]", {{"BLOB", vec_str}}));
  AssertSyntaxError(Parse("* =>[KNN 5 @field not_param]"));
  AssertSyntaxError(Parse("KNN 5 @vector $BLOB", {{"BLOB", vec_str}}));
  AssertSyntaxError(Parse("[KNN 5 @vector $BLOB]", {{"BLOB", vec_str}}));
  AssertSyntaxError(Parse("KNN 5 @vector $BLOB", {{"BLOB", vec_str}}));
  AssertSyntaxError(Parse("* =>[KNN -1 @vector $BLOB]", {{"BLOB", vec_str}}));
  AssertSyntaxError(Parse("*=>[KNN 5 $vector_blob_param]", {{"vector_blob_param", vec_str}}));
  AssertSyntaxError(Parse("(*) => [KNN 10 @doc_embedding $BLOB]", {{"BLOB", vec_str}}));
  AssertSyntaxError(Parse("(@a:[1 2]) => [KNN 8 @vec_embedding $blob]", {{"blob", vec_str}}));
  AssertSyntaxError(Parse("(@a:{x|y}) => [KNN 8 @vec_embedding $blob]", {{"blob", vec_str}}));
  AssertSyntaxError(Parse("(@a:{x|y}) => [KNN 8 @vec_embedding $blob]", {{"blob", vec_str}}));
  AssertSyntaxError(Parse("(@a:{x}|@b:[1 inf] | @c:{y}) => [KNN 8 @vec_embedding $blob]", {{"blob", vec_str}}));
  AssertSyntaxError(Parse("(@a:{x}|@b:[1 inf] | @field:[VECTOR_RANGE 10 $vector]) => [KNN 8 @vec_embedding $blob]",
                          {{"blob", vec_str}, {"vector", vec_str}}));

  AssertIR(Parse("@field:[VECTOR_RANGE 10 $vector]", {{"vector", vec_str}}),
           "field <-> [1.000000, 2.000000, 3.000000] < 10");
  AssertIR(Parse("@field:[VECTOR_RANGE 10 $vector]| @b:[1 inf]", {{"vector", vec_str}}),
           "(or field <-> [1.000000, 2.000000, 3.000000] < 10, b >= 1)");
  AssertIR(Parse("*=>[KNN 10 @doc_embedding $BLOB]", {{"BLOB", vec_str}}),
           "KNN k=10, doc_embedding <-> [1.000000, 2.000000, 3.000000]");
  AssertIR(Parse("* =>[KNN 5 @vector $BLOB]", {{"BLOB", vec_str}}),
           "KNN k=5, vector <-> [1.000000, 2.000000, 3.000000]");

  vec_str = vec_str.substr(0, 3);
  ASSERT_EQ(Parse("@field:[VECTOR_RANGE 10 $vector]", {{"vector", vec_str}}).Msg(),
            "data size is not a multiple of the target type size");
  vec_str = "";
  ASSERT_EQ(Parse("@field:[VECTOR_RANGE 10 $vector]", {{"vector", vec_str}}).Msg(), "empty vector is invalid");
}

TEST(RedisQueryParserTest, MaxNestingDepth) {
  auto make_not_query = [](size_t depth) { return std::string(depth, '-') + "@a:[1 2]"; };
  auto make_paren_query = [](size_t depth) { return std::string(depth, '(') + "@a:[1 2]" + std::string(depth, ')'); };

  // Expected limits follow kqir::kMaxQueryNestingDepth (issue 3810): up to 100 nested
  // NOT/parenthesis levels are accepted; 101 levels are rejected with a depth error.
  ASSERT_EQ(Parse(make_not_query(kqir::kMaxQueryNestingDepth)).Msg(), Status::ok_msg);
  ASSERT_EQ(Parse(make_not_query(kqir::kMaxQueryNestingDepth + 1)).Msg(), "maximum query nesting depth exceeded");
  ASSERT_EQ(Parse(make_not_query(20000)).Msg(), "maximum query nesting depth exceeded");

  ASSERT_EQ(Parse(make_paren_query(kqir::kMaxQueryNestingDepth)).Msg(), Status::ok_msg);
  ASSERT_EQ(Parse(make_paren_query(kqir::kMaxQueryNestingDepth + 1)).Msg(), "maximum query nesting depth exceeded");
  ASSERT_EQ(Parse(make_paren_query(20000)).Msg(), "maximum query nesting depth exceeded");

  // Mixed NOT and parenthesized expressions share the same nesting budget.
  std::string mixed_ok;
  for (size_t i = 0; i < kqir::kMaxQueryNestingDepth / 2; ++i) mixed_ok += "-(";
  mixed_ok += "@a:[1 2]";
  for (size_t i = 0; i < kqir::kMaxQueryNestingDepth / 2; ++i) mixed_ok += ")";
  ASSERT_EQ(Parse(mixed_ok).Msg(), Status::ok_msg);
  ASSERT_EQ(Parse("-" + mixed_ok).Msg(), "maximum query nesting depth exceeded");

  // Deepest tree within kMaxQueryNestingDepth: a top-level OR/AND plus 100
  // parenthesized groups, i.e. A | A (A | A (... (A | A A))) with A = @a:[1 2].
  // Alternating OR/AND at successive levels is not the worst case.
  const std::string A = "@a:[1 2]";
  std::string worst = "(" + A + " | " + A + " " + A + ")";
  for (size_t i = 1; i < kqir::kMaxQueryNestingDepth; ++i) {
    worst = "(" + A + " | " + A + " " + worst + ")";
  }
  worst = A + " | " + A + " " + worst;
  ASSERT_EQ(Parse(worst).Msg(), Status::ok_msg);

  // Flat wide queries reset nesting depth across siblings and must still succeed.
  std::string wide_query = "@a:[1 2]";
  for (size_t i = 0; i < 500; ++i) {
    wide_query += " | @a:[1 2]";
  }
  ASSERT_EQ(Parse(wide_query).Msg(), Status::ok_msg);

  // Direct Transformer::Transform depth guard: kMaxIRTransformDepth-1 NOT nodes
  // are accepted; kMaxIRTransformDepth NOT nodes are rejected.
  auto make_not_tree = [](size_t not_count) {
    auto leaf = std::make_unique<kqir::peg::parse_tree::node>();
    leaf->set_type<Wildcard>();
    auto head = std::move(leaf);
    for (size_t i = 0; i < not_count; ++i) {
      auto not_node = std::make_unique<kqir::peg::parse_tree::node>();
      not_node->set_type<NotExpr>();
      not_node->children.push_back(std::move(head));
      head = std::move(not_node);
    }
    auto root = std::make_unique<kqir::peg::parse_tree::node>();
    root->children.push_back(std::move(head));
    return root;
  };
  ASSERT_EQ(Transformer({}).Transform(make_not_tree(kqir::kMaxIRTransformDepth - 1)).Msg(), Status::ok_msg);
  ASSERT_EQ(Transformer({}).Transform(make_not_tree(kqir::kMaxIRTransformDepth)).Msg(),
            "maximum query nesting depth exceeded");
}
