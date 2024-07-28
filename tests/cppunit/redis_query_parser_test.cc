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
  AssertIR(Parse("@field:[VECTOR_RANGE 10 $vector]", {{"vector", "\x40\x59\x0b\x86\x6c\x3f\xf5\x3f"}}),
           "field vector_range 10 \"@Y\v\x86l?\xF5?\"");
  AssertIR(Parse("*=>[KNN 10 @doc_embedding $BLOB]", {{"BLOB", "\x40\x59\x0b\x86\x6c\x3f\xf5\x3f"}}),
           "doc_embedding vector_search 10 \"@Y\v\x86l?\xF5?\"");
  AssertIR(Parse("(*) => [KNN 10 @doc_embedding $BLOB]", {{"BLOB", "\x40\x59\x0b\x86\x6c\x3f\xf5\x3f"}}),
           "doc_embedding vector_search 10 \"@Y\v\x86l?\xF5?\"");
  AssertIR(Parse("(@a:[1 2]) => [KNN 8 @vec_embedding $blob]", {{"blob", "\x40\x59\x0b\x86\x6c\x3f\xf5\x3f"}}),
           "vec_embedding vector_search 8 \"@Y\v\x86l?\xF5?\"");
  AssertIR(Parse("* =>[KNN 5 @vector $BLOB]", {{"BLOB", "\x40\x59\x0b\x86\x6c\x3f\xf5\x3f"}}),
           "vector vector_search 5 \"@Y\v\x86l?\xF5?\"");
  AssertSyntaxError(
      Parse("*=>[KNN 5 $vector_blob_param]", {{"vector_blob_param", "\x40\x59\x0b\x86\x6c\x3f\xf5\x3f"}}));
}
