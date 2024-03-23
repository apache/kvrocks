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

#include "tao/pegtl/contrib/parse_tree_to_dot.hpp"
#include "tao/pegtl/string_input.hpp"

using namespace kqir::redis_query;

static auto Parse(const std::string& in) { return ParseToTree(string_input(in, "test")); }

#define AssertSyntaxError(node) ASSERT_EQ(node.Msg(), "invalid syntax");  // NOLINT

// NOLINTNEXTLINE
#define AssertIR(node, val)              \
  ASSERT_EQ(node.Msg(), Status::ok_msg); \
  ASSERT_EQ(node.GetValue()->Dump(), val);

TEST(RedisQueryParserTest, Simple) {
    if (auto root = Parse("@a:{ hello | hi } @b:[1 2] | @c:[(3 +inf]")) {
        parse_tree::print_dot(std::cout, **root);
    }
}
