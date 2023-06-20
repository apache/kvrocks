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

#include "commands/command_parser.h"

#include <gtest/gtest.h>

TEST(CommandParser, Parse) {
  // [ HELLO i1 v1 | HI v2 ] [X i2 | Y]
  std::vector<std::string> c1{"hello", "1", "a"}, c2{"hi", "b"}, c3{"hi", "c", "x", "2"}, c4{"hello", "3", "d", "y"},
      c5{"hi", "e", "y"}, c6{"y"}, d1{"hello"}, d2{"hi"}, d3{"hello", "no-int"}, d4{"x", "1", "y"},
      d5{"hello", "1", "v", "hi", "v"}, d6{"hello", "1"};

  std::int64_t i1 = 0, i2 = 0;
  std::string v1, v2;
  std::string_view hflag, xflag;

  auto parse = [&](auto&& parser) -> Status {
    hflag = {}, xflag = {};
    while (parser.Good()) {
      if (parser.EatEqICaseFlag("hello", hflag)) {
        i1 = GET_OR_RET(parser.TakeInt());
        v1 = GET_OR_RET(parser.TakeStr());
      } else if (parser.EatEqICaseFlag("hi", hflag)) {
        v2 = GET_OR_RET(parser.TakeStr());
      } else if (parser.EatEqICaseFlag("x", xflag)) {
        i2 = GET_OR_RET(parser.TakeInt());
      } else if (parser.EatEqICaseFlag("y", xflag)) {
        // pass
      } else {
        return parser.InvalidSyntax();
      }
    }

    return {};
  };

  ASSERT_TRUE(parse(CommandParser(c1)));
  ASSERT_EQ(i1, 1);
  ASSERT_EQ(v1, "a");
  ASSERT_TRUE(parse(CommandParser(c2)));
  ASSERT_EQ(v2, "b");
  ASSERT_TRUE(parse(CommandParser(c3)));
  ASSERT_EQ(v2, "c");
  ASSERT_EQ(i2, 2);
  ASSERT_TRUE(parse(CommandParser(c4)));
  ASSERT_EQ(i1, 3);
  ASSERT_EQ(v1, "d");
  ASSERT_EQ(xflag, "y");
  ASSERT_TRUE(parse(CommandParser(c5)));
  ASSERT_EQ(v2, "e");
  ASSERT_EQ(xflag, "y");
  ASSERT_TRUE(parse(CommandParser(c6)));
  ASSERT_EQ(hflag, "");
  ASSERT_EQ(xflag, "y");
  ASSERT_FALSE(parse(CommandParser(d1)));
  ASSERT_FALSE(parse(CommandParser(d2)));
  ASSERT_FALSE(parse(CommandParser(d3)));
  ASSERT_FALSE(parse(CommandParser(d4)));
  ASSERT_FALSE(parse(CommandParser(d5)));
  ASSERT_FALSE(parse(CommandParser(d6)));
}

TEST(CommandParser, ParseMove) {
  // k v [ HELLO a b | HI c ]
  std::vector<std::string> c1{"h", "i"}, c2{"g", "k", "HELLO", "l", "m"}, c3{"n", "o", "HI", "p"}, d1{"x"},
      d2{"x", "y", "HAHA"}, d3{};

  std::string k, v, a, b, c;

  auto parse = [&](auto&& parser) -> Status {
    k = GET_OR_RET(parser.TakeStr());
    v = GET_OR_RET(parser.TakeStr());
    if (parser.EatEqICase("HELLO")) {
      a = GET_OR_RET(parser.TakeStr());
      b = GET_OR_RET(parser.TakeStr());
    } else if (parser.EatEqICase("HI")) {
      c = GET_OR_RET(parser.TakeStr());
    } else if (parser.Good()) {
      return parser.InvalidSyntax();
    }

    return Status::OK();
  };

  ASSERT_TRUE(parse(CommandParser(std::move(c1))));
  ASSERT_EQ(k, "h");
  ASSERT_EQ(v, "i");
  ASSERT_TRUE(parse(CommandParser(std::move(c2))));
  ASSERT_EQ(k, "g");
  ASSERT_EQ(v, "k");
  ASSERT_EQ(a, "l");
  ASSERT_EQ(b, "m");
  ASSERT_TRUE(parse(CommandParser(std::move(c3))));
  ASSERT_EQ(k, "n");
  ASSERT_EQ(v, "o");
  ASSERT_EQ(c, "p");
  ASSERT_FALSE(parse(CommandParser(std::move(d1))));
  ASSERT_FALSE(parse(CommandParser(std::move(d2))));
  ASSERT_FALSE(parse(CommandParser(std::move(d3))));
}

TEST(CommandParser, ParseFloat) {
  std::vector<std::string> vec{"not float", "-3.5e-6", "3"};

  CommandParser p(vec);

  ASSERT_FALSE(p.TakeFloat());
  auto _ = p.TakeStr();
  ASSERT_EQ(p.TakeFloat().GetValue(), -3.5e-6);
  ASSERT_EQ(p.TakeFloat().GetValue(), 3);
  ASSERT_FALSE(p.TakeFloat());
}
