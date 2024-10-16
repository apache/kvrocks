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

#include "common/glob.h"

TEST(GlobUtil, GlobMatches) {
  /* Some basic tests */
  EXPECT_TRUE(util::GlobMatches("a", "a"));
  EXPECT_FALSE(util::GlobMatches("a", "b"));
  EXPECT_FALSE(util::GlobMatches("a", "aa"));
  EXPECT_FALSE(util::GlobMatches("a", ""));
  EXPECT_TRUE(util::GlobMatches("", ""));
  EXPECT_FALSE(util::GlobMatches("", "a"));

  /* Simple character class tests */
  EXPECT_TRUE(util::GlobMatches("[a]", "a"));
  EXPECT_FALSE(util::GlobMatches("[a]", "b"));
  EXPECT_FALSE(util::GlobMatches("[^a]", "a"));
  EXPECT_TRUE(util::GlobMatches("[^a]", "b"));
  EXPECT_TRUE(util::GlobMatches("[ab]", "a"));
  EXPECT_TRUE(util::GlobMatches("[ab]", "b"));
  EXPECT_FALSE(util::GlobMatches("[ab]", "c"));
  EXPECT_TRUE(util::GlobMatches("[^ab]", "c"));
  EXPECT_TRUE(util::GlobMatches("[a-c]", "b"));
  EXPECT_FALSE(util::GlobMatches("[a-c]", "d"));

  /* Corner cases in character class parsing */
  EXPECT_TRUE(util::GlobMatches("[a-c-e-g]", "-"));
  EXPECT_FALSE(util::GlobMatches("[a-c-e-g]", "d"));
  EXPECT_TRUE(util::GlobMatches("[a-c-e-g]", "f"));

  /* Escaping */
  EXPECT_TRUE(util::GlobMatches("\\?", "?"));
  EXPECT_FALSE(util::GlobMatches("\\?", "a"));
  EXPECT_TRUE(util::GlobMatches("\\*", "*"));
  EXPECT_FALSE(util::GlobMatches("\\*", "a"));
  EXPECT_TRUE(util::GlobMatches("\\[", "["));
  EXPECT_TRUE(util::GlobMatches("\\]", "]"));
  EXPECT_TRUE(util::GlobMatches("\\\\", "\\"));
  EXPECT_TRUE(util::GlobMatches("[\\.]", "."));
  EXPECT_TRUE(util::GlobMatches("[\\-]", "-"));
  EXPECT_TRUE(util::GlobMatches("[\\[]", "["));
  EXPECT_TRUE(util::GlobMatches("[\\]]", "]"));
  EXPECT_TRUE(util::GlobMatches("[\\\\]", "\\"));
  EXPECT_TRUE(util::GlobMatches("[\\?]", "?"));
  EXPECT_TRUE(util::GlobMatches("[\\*]", "*"));

  /* Simple wild cards */
  EXPECT_TRUE(util::GlobMatches("?", "a"));
  EXPECT_FALSE(util::GlobMatches("?", "aa"));
  EXPECT_FALSE(util::GlobMatches("??", "a"));
  EXPECT_TRUE(util::GlobMatches("?x?", "axb"));
  EXPECT_FALSE(util::GlobMatches("?x?", "abx"));
  EXPECT_FALSE(util::GlobMatches("?x?", "xab"));

  /* Asterisk wild cards (backtracking) */
  EXPECT_FALSE(util::GlobMatches("*??", "a"));
  EXPECT_TRUE(util::GlobMatches("*??", "ab"));
  EXPECT_TRUE(util::GlobMatches("*??", "abc"));
  EXPECT_TRUE(util::GlobMatches("*??", "abcd"));
  EXPECT_FALSE(util::GlobMatches("??*", "a"));
  EXPECT_TRUE(util::GlobMatches("??*", "ab"));
  EXPECT_TRUE(util::GlobMatches("??*", "abc"));
  EXPECT_TRUE(util::GlobMatches("??*", "abcd"));
  EXPECT_FALSE(util::GlobMatches("?*?", "a"));
  EXPECT_TRUE(util::GlobMatches("?*?", "ab"));
  EXPECT_TRUE(util::GlobMatches("?*?", "abc"));
  EXPECT_TRUE(util::GlobMatches("?*?", "abcd"));
  EXPECT_TRUE(util::GlobMatches("*b", "b"));
  EXPECT_TRUE(util::GlobMatches("*b", "ab"));
  EXPECT_FALSE(util::GlobMatches("*b", "ba"));
  EXPECT_TRUE(util::GlobMatches("*b", "bb"));
  EXPECT_TRUE(util::GlobMatches("*b", "abb"));
  EXPECT_TRUE(util::GlobMatches("*b", "bab"));
  EXPECT_TRUE(util::GlobMatches("*bc", "abbc"));
  EXPECT_TRUE(util::GlobMatches("*bc", "bc"));
  EXPECT_TRUE(util::GlobMatches("*bc", "bbc"));
  EXPECT_TRUE(util::GlobMatches("*bc", "bcbc"));

  /* Multiple asterisks (complex backtracking) */
  EXPECT_TRUE(util::GlobMatches("*ac*", "abacadaeafag"));
  EXPECT_TRUE(util::GlobMatches("*ac*ae*ag*", "abacadaeafag"));
  EXPECT_TRUE(util::GlobMatches("*a*b*[bc]*[ef]*g*", "abacadaeafag"));
  EXPECT_FALSE(util::GlobMatches("*a*b*[ef]*[cd]*g*", "abacadaeafag"));
  EXPECT_TRUE(util::GlobMatches("*abcd*", "abcabcabcabcdefg"));
  EXPECT_TRUE(util::GlobMatches("*ab*cd*", "abcabcabcabcdefg"));
  EXPECT_TRUE(util::GlobMatches("*abcd*abcdef*", "abcabcdabcdeabcdefg"));
  EXPECT_FALSE(util::GlobMatches("*abcd*", "abcabcabcabcefg"));
  EXPECT_FALSE(util::GlobMatches("*ab*cd*", "abcabcabcabcefg"));

  /* Robustness to exponential blow-ups with lots of non-collapsible asterisks */
  EXPECT_TRUE(
      util::GlobMatches("?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
  EXPECT_FALSE(
      util::GlobMatches("?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*?*b", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
}

TEST(GlobUtil, SplitGlob) {
    using namespace std::string_literals;
    
    // Basic functionality: no escaped characters
    EXPECT_EQ(util::SplitGlob("string"), std::make_pair("string"s, ""s));
    EXPECT_EQ(util::SplitGlob("string*"), std::make_pair("string"s, "*"s));
    EXPECT_EQ(util::SplitGlob("*string"), std::make_pair(""s, "*string"s));
    EXPECT_EQ(util::SplitGlob("str*ing"), std::make_pair("str"s, "*ing"s));
    EXPECT_EQ(util::SplitGlob("string?"), std::make_pair("string"s, "?"s));
    EXPECT_EQ(util::SplitGlob("?string"), std::make_pair(""s, "?string"s));
    EXPECT_EQ(util::SplitGlob("ab[cd]ef"), std::make_pair("ab"s, "[cd]ef"s));

    // Escaped characters; also tests that prefix is trimmed of backslashes
    EXPECT_EQ(util::SplitGlob("str\\*ing*"), std::make_pair("str*ing"s, "*"s));
    EXPECT_EQ(util::SplitGlob("str\\?ing?"), std::make_pair("str?ing"s, "?"s));
    EXPECT_EQ(util::SplitGlob("str\\[ing[a]"), std::make_pair("str[ing"s, "[a]"s));
}