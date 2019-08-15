#include <gtest/gtest.h>
#include <map>
#include "util.h"

TEST(StringUtil, ToLower) {
  std::map<std::string, std::string> cases {
          {"ABC", "abc"},
          {"AbC", "abc"},
          {"abc", "abc"},
  };
  for (auto iter = cases.begin(); iter != cases.end(); iter++) {
    std::string input = iter->first;
    input = Util::ToLower(input);
    ASSERT_EQ(input, iter->second);
  }
}

TEST(StringUtil, Trim) {
  std::map<std::string, std::string> cases {
          {"abc", "abc"},
          {"   abc    ", "abc"},
          {"\t\tabc\t\t", "abc"},
          {"\t\tabc\n\n", "abc"},
          {"\n\nabc\n\n", "abc"},
  };
  for (auto iter = cases.begin(); iter != cases.end(); iter++) {
    std::string input = iter->first;
    std::string output;
    Util::Trim(input, " \t\n", &output);
    ASSERT_EQ(output, iter->second);
  }
}

TEST(StringUtil, Split) {
  std::vector<std::string> array;
  std::vector<std::string> expected = {"a", "b", "c", "d"};
  Util::Split("a,b,c,d", ",", &array);
  ASSERT_EQ(expected, array);
  Util::Split("a,b,,c,d,", ",", &array);
  ASSERT_EQ(expected, array);
  Util::Split(",a,b,c,d,", ",", &array);
  ASSERT_EQ(expected, array);
  Util::Split("a     b  c  d   ", " ", &array);
  ASSERT_EQ(expected, array);
  Util::Split("a\tb\nc\t\nd   ", " \t\n", &array);
  ASSERT_EQ(expected, array);
}