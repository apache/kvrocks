// Range v3 library
//
//  Copyright Andrew Sutton 2014
//  Copyright Gonzalo Brito Gadeschi 2014
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/none_of.hpp>
#include "../simple_test.hpp"

constexpr bool even(int n) { return n % 2 == 0; }

struct S {
  S(bool p) : test(p) { }

  bool p() const { return test; }

  bool test;
};

constexpr bool test_constexpr(std::initializer_list<int> il)
{
    return ranges::none_of(il, even);
}

int main()
{
  std::vector<int> all_even { 0, 2, 4, 6 };
  std::vector<int> one_even { 1, 3, 4, 7 };
  std::vector<int> none_even { 1, 3, 5, 7 };
  CHECK(!ranges::none_of(all_even.begin(), all_even.end(), even));
  CHECK(!ranges::none_of(one_even.begin(), one_even.end(), even));
  CHECK(ranges::none_of(none_even.begin(), none_even.end(), even));

  CHECK(!ranges::none_of(all_even, even));
  CHECK(!ranges::none_of(one_even, even));
  CHECK(ranges::none_of(none_even, even));

  using ILI = std::initializer_list<int>;
  CHECK(!ranges::none_of(ILI{0, 2, 4, 6}, [](int n) { return n % 2 == 0; }));
  CHECK(!ranges::none_of(ILI{1, 3, 4, 7}, [](int n) { return n % 2 == 0; }));
  CHECK(ranges::none_of(ILI{1, 3, 5, 7}, [](int n) { return n % 2 == 0; }));

  std::vector<S> all_true { true, true, true };
  std::vector<S> one_true { false, false, true };
  std::vector<S> none_true { false, false, false };
  CHECK(!ranges::none_of(all_true.begin(), all_true.end(), &S::p));
  CHECK(!ranges::none_of(one_true.begin(), one_true.end(), &S::p));
  CHECK(ranges::none_of(none_true.begin(), none_true.end(), &S::p));

  CHECK(!ranges::none_of(all_true, &S::p));
  CHECK(!ranges::none_of(one_true, &S::p));
  CHECK(ranges::none_of(none_true, &S::p));

  using ILS = std::initializer_list<S>;
  CHECK(!ranges::none_of(ILS{S(true), S(true), S(true)}, &S::p));
  CHECK(!ranges::none_of(ILS{S(false), S(true), S(false)}, &S::p));
  CHECK(ranges::none_of(ILS{S(false), S(false), S(false)}, &S::p));

  STATIC_CHECK(!test_constexpr({0, 2, 4, 6}));
  STATIC_CHECK(!test_constexpr({1, 3, 4, 7}));
  STATIC_CHECK(test_constexpr({1, 3, 5, 7}));

  return ::test_result();
}
