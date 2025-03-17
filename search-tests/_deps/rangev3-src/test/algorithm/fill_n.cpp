// Range v3 library
//
//  Copyright Eric Niebler 2014
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#include <cstring>
#include <string>
#include <vector>

#include <range/v3/algorithm/equal.hpp>
#include <range/v3/algorithm/fill.hpp>
#include <range/v3/core.hpp>

#include "../array.hpp"
#include "../simple_test.hpp"
#include "../test_iterators.hpp"
#include "../test_utils.hpp"

#ifdef RANGES_CXX_GREATER_THAN_11

RANGES_CXX14_CONSTEXPR auto fives()
{
    array<int, 4> a{{0}};
    ranges::fill(a, 5);
    return a;
}

RANGES_CXX14_CONSTEXPR auto fives(int n)
{
    array<int, 4> a{{0}};
    ranges::fill_n(ranges::begin(a), n, 5);
    return a;
}

#endif

int main()
{
    test_char<forward_iterator<char *>>();
    test_char<bidirectional_iterator<char *>>();
    test_char<random_access_iterator<char *>>();
    test_char<char *>();

    test_char<forward_iterator<char *>, sentinel<char *>>();
    test_char<bidirectional_iterator<char *>, sentinel<char *>>();
    test_char<random_access_iterator<char *>, sentinel<char *>>();

    test_int<forward_iterator<int *>>();
    test_int<bidirectional_iterator<int *>>();
    test_int<random_access_iterator<int *>>();
    test_int<int *>();

    test_int<forward_iterator<int *>, sentinel<int *>>();
    test_int<bidirectional_iterator<int *>, sentinel<int *>>();
    test_int<random_access_iterator<int *>, sentinel<int *>>();

#ifdef RANGES_CXX_GREATER_THAN_11
    {
        STATIC_CHECK(ranges::equal(fives(), {5, 5, 5, 5}));
        STATIC_CHECK(ranges::equal(fives(2), {5, 5, 0, 0}));
        STATIC_CHECK(!ranges::equal(fives(2), {5, 5, 5, 5}));
    }
#endif

    return ::test_result();
}
