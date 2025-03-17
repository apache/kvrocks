// Range v3 library
//
//  Copyright Eric Niebler 2014-present
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
#include <range/v3/core.hpp>
#include <range/v3/algorithm/fill.hpp>
#include <range/v3/algorithm/equal.hpp>

#include "../array.hpp"
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

template<class Iter, class Sent = Iter>
void
test_char()
{
    const unsigned n = 4;
    char ca[n] = {0};
    auto i = ranges::fill(Iter(ca), Sent(ca+n), char(1));
    CHECK(ca[0] == 1);
    CHECK(ca[1] == 1);
    CHECK(ca[2] == 1);
    CHECK(ca[3] == 1);
    CHECK(i == Iter(ca + 4));

    auto rng = ranges::make_subrange(Iter(ca), Sent(ca+n));
    i = ranges::fill(rng, char(2));
    CHECK(ca[0] == 2);
    CHECK(ca[1] == 2);
    CHECK(ca[2] == 2);
    CHECK(ca[3] == 2);
    CHECK(i == Iter(ca + 4));

    auto j = ranges::fill(::MakeTestRange(Iter(ca), Sent(ca+n)), char(3));
    CHECK(ca[0] == 3);
    CHECK(ca[1] == 3);
    CHECK(ca[2] == 3);
    CHECK(ca[3] == 3);
    CHECK(::is_dangling(j));
}

template<class Iter, class Sent = Iter>
void
test_int()
{
    const unsigned n = 4;
    int ia[n] = {0};
    ranges::fill(Iter(ia), Sent(ia+n), 1);
    CHECK(ia[0] == 1);
    CHECK(ia[1] == 1);
    CHECK(ia[2] == 1);
    CHECK(ia[3] == 1);

    auto rng = ranges::make_subrange(Iter(ia), Sent(ia+n));
    ranges::fill(rng, 2);
    CHECK(ia[0] == 2);
    CHECK(ia[2] == 2);
    CHECK(ia[2] == 2);
    CHECK(ia[3] == 2);
}

constexpr auto fives()
{
    test::array<int, 4> a{{0}};
    ranges::fill(a, 5);
    return a;
}

constexpr auto fives(int n)
{
    test::array<int, 4> a{{0}};
    ranges::fill_n(ranges::begin(a), n, 5);
    return a;
}

int main()
{
    test_char<ForwardIterator<char*> >();
    test_char<BidirectionalIterator<char*> >();
    test_char<RandomAccessIterator<char*> >();
    test_char<char*>();

    test_char<ForwardIterator<char*>, Sentinel<char*> >();
    test_char<BidirectionalIterator<char*>, Sentinel<char*> >();
    test_char<RandomAccessIterator<char*>, Sentinel<char*> >();

    test_int<ForwardIterator<int*> >();
    test_int<BidirectionalIterator<int*> >();
    test_int<RandomAccessIterator<int*> >();
    test_int<int*>();

    test_int<ForwardIterator<int*>, Sentinel<int*> >();
    test_int<BidirectionalIterator<int*>, Sentinel<int*> >();
    test_int<RandomAccessIterator<int*>, Sentinel<int*> >();

    {
        using IL = std::initializer_list<int>;
        STATIC_CHECK(ranges::equal(fives(), IL{5, 5, 5, 5}));
        STATIC_CHECK(ranges::equal(fives(2), IL{5, 5, 0, 0}));
        STATIC_CHECK(!ranges::equal(fives(2), IL{5, 5, 5, 5}));
    }

    return ::test_result();
}
