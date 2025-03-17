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
//
//  Copyright 2005 - 2007 Adobe Systems Incorporated
//  Distributed under the MIT License(see accompanying file LICENSE_1_0_0.txt
//  or a copy at http://stlab.adobe.com/licenses.html)

//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#include <array>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/equal.hpp>
#include <range/v3/algorithm/shuffle.hpp>
#include <range/v3/numeric/iota.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

int main()
{
    constexpr unsigned N = 100;
    {
        std::array<int, N> a, b, c;
        for (auto p : {&a, &b, &c})
            ranges::iota(*p, 0);
        std::minstd_rand g1, g2 = g1;
        ranges::shuffle(RandomAccessIterator<int*>(a.data()), Sentinel<int*>(a.data()+N), g1);
        CHECK(!ranges::equal(a, b));

        CHECK(ranges::shuffle(b.begin(), b.end(), g1) == b.end());
        CHECK(!ranges::equal(a, b));

        CHECK(ranges::shuffle(c.begin(), c.end(), g2) == c.end());
        CHECK(ranges::equal(a, c));
        CHECK(!ranges::equal(b, c));
    }

    {
        std::array<int, N> a, b, c;
        for (auto p : {&a, &b, &c})
            ranges::iota(*p, 0);
        std::minstd_rand g1, g2 = g1;
        auto rng = ::MakeTestRange(RandomAccessIterator<int*>(a.data()), Sentinel<int*>(a.data() + N));
        ranges::shuffle(rng, g1);
        CHECK(!ranges::equal(a, b));

        CHECK(ranges::shuffle(b, g2) == b.end());
        CHECK(ranges::equal(a, b));

        CHECK(ranges::shuffle(b, g1) == b.end());
        CHECK(!ranges::equal(a, b));
        CHECK(!ranges::equal(b, c));

        ranges::iota(a, 0);
        CHECK(::is_dangling(ranges::shuffle(std::move(rng), g1)));
        CHECK(!ranges::equal(a, c));
    }

    {
        std::array<int, N> a, b, c;
        for (auto p : {&a, &b, &c})
            ranges::iota(*p, 0);
        ranges::shuffle(RandomAccessIterator<int*>(a.data()), Sentinel<int*>(a.data() + N));
        CHECK(!ranges::equal(a, c));

        ranges::shuffle(b);
        CHECK(!ranges::equal(b, c));
        CHECK(!ranges::equal(a, b));
    }

    {
        std::array<int, N> a, b;
        for (auto p : {&a, &b})
            ranges::iota(*p, 0);
        ranges::shuffle(a);
        CHECK(!ranges::equal(a, b));
    }

    return ::test_result();
}
