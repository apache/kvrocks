// Range v3 library
//
//  Copyright Johel Guerrero 2019
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#include <range/v3/algorithm/contains.hpp>

#include "../simple_test.hpp"

int main()
{
    using ranges::contains;

    constexpr int rng[] = {4, 2};
    const auto first = rng;
    const auto last = rng + 2;

    CHECK(!contains(first, first, 0));
    CHECK(!contains(first, last, 1));
    CHECK(contains(first, last, 2));
    CHECK(!contains(first, last, 3));
    CHECK(contains(first, last, 4));

#ifndef RANGES_WORKAROUND_CLANG_23135
    static_assert(!contains(rng, 1), "");
    static_assert(contains(rng, 2), "");
    static_assert(!contains(rng, 3), "");
    static_assert(contains(rng, 4), "");
#endif

    return ::test_result();
}
