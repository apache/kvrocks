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

#include <range/v3/core.hpp>
#include <range/v3/algorithm/adjacent_find.hpp>

#include "../array.hpp"
#include "../simple_test.hpp"

int main()
{
    int v1[] = { 0, 2, 2, 4, 6 };
    CHECK(ranges::adjacent_find(ranges::begin(v1), ranges::end(v1)) == &v1[1]);
    CHECK(ranges::adjacent_find(v1) == &v1[1]);

    std::pair<int, int> v2[] = {{0, 0}, {0, 2}, {0, 2}, {0, 4}, {0, 6}};
    CHECK(ranges::adjacent_find(ranges::begin(v2), ranges::end(v2),
        ranges::equal_to{}, &std::pair<int, int>::second) == &v2[1]);
    CHECK(ranges::adjacent_find(v2, ranges::equal_to{}, &std::pair<int, int>::second) == &v2[1]);
    static_assert(std::is_same<std::pair<int,int>*,
                               decltype(ranges::adjacent_find(v2, ranges::equal_to{},
                                    &std::pair<int, int>::second))>::value, "");

    {
        using namespace ranges;
        constexpr auto a1 = test::array<int, 5>{{0, 2, 2, 4, 6}};
        STATIC_CHECK(adjacent_find(begin(a1), end(a1)) == (begin(a1) + 1));
        STATIC_CHECK(adjacent_find(a1) == (begin(a1) + 1));
        constexpr std::pair<int, int> a2[] = {{0, 0}, {0, 2}, {0, 2}, {0, 4}, {0, 6}};
        STATIC_CHECK(adjacent_find(a2, ranges::equal_to{}) == (begin(a2) + 1));
    }
    return test_result();
}
