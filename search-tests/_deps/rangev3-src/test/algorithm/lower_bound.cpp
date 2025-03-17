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

#include <utility>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/lower_bound.hpp>
#include "../simple_test.hpp"
#include "../test_iterators.hpp"
#include "../test_utils.hpp"

struct my_int
{
    int value;
};

bool compare(my_int lhs, my_int rhs)
{
    return lhs.value < rhs.value;
}

void not_totally_ordered()
{
    // This better compile!
    std::vector<my_int> vec;
    ranges::lower_bound(vec, my_int{10}, compare);
}

int main()
{
    using ranges::begin;
    using ranges::end;
    using ranges::size;
    using ranges::less;

    constexpr std::pair<int, int> a[] = {{0, 0}, {0, 1}, {1, 2}, {1, 3}, {3, 4}, {3, 5}};
    constexpr const std::pair<int, int> c[] = {
        {0, 0}, {0, 1}, {1, 2}, {1, 3}, {3, 4}, {3, 5}};

    CHECK(ranges::aux::lower_bound_n(begin(a), size(a), a[0]) == &a[0]);
    CHECK(ranges::aux::lower_bound_n(begin(a), size(a), a[1], less()) == &a[1]);
    CHECK(ranges::aux::lower_bound_n(begin(a), size(a), 1, less(), &std::pair<int, int>::first) == &a[2]);

    CHECK(ranges::lower_bound(begin(a), end(a), a[0]) == &a[0]);
    CHECK(ranges::lower_bound(begin(a), end(a), a[1], less()) == &a[1]);
    CHECK(ranges::lower_bound(begin(a), end(a), 1, less(), &std::pair<int, int>::first) == &a[2]);

    CHECK(ranges::lower_bound(a, a[2]) == &a[2]);
    CHECK(ranges::lower_bound(c, c[3]) == &c[3]);

    CHECK(ranges::lower_bound(a, a[4], less()) == &a[4]);
    CHECK(ranges::lower_bound(c, c[5], less()) == &c[5]);

    CHECK(ranges::lower_bound(a, 1, less(), &std::pair<int, int>::first) == &a[2]);
    CHECK(ranges::lower_bound(c, 1, less(), &std::pair<int, int>::first) == &c[2]);

    CHECK(ranges::lower_bound(ranges::views::all(a), 1, less(), &std::pair<int, int>::first) == &a[2]);
    CHECK(ranges::lower_bound(ranges::views::all(c), 1, less(), &std::pair<int, int>::first) == &c[2]);
#ifndef RANGES_WORKAROUND_MSVC_573728
    CHECK(::is_dangling(ranges::lower_bound(std::move(a), 1, less(), &std::pair<int, int>::first)));
    CHECK(::is_dangling(ranges::lower_bound(std::move(c), 1, less(), &std::pair<int, int>::first)));
#endif // RANGES_WORKAROUND_MSVC_573728
    {
        std::vector<std::pair<int, int>> vec_a(ranges::begin(a), ranges::end(a));
        CHECK(::is_dangling(ranges::lower_bound(std::move(vec_a), 1, less(), &std::pair<int, int>::first)));
    }
    {
        std::vector<std::pair<int, int>> const vec_c(ranges::begin(c), ranges::end(c));
        CHECK(::is_dangling(ranges::lower_bound(std::move(vec_c), 1, less(), &std::pair<int, int>::first)));
    }

    {
        using namespace ranges;

        STATIC_CHECK(aux::lower_bound_n(begin(a), size(a), a[0]) == &a[0]);
        STATIC_CHECK(aux::lower_bound_n(begin(a), size(a), a[1], less()) == &a[1]);

        STATIC_CHECK(lower_bound(begin(a), end(a), a[0]) == &a[0]);
        STATIC_CHECK(lower_bound(begin(a), end(a), a[1], less()) == &a[1]);
        STATIC_CHECK(lower_bound(a, a[2]) == &a[2]);
        STATIC_CHECK(lower_bound(a, a[4], less()) == &a[4]);
        STATIC_CHECK(lower_bound(a, std::make_pair(1, 2), less()) == &a[2]);
#if RANGES_CXX_CONSTEXPR >= RANGES_CXX_CONSTEXPR_17
        // requires constexpr std::addressof
        STATIC_CHECK(lower_bound(views::all(a), std::make_pair(1, 2), less()) == &a[2]);
#endif
    }

    return test_result();
}
