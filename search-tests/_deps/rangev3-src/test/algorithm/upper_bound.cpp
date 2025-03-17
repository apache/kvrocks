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
#include <range/v3/algorithm/upper_bound.hpp>
#include "../simple_test.hpp"
#include "../test_iterators.hpp"

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
    ranges::upper_bound(vec, my_int{10}, compare);
}

int main()
{
    using ranges::begin;
    using ranges::end;
    using ranges::size;
    using ranges::less;

    using P = std::pair<int, int>;

    constexpr P a[] = {{0, 0}, {0, 1}, {1, 2}, {1, 3}, {3, 4}, {3, 5}};
    constexpr P const c[] = {{0, 0}, {0, 1}, {1, 2}, {1, 3}, {3, 4}, {3, 5}};

    CHECK(ranges::aux::upper_bound_n(begin(a), size(a), a[0]) == &a[1]);
    CHECK(ranges::aux::upper_bound_n(begin(a), size(a), a[1], less()) == &a[2]);
    CHECK(ranges::aux::upper_bound_n(begin(a), size(a), 1, less(), &std::pair<int, int>::first) == &a[4]);

    CHECK(ranges::upper_bound(begin(a), end(a), a[0]) == &a[1]);
    CHECK(ranges::upper_bound(begin(a), end(a), a[1], less()) == &a[2]);
    CHECK(ranges::upper_bound(begin(a), end(a), 1, less(), &std::pair<int, int>::first) == &a[4]);

    CHECK(ranges::upper_bound(a, a[2]) == &a[3]);
    CHECK(ranges::upper_bound(c, c[3]) == &c[4]);

    CHECK(ranges::upper_bound(a, a[4], less()) == &a[5]);
    CHECK(ranges::upper_bound(c, c[5], less()) == &c[6]);

    CHECK(ranges::upper_bound(a, 1, less(), &std::pair<int, int>::first) == &a[4]);
    CHECK(ranges::upper_bound(c, 1, less(), &std::pair<int, int>::first) == &c[4]);

    std::vector<P> vec_a(ranges::begin(a), ranges::end(a));
    std::vector<P> const vec_c(ranges::begin(c), ranges::end(c));

    CHECK(ranges::upper_bound(ranges::views::all(a), a[2]) == &a[3]);
    CHECK(ranges::upper_bound(ranges::views::all(c), c[3]) == &c[4]);
#ifndef RANGES_WORKAROUND_MSVC_573728
    CHECK(::is_dangling(ranges::upper_bound(std::move(a), a[2])));
    CHECK(::is_dangling(ranges::upper_bound(std::move(c), c[3])));
#endif // RANGES_WORKAROUND_MSVC_573728
    CHECK(::is_dangling(ranges::upper_bound(std::move(vec_a), vec_a[2])));
    CHECK(::is_dangling(ranges::upper_bound(std::move(vec_c), vec_c[3])));

    CHECK(ranges::upper_bound(ranges::views::all(a), a[4], less()) == &a[5]);
    CHECK(ranges::upper_bound(ranges::views::all(c), c[5], less()) == &c[6]);
#ifndef RANGES_WORKAROUND_MSVC_573728
    CHECK(::is_dangling(ranges::upper_bound(std::move(a), a[4], less())));
    CHECK(::is_dangling(ranges::upper_bound(std::move(c), c[5], less())));
#endif // RANGES_WORKAROUND_MSVC_573728
    CHECK(::is_dangling(ranges::upper_bound(std::move(vec_a), vec_a[4], less())));
    CHECK(::is_dangling(ranges::upper_bound(std::move(vec_c), vec_c[5], less())));

    CHECK(ranges::upper_bound(ranges::views::all(a), 1, less(), &std::pair<int, int>::first) == &a[4]);
    CHECK(ranges::upper_bound(ranges::views::all(c), 1, less(), &std::pair<int, int>::first) == &c[4]);
#ifndef RANGES_WORKAROUND_MSVC_573728
    CHECK(::is_dangling(ranges::upper_bound(std::move(a), 1, less(), &std::pair<int, int>::first)));
    CHECK(::is_dangling(ranges::upper_bound(std::move(c), 1, less(), &std::pair<int, int>::first)));
#endif // RANGES_WORKAROUND_MSVC_573728
    CHECK(::is_dangling(ranges::upper_bound(std::move(vec_a), 1, less(), &std::pair<int, int>::first)));
    CHECK(::is_dangling(ranges::upper_bound(std::move(vec_c), 1, less(), &std::pair<int, int>::first)));

    {
        using namespace ranges;

        STATIC_CHECK(aux::upper_bound_n(begin(a), size(a), a[0]) == &a[1]);
        STATIC_CHECK(aux::upper_bound_n(begin(a), size(a), a[1], less()) == &a[2]);

        STATIC_CHECK(upper_bound(begin(a), end(a), a[0]) == &a[1]);
        STATIC_CHECK(upper_bound(begin(a), end(a), a[1], less()) == &a[2]);
        STATIC_CHECK(upper_bound(a, a[2]) == &a[3]);
        STATIC_CHECK(upper_bound(a, a[3], less()) == &a[4]);

        STATIC_CHECK(upper_bound(a, std::make_pair(1, 3), less()) == &a[4]);
#if RANGES_CXX_CONSTEXPR >= RANGES_CXX_CONSTEXPR_17
        // requires constexpr std::addressof
        STATIC_CHECK(upper_bound(views::all(a), std::make_pair(1, 3), less()) == &a[4]);
#endif
    }

    return test_result();
}
