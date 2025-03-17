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

#include <vector>
#include <sstream>
#include <cstring>
#include <utility>
#include <algorithm>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/copy.hpp>
#include <range/v3/algorithm/equal.hpp>
#include <range/v3/view/delimit.hpp>
#include <range/v3/iterator/stream_iterators.hpp>

#include "../array.hpp"
#include "../simple_test.hpp"
#include "../test_iterators.hpp"

#if RANGES_CXX_CONSTEXPR >= RANGES_CXX_CONSTEXPR_14 && RANGES_CONSTEXPR_INVOKE
constexpr /*c++14*/
bool test_constexpr_copy()
{
    int a[4] = {0, 0, 0, 0};
    int const b[4] = {1, 2, 3, 4};
    ranges::copy(b, a);
    return ranges::equal(b, a);
}

static_assert(test_constexpr_copy(), "");
#endif

constexpr bool test_constexpr()
{
    using IL = std::initializer_list<int>;
    constexpr test::array<int, 4> input{{0, 1, 2, 3}};
    test::array<int, 4> tmp{{0, 0, 0, 0}};
    
    auto res = ranges::copy(input, ranges::begin(tmp));
    
    STATIC_CHECK_RETURN(res.in == ranges::end(input));
    STATIC_CHECK_RETURN(res.out == ranges::end(tmp));
    STATIC_CHECK_RETURN(ranges::equal(tmp, IL{0, 1, 2, 3}));

    return true;
}

int main()
{
    using ranges::begin;
    using ranges::end;
    using ranges::size;

    std::pair<int, int> const a[] = {{0, 0}, {0, 1}, {1, 2}, {1, 3}, {3, 4}, {3, 5}};
    static_assert(size(a) == 6, "");
    std::pair<int, int> out[size(a)] = {};

    auto res = ranges::copy(begin(a), end(a), out);
    CHECK(res.in == end(a));
    CHECK(res.out == out + size(out));
    CHECK(std::equal(a, a + size(a), out));

    std::fill_n(out, size(out), std::make_pair(0, 0));
    CHECK(!std::equal(a, a + size(a), out));

    res = ranges::copy(a, out);
    CHECK(res.in == a + size(a));
    CHECK(res.out == out + size(out));
    CHECK(std::equal(a, a + size(a), out));

    std::fill_n(out, size(out), std::make_pair(0, 0));

    using ranges::views::delimit;
    {
        char const *sz = "hello world";
        char buf[50];
        auto str = delimit(sz, '\0');
        auto res3 = ranges::copy(str, buf);
        *res3.out = '\0';
        CHECK(res3.in == std::next(begin(str), static_cast<std::ptrdiff_t>(std::strlen(sz))));
        CHECK(res3.out == buf + std::strlen(sz));
        CHECK(std::strcmp(sz, buf) == 0);
    }

    {
        char const *sz = "hello world";
        char buf[50];
        auto str = delimit(sz, '\0');
        auto res3 = ranges::copy(std::move(str), buf);
        *res3.out = '\0';
        CHECK(!::is_dangling(res3.in));
        CHECK(res3.out == buf + std::strlen(sz));
        CHECK(std::strcmp(sz, buf) == 0);
    }

    {
        using namespace ranges;
        std::ostringstream sout;
        std::vector<int> copy_vec{1,1,1,1,1};
        copy(copy_vec, ostream_iterator<>(sout, " "));
        CHECK(sout.str() == "1 1 1 1 1 ");
    }

    {
        STATIC_CHECK(test_constexpr());
    }

    return test_result();
}
