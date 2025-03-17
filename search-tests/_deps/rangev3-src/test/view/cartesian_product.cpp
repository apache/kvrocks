/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2013-2014.
//  Copyright Casey Carter 2017.
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#include <climits>
#include <iostream>
#include <range/v3/range/access.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/range_for.hpp>
#include <range/v3/view/span.hpp>
#include <range/v3/utility/tuple_algorithm.hpp>
#include <range/v3/view/cartesian_product.hpp>
#include <range/v3/view/chunk.hpp>
#include <range/v3/view/empty.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/indices.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/view/single.hpp>
#include <range/v3/view/take_exactly.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/enumerate.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

RANGES_DIAGNOSTIC_IGNORE_RANGE_LOOP_ANALYSIS

using namespace ranges;

struct printer
{
    std::ostream &os_;
    bool &first_;

    template<typename T>
    void operator()(T const &t) const
    {
        if (first_) first_ = false;
        else os_ << ',';
        os_ << t;
    }
};

namespace std
{
    template<typename... Ts>
    std::ostream &operator<<(std::ostream &os, std::tuple<Ts...> const &t)
    {
        os << '(';
        auto first = true;
        ::ranges::tuple_for_each(t, ::printer{os, first});
        os << ')';
        return os;
    }
}

void test_empty_set()
{
    auto rng = views::cartesian_product();
    using Rng = decltype(rng);
    CPP_assert(range_cardinality<Rng>::value ==
        static_cast<cardinality>(0));

    CPP_assert(random_access_range<Rng> && view_<Rng>);
    CPP_assert(common_range<Rng>);
    CPP_assert(sized_range<Rng>);
    CHECK(size(rng) == 0u);
    CHECK(empty(rng));

    CPP_assert(std::is_same<
        range_value_t<Rng>,
        std::tuple<>>());
    CPP_assert(std::is_same<
        range_reference_t<Rng>,
        std::tuple<>&>());

    std::initializer_list<common_tuple<>> control{};
    ::check_equal(rng, control);
    ::check_equal(views::reverse(rng), views::reverse(control));

    auto const first = begin(rng);
    auto const last = end(rng);
    CHECK(decltype(size(rng))(last - first) == size(rng));
    for(auto i = 0; i <= distance(rng); ++i)
    {
        for(auto j = 0; j <= distance(rng); ++j)
        {
            CHECK((next(first, i) - next(first, j)) == i - j);
        }
    }
}

void test_empty_range()
{
    int some_ints[] = {0,1,2,3};
    auto e = views::empty<char>;
    auto rng = views::cartesian_product(
        span<int, size(some_ints)>{some_ints},
        e
    );
    using Rng = decltype(rng);
    CPP_assert(range_cardinality<Rng>::value ==
        static_cast<cardinality>(0));

    CPP_assert(random_access_range<Rng> && view_<Rng>);
    CPP_assert(common_range<Rng>);
    CPP_assert(sized_range<Rng>);
    CHECK(size(rng) == 0u);

    CPP_assert(std::is_same<
        range_value_t<Rng>,
        std::tuple<int, char>>());
    CPP_assert(std::is_same<
        range_reference_t<Rng>,
        common_tuple<int &, char &>>());

    using CT = common_tuple<int, char>;
    std::initializer_list<CT> control = {};

    ::check_equal(rng, control);
    ::check_equal(views::reverse(rng), views::reverse(control));

    auto const first = begin(rng);
    auto const last = end(rng);
    CHECK((last - first) == (std::intmax_t) size(rng));
    for(auto i = 0; i <= distance(rng); ++i)
    {
        for(auto j = 0; j <= distance(rng); ++j)
        {
            CHECK((next(first, i) - next(first, j)) == i - j);
        }
    }
}

void test_bug_820()
{
    // https://github.com/ericniebler/range-v3/issues/820
    using CT = common_tuple<int, int>;
    std::initializer_list<CT> control = {
        CT{0, 0}, CT{0, 1}, CT{0, 2},
        CT{1, 0}, CT{1, 1}, CT{1, 2},
        CT{2, 0}, CT{2, 1}, CT{2, 2}
    };

    auto x = ranges::views::iota(0) | ranges::views::take_exactly(3);
    auto y = ranges::views::cartesian_product(x, x);
    ::check_equal(y, control);
}

void test_bug_823()
{
    // https://github.com/ericniebler/range-v3/issues/823
    auto three = ranges::views::iota(0) | ranges::views::take_exactly(3);
    CPP_assert(ranges::random_access_range<decltype(three)> && ranges::view_<decltype(three)>);
    CPP_assert(!(ranges::random_access_range<const decltype(three)> && ranges::view_<const decltype(three)>));

    auto prod = ranges::views::cartesian_product(three, three);
    CPP_assert(ranges::random_access_range<decltype(prod)> && ranges::view_<decltype(prod)>);
    CPP_assert(!(ranges::random_access_range<const decltype(prod)> && ranges::view_<const decltype(prod)>));
    CPP_assert(ranges::sized_range<decltype(prod)>);
    CHECK(ranges::size(prod) == 9u);

    {
        int i = 0;
        RANGES_FOR(auto&& x, prod) {
            (void)x;
            RANGES_ENSURE(i++ < 3 * 3);
        }
        CHECK(i == 3 * 3);
    }

    auto twoD = prod | ranges::views::chunk(3);
    CPP_assert(ranges::random_access_range<decltype(twoD)> && ranges::view_<decltype(twoD)>);
    CPP_assert(!(ranges::random_access_range<const decltype(twoD)> && ranges::view_<const decltype(twoD)>));

    {
        int i = 0;
        RANGES_FOR(auto&& row, twoD) {
            (void)row;
            RANGES_ENSURE(i++ < 3);
        }
        CHECK(i == 3);
    }

    {
        int i = 0;
        RANGES_FOR(auto&& row, twoD) {
            RANGES_ENSURE(i++ < 3);
            int j = 0;
            RANGES_FOR(auto&& col, row) {
                (void)col;
                RANGES_ENSURE(j++ < 3);
            }
            CHECK(j == 3);
        }
        CHECK(i == 3);
    }
}

void test_bug_919()
{
    // https://github.com/ericniebler/range-v3/issues/919
    int some_ints[] = {0,1,2,3};
    char const * some_strings[] = {"John", "Paul", "George", "Ringo"};
    auto rng = views::cartesian_product(
        span<int, size(some_ints)>{some_ints},
        span<char const*, size(some_strings)>{some_strings}
    );
    constexpr std::intmax_t n = size(rng);
    static_assert(n == 16, "");

    for (std::intmax_t i = 0; i <= n; ++i) {
        auto const x = rng.begin() + i;
        CHECK((x == rng.end() - (n - i)));
        for (std::intmax_t j = 0; j <= n; ++j)
            CHECK((rng.begin() + j == x + (j - i)));
    }
}

void test_bug_978()
{
    // https://github.com/ericniebler/range-v3/issues/978
    int rgi[] = {1};
    ranges::views::cartesian_product(
        rgi | ranges::views::filter([](int){ return true; }),
        rgi
    );
}

void test_bug_1269()
{
    // https://github.com/ericniebler/range-v3/issues/1269
    int data0[2]{}, data1[3]{}, data2[5]{}, data3[7]{};
    constexpr std::size_t N = ranges::size(data0) * ranges::size(data1) *
        ranges::size(data2) * ranges::size(data3);
    CPP_assert(N < INT_MAX);

    auto rng = ranges::views::cartesian_product(data0, data1, data2, data3);
    CPP_assert(ranges::sized_range<decltype(rng)>);
    CHECK(ranges::size(rng) == N);

    CPP_assert(ranges::random_access_range<decltype(rng)>);
    CPP_assert(ranges::sized_sentinel_for<ranges::sentinel_t<decltype(rng)>, ranges::iterator_t<decltype(rng)>>);
    for (int i = 0; i < int{N}; ++i)
    {
        auto pos = ranges::begin(rng) + i;
        CHECK((ranges::end(rng) - pos) == std::intmax_t{N} - i);
    }
}

void test_bug_1279()
{
    // https://github.com/ericniebler/range-v3/issues/1279
    auto const xs = ranges::views::indices(std::size_t{0}, std::size_t{10});
    auto const ys = ranges::views::indices(std::size_t{0}, std::size_t{10});

    for(auto r : ranges::views::cartesian_product(ys, xs))
    {
        (void) r;
    }
}

void test_bug_1296()
{
    // https://github.com/ericniebler/range-v3/issues/1296
    auto v = ranges::views::cartesian_product(ranges::views::single(42))
        | ranges::views::transform([](std::tuple<int> a) {
            return std::get<0>(a);
        });

    CHECK(ranges::size(v) == 1u);
    CHECK(*ranges::begin(v) == 42);
}

// https://github.com/ericniebler/range-v3/issues/1422
void test_1422()
{
    int v1[] = {1,2,3};
    auto e = v1 | ranges::views::enumerate;
    auto cp = ranges::views::cartesian_product(e, e);
    using CP = decltype(cp);
    CPP_assert(ranges::input_range<CP>);
}

int main()
{
    int some_ints[] = {0,1,2,3};
    char const * some_strings[] = {"John", "Paul", "George", "Ringo"};
    auto rng = views::cartesian_product(
        span<int, size(some_ints)>{some_ints},
        span<char const*, size(some_strings)>{some_strings}
    );
    using Rng = decltype(rng);
    CPP_assert(range_cardinality<Rng>::value ==
        range_cardinality<decltype(some_ints)>::value *
        range_cardinality<decltype(some_strings)>::value);

    CPP_assert(random_access_range<Rng> && view_<Rng>);
    CPP_assert(common_range<Rng>);
    CPP_assert(sized_range<Rng>);
    CHECK(size(rng) == size(some_ints) * size(some_strings));

    CPP_assert(std::is_same<
        range_value_t<Rng>,
        std::tuple<int, char const *>>());
    CPP_assert(std::is_same<
        range_reference_t<Rng>,
        common_tuple<int &, char const * &>>());

    using CT = common_tuple<int, std::string>;
    std::initializer_list<CT> control = {
        CT{0, "John"}, CT{0, "Paul"}, CT{0, "George"}, CT{0, "Ringo"},
        CT{1, "John"}, CT{1, "Paul"}, CT{1, "George"}, CT{1, "Ringo"},
        CT{2, "John"}, CT{2, "Paul"}, CT{2, "George"}, CT{2, "Ringo"},
        CT{3, "John"}, CT{3, "Paul"}, CT{3, "George"}, CT{3, "Ringo"}
    };

    ::check_equal(rng, control);
    ::check_equal(views::reverse(rng), views::reverse(control));

    auto const first = begin(rng);
    auto const last = end(rng);
    CHECK((last - first) == (std::intmax_t) size(rng));
    for(auto i = 0; i <= distance(rng); ++i)
    {
        for(auto j = 0; j <= distance(rng); ++j)
        {
            CHECK((next(first, i) - next(first, j)) == i - j);
        }
    }

    test_empty_set();
    test_empty_range();
    test_bug_820();
    test_bug_823();
    test_bug_919();
    test_bug_978();
    test_bug_1269();
    test_bug_1279();
    test_bug_1296();

    return test_result();
}
