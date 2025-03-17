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
#include <range/v3/view/drop_exactly.hpp>
#include <range/v3/view/generate_n.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

namespace views = ranges::views;

int main()
{
    // Test for constant generator functions
    {
        int i = 0, j = 1;
        auto fib = views::generate_n([&]()->int{int tmp = i; i += j; std::swap(i, j); return tmp;}, 10);
        CPP_assert(ranges::input_range<decltype(fib)> && ranges::view_<decltype(fib)>);
        check_equal(fib, {0,1,1,2,3,5,8,13,21,34});
    }

    // Test for mutable-only generator functions
    {
        int i = 0, j = 1;
        auto fib = views::generate_n([=]()mutable->int{int tmp = i; i += j; std::swap(i, j); return tmp;}, 10);
        CPP_assert(ranges::input_range<decltype(fib)> && ranges::view_<decltype(fib)>);
        check_equal(fib, {0,1,1,2,3,5,8,13,21,34});
        // The generator cannot be called when it's const-qualified, so "fib const"
        // does not model View.
        CPP_assert(!ranges::view_<decltype(fib) const>);
    }

    // Test for generator functions that return move-only types
    // https://github.com/ericniebler/range-v3/issues/905
    {
        char str[] = "gi";
        auto rng = views::generate_n([&]{str[0]++; return MoveOnlyString{str};}, 2);
        CPP_assert(ranges::input_range<decltype(rng)> && ranges::view_<decltype(rng)>);
        auto i = rng.begin();
        CHECK(bool(*i == MoveOnlyString{"hi"}));
        CHECK(bool(*i == MoveOnlyString{"hi"}));
        CHECK(bool(*rng.begin() == MoveOnlyString{"hi"}));
        CHECK(bool(*rng.begin() == MoveOnlyString{"hi"}));
        CPP_assert(ranges::input_range<decltype(rng)> && ranges::view_<decltype(rng)>);
        check_equal(rng, {MoveOnlyString{"hi"}, MoveOnlyString{"ii"}});
        static_assert(std::is_same<ranges::range_reference_t<decltype(rng)>, MoveOnlyString &&>::value, "");
    }

    // Test for generator functions that return internal references
    // https://github.com/ericniebler/range-v3/issues/807
    {
        int i = 42;
        auto rng = views::generate_n([i]{return &i;}, 2);
        auto rng2 = std::move(rng);
        auto it = rng2.begin();
        auto p = *it;
        auto p2 = *++it;
        CHECK(p == p2);
    }

    // Test that we only call the function once for each dereferenceable position
    // https://github.com/ericniebler/range-v3/issues/819
    {
        int i = 0;
        auto rng = views::generate_n([&i]{return ++i;}, 2);
        auto rng2 = std::move(rng);
        auto it = rng2.begin();
        CHECK(i == 0);
        CHECK(*it == 1);
        CHECK(i == 1);
        ++it;
        CHECK(i == 1);
        CHECK(*it == 2);
        CHECK(i == 2);
    }

    // Test view_interaface sized()-based empty()
    // https://github.com/ericniebler/range-v3/issues/1147
    {
        int i = 0;
        auto rng = views::generate_n([&i]{return ++i;}, 2);
        CHECK(!rng.empty());
    }


    // Test that skipping past positions works correctly
    // https://github.com/ericniebler/range-v3/issues/1258
    {
        auto fib = [p = std::make_pair(0, 1)]() mutable -> int {
            auto a = p.first;
            p = {p.second, p.first + p.second};
            return a;
        };

        auto rng = ranges::views::generate_n(fib, 8)
            | ranges::views::drop_exactly(3);

        check_equal(rng, {2,3,5,8,13});
    }

    return test_result();
}
