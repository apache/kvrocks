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

#include <array>
#include <vector>
#include <iterator>
#include <functional>
#include <range/v3/core.hpp>
#include <range/v3/functional/not_fn.hpp>
#include <range/v3/view/remove_if.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/counted.hpp>
#include <range/v3/view/concat.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/view/subrange.hpp>
#include <range/v3/utility/copy.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

struct is_odd
{
    constexpr bool operator()(int i) const
    {
        return (i % 2) == 1;
    }
};

struct is_even
{
    constexpr bool operator()(int i) const
    {
        return (i % 2) == 0;
    }
};

struct my_data
{
    int i;
};

constexpr bool operator==(my_data left, my_data right)
{
    return left.i == right.i;
}

int main()
{
    using namespace ranges;

    int rgi[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    {
        auto rng = rgi | views::remove_if(is_even());
        has_type<int &>(*begin(rgi));
        has_type<int &>(*begin(rng));
        CPP_assert(view_<decltype(rng)>);
        CPP_assert(common_range<decltype(rng)>);
        CPP_assert(!sized_range<decltype(rng)>);
        CPP_assert(bidirectional_range<decltype(rng)>);
        CPP_assert(!random_access_range<decltype(rng)>);
        ::check_equal(rng, {1,3,5,7,9});
        ::check_equal(rng | views::reverse, {9,7,5,3,1});
        auto tmp = rng | views::reverse;
        CHECK(&*begin(tmp) == &rgi[8]);
    }

    {
        auto rng2 = views::counted(rgi, 10) | views::remove_if(not_fn(is_odd()));
        has_type<int &>(*begin(rng2));
        CPP_assert(view_<decltype(rng2)>);
        CPP_assert(bidirectional_range<decltype(rng2)>);
        CPP_assert(!random_access_range<decltype(rng2)>);
        CPP_assert(common_range<decltype(rng2)>);
        CPP_assert(!sized_range<decltype(rng2)>);
        ::check_equal(rng2, {1,3,5,7,9});
        CHECK(&*begin(rng2) == &rgi[0]);
    }

    {
        auto rng3 = views::counted(BidirectionalIterator<int*>{rgi}, 10) | views::remove_if(is_even());
        has_type<int &>(*begin(rng3));
        CPP_assert(view_<decltype(rng3)>);
        CPP_assert(bidirectional_range<decltype(rng3)>);
        CPP_assert(!random_access_range<decltype(rng3)>);
        CPP_assert(!common_range<decltype(rng3)>);
        CPP_assert(!sized_range<decltype(rng3)>);
        ::check_equal(rng3, {1,3,5,7,9});
        CHECK(&*begin(rng3) == &rgi[0]);
        CHECK(&*prev(next(begin(rng3))) == &rgi[0]);
    }

    {
        // Test remove_if with a mutable lambda
        bool flag = true;
        auto mutable_rng = views::remove_if(rgi, [flag](int) mutable { return flag = !flag;});
        ::check_equal(mutable_rng, {1,3,5,7,9});
        CPP_assert(view_<decltype(mutable_rng)>);
        CPP_assert(!view_<decltype(mutable_rng) const>);
    }

    {
        const std::array<int, 3> a{{0, 1, 2}};
        const std::vector<int> b{3, 4, 5, 6};

        auto r = views::concat(a, b);
        auto f = [](int i) { return i != 1 && i != 5; };
        auto r2 = r | views::remove_if(f);
        ::check_equal(r2, {1,5});
    }

    {
        auto rng = debug_input_view<int const>{rgi} | views::remove_if(is_even{});
        ::check_equal(rng, {1,3,5,7,9});
    }

    {
        // Defend against regression of #793
        int const some_ints[] = {1, 2, 3};
        auto a = some_ints | ranges::views::remove_if([](int val) { return val > 0; });
        CHECK(a.empty());
    }

    {
        // with projection
        const std::vector<my_data> some_my_datas{{1}, {2}, {3}, {4}};

        {
            // views::remove_if without pipe
            auto rng = ranges::views::remove_if(some_my_datas, is_even(), &my_data::i);
            ::check_equal(rng, std::vector<my_data>{{1}, {3}});
        }

        {
            // views::remove_if with pipe
            auto rng = some_my_datas | ranges::views::remove_if(is_even(), &my_data::i);
            ::check_equal(rng, std::vector<my_data>{{1}, {3}});
        }

        {
            // views::filter without pipe
            auto rng = ranges::views::filter(some_my_datas, is_even(), &my_data::i);
            ::check_equal(rng, std::vector<my_data>{{2}, {4}});
        }

        {
            // views::filter with pipe
            auto rng = some_my_datas | ranges::views::filter(is_even(), &my_data::i);
            ::check_equal(rng, std::vector<my_data>{{2}, {4}});
        }
    }

    // test constexpr binding
    {
        using namespace ranges;
        constexpr std::array<int, 4> is = {{1,2,3,4}};
        constexpr auto filter = views::remove_if(is_even()) | views::remove_if(is_odd());
        auto rng = is | filter;
        CHECK(rng.empty());
    }
    {
        const std::vector<my_data> some_my_datas{{1}, {2}, {3}, {4}};
        constexpr auto filter = views::remove_if(is_even(), &my_data::i) | views::remove_if(is_odd(), &my_data::i);
        auto rng = some_my_datas | filter;
        CHECK(rng.empty());
    }

    // test issue #1424 <https://github.com/ericniebler/range-v3/issues/1424>
    {
        std::vector<int> v{1, 2, 3, 4};
        auto rng = views::filter(v, is_odd());
        ::check_equal(rng, {1, 3});
    }

    return test_result();
}
