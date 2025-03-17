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

#include <list>
#include <range/v3/core.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/view/drop_while.hpp>
#include <range/v3/utility/copy.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

struct my_data
{
    int i;
};

bool operator==(my_data left, my_data right)
{
    return left.i == right.i;
}

int main()
{
    using namespace ranges;

    auto rng0 = views::iota(10) | views::drop_while([](int i) { return i < 25; });
    CPP_assert(range_cardinality<decltype(rng0)>::value == unknown);
    CPP_assert(view_<decltype(rng0)>);
    CPP_assert(random_access_range<decltype(rng0)>);
    CPP_assert(!common_range<decltype(rng0)>);
    CPP_assert(!sized_range<decltype(rng0)>);
    CPP_assert(random_access_iterator<decltype(rng0.begin())>);
    auto b = rng0.begin();
    CHECK(*b == 25);
    CHECK(*(b+1) == 26);
    ::check_equal(rng0 | views::take(10), {25, 26, 27, 28, 29, 30, 31, 32, 33, 34});

    std::list<int> vi{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    auto rng1 = vi | views::drop_while([](int i) { return i != 50; });
    CPP_assert(range_cardinality<decltype(rng1)>::value == ranges::finite);
    CPP_assert(view_<decltype(rng1)>);
    CPP_assert(bidirectional_range<decltype(rng1)>);
    CPP_assert(common_range<decltype(rng1)>);
    CPP_assert(!sized_range<decltype(rng1)>);
    CPP_assert(bidirectional_iterator<decltype(rng1.begin())>);
    CHECK(rng1.begin() == rng1.end());

    // Check with a mutable predicate
    static int const rgi[] = {0,1,2,3,4,5,6,7,8,9};
    int cnt = 0;
    auto mutable_only = views::drop_while(rgi, [cnt](int) mutable { return ++cnt <= 5;});
    ::check_equal(mutable_only, {5,6,7,8,9});
    CPP_assert(view_<decltype(mutable_only)>);
    CPP_assert(!view_<decltype(mutable_only) const>);

    {
        auto rng = debug_input_view<const int>{rgi} | views::drop_while([](int i){ return i < 4; });
        using R = decltype(rng);
        CPP_assert(input_range<R> && view_<R>);
        CPP_assert(!forward_range<R>);
        CPP_assert(!common_range<R>);
        CPP_assert(!sized_range<R>);
        CPP_assert(same_as<int const&, range_reference_t<R>>);
        ::check_equal(rng, {4,5,6,7,8,9});
    }

    {
        // with projection
        const std::list<my_data> data_list{{1}, {2}, {3}, {1}};
        auto rng = data_list | views::drop_while([](int i){ return i <= 2; }, &my_data::i);
        ::check_equal(rng, std::list<my_data>{{3}, {1}});
    }

    auto rng2 = rgi | views::drop_while([](int i) { return i != 50; });
    CPP_assert(range_cardinality<decltype(rng2)>::value == ranges::finite);
    CPP_assert(view_<decltype(rng2)>);
    CPP_assert(contiguous_range<decltype(rng2)>);
    CPP_assert(common_range<decltype(rng2)>);
    CPP_assert(!sized_range<decltype(rng2)>);
    CPP_assert(contiguous_iterator<decltype(rng2.begin())>);
    CHECK(ranges::size(rng2) == 0u);

    return test_result();
}
