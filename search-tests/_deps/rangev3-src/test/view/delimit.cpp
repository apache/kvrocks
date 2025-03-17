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
#include <range/v3/core.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/delimit.hpp>
#include <range/v3/algorithm/for_each.hpp>
#include <range/v3/utility/copy.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;

    auto rng0 = views::iota(10) | views::delimit(25);
    ::check_equal(rng0, {10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24});
    CPP_assert(view_<decltype(rng0)>);
    CPP_assert(!common_range<decltype(rng0)>);
    CPP_assert(random_access_iterator<decltype(rng0.begin())>);
    CPP_assert(view_<delimit_view<views::all_t<std::vector<int> &>, int>>);
    CPP_assert(random_access_range<delimit_view<views::all_t<std::vector<int> &>, int>>);

    std::vector<int> vi{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    auto rng1 = vi | views::delimit(50);
    ::check_equal(rng1, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

    auto rng2 = views::delimit(vi.begin(), 8);
    ::check_equal(rng2, {0, 1, 2, 3, 4, 5, 6, 7});

    {
        int const some_ints[] = {1,2,3,0,4,5,6};
        auto rng = debug_input_view<const int>{some_ints} | views::delimit(0);
        ::check_equal(rng, {1,2,3});
    }

    {
        int const some_ints[] = {1,2,3};
        auto rng = views::delimit(some_ints, 0);
        ::check_equal(rng, {1,2,3});
    }

    return test_result();
}
