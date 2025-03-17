// Range v3 library
//
//  Copyright Barry Revzin 2019-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <range/v3/utility/copy.hpp>
#include <range/v3/view/take_last.hpp>

#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;

    int rgi[] = {0, 1, 2, 3, 4, 5};

    auto rng0 = rgi | views::take_last(3);
    has_type<int &>(*begin(rng0));
    CPP_assert(view_<decltype(rng0)>);
    CPP_assert(common_range<decltype(rng0)>);
    CPP_assert(sized_range<decltype(rng0)>);
    CPP_assert(random_access_iterator<decltype(begin(rng0))>);
    ::check_equal(rng0, {3, 4, 5});
    CHECK(size(rng0) == 3u);

    auto rng1 = rgi | views::take_last(7);
    ::check_equal(rng1, {0, 1, 2, 3, 4, 5});
}
