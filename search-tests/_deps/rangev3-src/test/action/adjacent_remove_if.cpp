/// \file
// Range v3 library
//
//  Copyright Eric Niebler
//  Copyright Christopher Di Bella
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
#include <range/v3/action/adjacent_remove_if.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include <range/v3/core.hpp>
#include <range/v3/view/iota.hpp>
#include <vector>

int main()
{
    using namespace ranges;

    auto v = views::ints(1,21) | to<std::vector>();
    auto & v2 = actions::adjacent_remove_if(v, [](int x, int y){ return (x + y) % 3 == 0; });
    CHECK(std::addressof(v) == std::addressof(v2));
    check_equal(v, {2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18, 20});

    v |= actions::adjacent_remove_if([](int x, int y){ return (y - x) == 2; });
    check_equal(v, {2, 5, 8, 11, 14, 17, 20});

    return ::test_result();
}
