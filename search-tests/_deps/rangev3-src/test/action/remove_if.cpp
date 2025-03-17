// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)

#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/algorithm/move.hpp>
#include <range/v3/action/sort.hpp>
#include <range/v3/action/remove_if.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;

    auto v = views::ints(1,21) | to<std::vector>();
    auto & v2 = actions::remove_if(v, [](int i){return i % 2 == 0;});
    CHECK(&v2 == &v);
    check_equal(v, {1,3,5,7,9,11,13,15,17,19});

    auto && v3 = v | move | actions::remove_if(std::bind(std::less<int>{}, std::placeholders::_1, 10));
    check_equal(v3, {11,13,15,17,19});

    return ::test_result();
}
