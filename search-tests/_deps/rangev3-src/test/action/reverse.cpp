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
#include <range/v3/view/repeat_n.hpp>
#include <range/v3/view/for_each.hpp>
#include <range/v3/action/reverse.hpp>
#include <range/v3/action/unique.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

using namespace ranges;

int main()
{
    // [1,2,2,3,3,3,4,4,4,4,5,5,5,5,5,...]
    auto v =
        views::for_each(views::ints(1,6), [](int i){
            return yield_from(views::repeat_n(i,i));
        }) | to<std::vector>();
    check_equal(v, {1,2,2,3,3,3,4,4,4,4,5,5,5,5,5});

    v |= actions::unique | actions::reverse;
    check_equal(v, {5,4,3,2,1});

    return ::test_result();
}
