// Range v3 library
//
//  Copyright Eric Niebler 2020-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <range/v3/utility/semiregular_box.hpp>
#include "../simple_test.hpp"

using namespace ranges;

// #https://github.com/ericniebler/range-v3/issues/1499
void test_1499()
{
    ranges::semiregular_box_t<int> box1;
    ranges::semiregular_box_t<int &> box2;

    detail::ignore_unused(
        box1,  //
        box2); //
}

int main()
{
    test_1499();

    return ::test_result();
}
