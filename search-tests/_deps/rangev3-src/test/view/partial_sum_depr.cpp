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
RANGES_DIAGNOSTIC_IGNORE_DEPRECATED_DECLARATIONS
#include <range/v3/view/partial_sum.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    static int const some_ints[] = {0,1,2,3,4};
    auto rng = some_ints | ranges::views::partial_sum();
    ::check_equal(rng, {0,1,3,6,10});

    return ::test_result();
}
