// Range v3 library
//
//  Copyright Eric Niebler 2020
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <range/v3/functional/on.hpp>
#include <range/v3/functional/concepts.hpp>
#include "../simple_test.hpp"

using namespace ranges;

int square(int i) { return i * i; }

int main()
{
    auto fn = on(std::multiplies<>{}, square);
    CHECK(fn(2, 4) == 64);

    return ::test_result();
}
