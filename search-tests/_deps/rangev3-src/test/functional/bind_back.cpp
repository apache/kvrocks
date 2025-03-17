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

#include <range/v3/functional/bind_back.hpp>
#include <range/v3/functional/concepts.hpp>
#include "../simple_test.hpp"

using namespace ranges;

int* test(int & i)
{
    return &i;
}

int main()
{
    int i = 42;
    auto fn = bind_back(test, i);
    int* pi = fn();
    CHECK(pi != &i);
    CHECK(*pi == i);

    CPP_assert(!invocable<decltype(fn)>);
    CPP_assert(invocable<decltype(fn) &>);
    CPP_assert(!invocable<decltype(fn) const &>);

    return ::test_result();
}
