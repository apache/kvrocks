// Range v3 library
//
//  Copyright Eric Niebler 2014
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#include <utility>

#include <range/v3/algorithm/find_if_not.hpp>
#include <range/v3/core.hpp>

#include "../simple_test.hpp"
#include "../test_iterators.hpp"

constexpr bool is_three(int i)
{
    return i == 3;
}

template<class Rng>
constexpr bool contains_other_than_three(Rng r)
{
    auto it = ranges::find_if_not(r, is_three);
    return it != ranges::end(r);
}

int main()
{
    using namespace ranges;

    {
        using IL = std::initializer_list<int>;
        STATIC_CHECK(contains_other_than_three(IL{3, 3, 2, 3}));
        STATIC_CHECK(!contains_other_than_three(IL{3, 3, 3}));
    }

    return ::test_result();
}
