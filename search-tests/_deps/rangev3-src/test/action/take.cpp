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
#include <range/v3/action/take.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;

    auto v = views::ints(1,21) | to<std::vector>();
    auto & v2 = actions::take(v, 17);
    CHECK(&v2 == &v);
    CHECK(v.size() == 17u);
    CHECK(v.back() == 17);

    v = std::move(v) | actions::take(14);
    CHECK(v.size() == 14u);
    CHECK(v.back() == 14);

    v |= actions::take(11);
    CHECK(v.size() == 11u);
    CHECK(v.back() == 11);

    v |= actions::take(100);
    CHECK(v.size() == 11u);

    v |= actions::take(0);
    CHECK(v.size() == 0u);

    return ::test_result();
}
