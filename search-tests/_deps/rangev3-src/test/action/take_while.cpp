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
#include <range/v3/action/take_while.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;
    using namespace std::placeholders;

    auto v = views::ints(1,21) | to<std::vector>();
    auto & v2 = actions::take_while(v, std::bind(std::less<int>(), _1, 18));
    CHECK(&v2 == &v);
    CHECK(v.size() == 17u);
    CHECK(v.back() == 17);

    v = std::move(v) | actions::take_while([](int i){return i < 15;});
    CHECK(v.size() == 14u);
    CHECK(v.back() == 14);

    v |= actions::take_while([](int i){return i < 12;});
    CHECK(v.size() == 11u);
    CHECK(v.back() == 11);

    v |= actions::take_while([](int){return true;});
    CHECK(v.size() == 11u);

    v |= actions::take_while([](int){return false;});
    CHECK(v.size() == 0u);

    return ::test_result();
}
