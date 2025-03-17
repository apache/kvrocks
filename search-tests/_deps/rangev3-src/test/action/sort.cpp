// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)

#include <array>
#include <random>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/stride.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/algorithm/shuffle.hpp>
#include <range/v3/algorithm/copy.hpp>
#include <range/v3/algorithm/move.hpp>
#include <range/v3/algorithm/is_sorted.hpp>
#include <range/v3/algorithm/equal.hpp>
#include <range/v3/action/shuffle.hpp>
#include <range/v3/action/sort.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;
    std::mt19937 gen;

    auto v = views::ints(0,100) | to<std::vector>();
    v |= actions::shuffle(gen);
    CHECK(!is_sorted(v));

    auto v2 = v | copy | actions::sort;
    CHECK(size(v2) == size(v));
    CHECK(is_sorted(v2));
    CHECK(!is_sorted(v));
    CPP_assert(same_as<decltype(v), decltype(v2)>);

    v |= actions::sort;
    CHECK(is_sorted(v));

    v |= actions::shuffle(gen);
    CHECK(!is_sorted(v));

    v = v | move | actions::sort(std::less<int>());
    CHECK(is_sorted(v));
    CHECK(equal(v, v2));

    // Container algorithms can also be called directly
    // in which case they take and return by reference
    shuffle(v, gen);
    CHECK(!is_sorted(v));
    auto & v3 = actions::sort(v);
    CHECK(is_sorted(v));
    CHECK(&v3 == &v);

    auto r = views::ref(v);
    r |= actions::sort;

    // Can pipe a view to a "container" algorithm.
    actions::sort(v, std::greater<int>());
    v | views::stride(2) | actions::sort;
    check_equal(views::take(v, 10), {1,98,3,96,5,94,7,92,9,90});

    return ::test_result();
}
