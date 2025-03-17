// Range v3 library
//
//  Copyright Filip Matzner 2015
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)

#include <vector>
#include <random>
#include <range/v3/core.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/stride.hpp>
#include <range/v3/algorithm/copy.hpp>
#include <range/v3/algorithm/move.hpp>
#include <range/v3/algorithm/is_sorted.hpp>
#include <range/v3/algorithm/equal.hpp>
#include <range/v3/algorithm/sort.hpp>
#include <range/v3/action/shuffle.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;
    std::mt19937 gen;

    // "Ints" view vs. shuffled
    auto v = views::ints(0,100) | to<std::vector>();
    auto v2 = v | copy | actions::shuffle(gen);
    CHECK(is_sorted(v));
    CHECK(!is_sorted(v2));
    CHECK(size(v2) == size(v));
    CPP_assert(same_as<decltype(v), decltype(v2)>);
    CHECK(!equal(v, v2));

    // "Ints" view vs. shuffled and sorted
    sort(v2);
    CHECK(is_sorted(v2));
    CHECK(equal(v, v2));

    // Shuffled vs. shuffled
    v |= actions::shuffle(gen);
    v2 = v2 | move | actions::shuffle(gen);
    CHECK(!is_sorted(v));
    CHECK(!is_sorted(v2));
    CHECK(size(v2) == size(v));
    CHECK(!equal(v, v2));

    // Container algorithms can also be called directly
    // in which case they take and return by reference
    v = views::ints(0,100) | to<std::vector>();
    auto & v3 = actions::shuffle(v, gen);
    CHECK(!is_sorted(v));
    CHECK(&v3 == &v);

    // Create and shuffle container reference
    v = views::ints(0,100) | to<std::vector>();
    auto r = views::ref(v);
    r |= actions::shuffle(gen);
    CHECK(!is_sorted(v));

    // Can pipe a view to a "container" algorithm.
    v = views::ints(0,100) | to<std::vector>();
    v | views::stride(2) | actions::shuffle(gen);
    CHECK(!is_sorted(v));

    return ::test_result();
}
