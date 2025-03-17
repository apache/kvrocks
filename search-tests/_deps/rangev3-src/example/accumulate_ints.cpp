// Range v3 library
//
//  Copyright Eric Niebler 2019
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

///[accumulate_ints]
// Sums the first ten squares and prints them, using views::ints to generate
// and infinite range of integers, views::transform to square them, views::take
// to drop all but the first 10, and accumulate to sum them.

#include <iostream>
#include <vector>

#include <range/v3/numeric/accumulate.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/view/transform.hpp>
using std::cout;

int main()
{
    using namespace ranges;
    int sum = accumulate(views::ints(1, unreachable) | views::transform([](int i) {
                             return i * i;
                         }) | views::take(10),
                         0);
    // prints: 385
    cout << sum << '\n';
}
///[accumulate_ints]
