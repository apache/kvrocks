
// Range v3 library
//
//  Copyright Jeff Garland 2017
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

///[count_if]
// This example counts element of a range that match a supplied predicate.
// output
// vector:   2
// array:    2

#include <array>
#include <iostream>
#include <range/v3/algorithm/count_if.hpp> // specific includes
#include <vector>
using std::cout;

auto is_six = [](int i) -> bool { return i == 6; };

int
main()
{
    std::vector<int> v{6, 2, 3, 4, 5, 6};
    auto c = ranges::count_if(v, is_six);
    cout << "vector:   " << c << '\n'; // 2

    std::array<int, 6> a{6, 2, 3, 4, 5, 6};
    c = ranges::count_if(a, is_six);
    cout << "array:    " << c << '\n'; // 2
}
///[count_if]
