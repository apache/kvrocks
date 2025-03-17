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

///[count]
// This example demonstrates counting the number of
// elements that match a given value.
// output...
// vector:   2
// array:    2

#include <iostream>
#include <range/v3/algorithm/count.hpp> // specific includes
#include <vector>
using std::cout;

int
main()
{
    std::vector<int> v{6, 2, 3, 4, 5, 6};
    // note the count return is a numeric type
    // like int or long -- auto below make sure
    // it matches the implementation
    auto c = ranges::count(v, 6);
    cout << "vector:   " << c << '\n';

    std::array<int, 6> a{6, 2, 3, 4, 5, 6};
    c = ranges::count(a, 6);
    cout << "array:    " << c << '\n';
}

///[count]
