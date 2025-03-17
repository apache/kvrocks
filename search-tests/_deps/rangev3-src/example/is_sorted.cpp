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

///[is_sorted]
// Check if a container is sorted
// output
// vector:   true
// array:    false

#include <array>
#include <iostream>
#include <range/v3/algorithm/is_sorted.hpp> // specific includes
#include <vector>
using std::cout;

int
main()
{
    cout << std::boolalpha;
    std::vector<int> v{1, 2, 3, 4, 5, 6};
    cout << "vector:   " << ranges::is_sorted(v) << '\n';

    std::array<int, 6> a{6, 2, 3, 4, 5, 6};
    cout << "array:    " << ranges::is_sorted(a) << '\n';
}
///[is_sorted]
