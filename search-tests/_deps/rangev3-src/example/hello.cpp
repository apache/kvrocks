
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

///[hello]
#include <iostream>
#include <range/v3/all.hpp> // get everything
#include <string>
using std::cout;

int
main()
{
    std::string s{"hello"};

    // output: h e l l o
    ranges::for_each(s, [](char c) { cout << c << ' '; });
    cout << '\n';
}
///[hello]
