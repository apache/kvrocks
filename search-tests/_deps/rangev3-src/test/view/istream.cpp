// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <range/v3/view/istream.hpp>
#include <range/v3/view/subrange.hpp>
#include <sstream>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    static const char test[] = "abcd3210";
    std::istringstream ss{test};
    ::check_equal(ranges::istream<char>(ss),
                  ranges::make_subrange(test, test + sizeof(test) - 1));
    return ::test_result();
}
