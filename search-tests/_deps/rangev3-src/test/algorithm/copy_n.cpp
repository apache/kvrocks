// Range v3 library
//
//  Copyright Eric Niebler 2014
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <range/v3/algorithm/copy_n.hpp>

#include "../array.hpp"
#include "../simple_test.hpp"

constexpr bool test_constexpr()
{
    using namespace ranges;
    test::array<int, 4> a{{1, 2, 3, 4}};
    test::array<int, 4> b{{0, 0, 0, 0}};
    const auto res = copy_n(begin(a), 2, ranges::begin(b));
    STATIC_CHECK_RETURN(res.first == begin(a) + 2);
    STATIC_CHECK_RETURN(res.second == begin(b) + 2);
    STATIC_CHECK_RETURN(a[0] == 1);
    STATIC_CHECK_RETURN(a[1] == 2);
    STATIC_CHECK_RETURN(a[2] == 3);
    STATIC_CHECK_RETURN(a[3] == 4);
    STATIC_CHECK_RETURN(b[0] == 1);
    STATIC_CHECK_RETURN(b[1] == 2);
    STATIC_CHECK_RETURN(b[2] == 0);
    STATIC_CHECK_RETURN(b[3] == 0);
    return true;
}

int main()
{

    {
        STATIC_CHECK(test_constexpr());
    }

    return test_result();
}
