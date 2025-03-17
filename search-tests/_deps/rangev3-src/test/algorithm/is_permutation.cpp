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
//
//  Copyright 2005 - 2007 Adobe Systems Incorporated
//  Distributed under the MIT License(see accompanying file LICENSE_1_0_0.txt
//  or a copy at http://stlab.adobe.com/licenses.html)

//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#include <utility>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/permutation.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

RANGES_DIAGNOSTIC_IGNORE_DEPRECATED_DECLARATIONS

int comparison_count = 0;

template<typename T>
bool counting_equals( T const &a, T const &b )
{
    ++comparison_count;
    return a == b;
}

struct S
{
    int i;
};

struct T
{
    int i;
};

int main()
{
    {
        const int ia[] = {0};
        const int ib[] = {0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + 0),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa - 1),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0};
        const int ib[] = {1};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }

    {
        const int ia[] = {0, 0};
        const int ib[] = {0, 0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa - 1),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 0};
        const int ib[] = {0, 1};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 0};
        const int ib[] = {1, 0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 0};
        const int ib[] = {1, 1};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 1};
        const int ib[] = {0, 0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 1};
        const int ib[] = {0, 1};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa - 1),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 1};
        const int ib[] = {1, 0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa - 1),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 1};
        const int ib[] = {1, 1};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {1, 0};
        const int ib[] = {0, 0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {1, 0};
        const int ib[] = {0, 1};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa - 1),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {1, 0};
        const int ib[] = {1, 0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa - 1),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {1, 0};
        const int ib[] = {1, 1};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {1, 1};
        const int ib[] = {0, 0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {1, 1};
        const int ib[] = {0, 1};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {1, 1};
        const int ib[] = {1, 0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {1, 1};
        const int ib[] = {1, 1};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa - 1),
                                   std::equal_to<const int>()) == false);
    }

    {
        const int ia[] = {0, 0, 0};
        const int ib[] = {1, 0, 0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 0, 0};
        const int ib[] = {1, 0, 1};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 0, 0};
        const int ib[] = {1, 0, 2};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 0, 0};
        const int ib[] = {1, 1, 0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 0, 0};
        const int ib[] = {1, 1, 1};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 0, 0};
        const int ib[] = {1, 1, 2};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 0, 0};
        const int ib[] = {1, 2, 0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 0, 0};
        const int ib[] = {1, 2, 1};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 0, 0};
        const int ib[] = {1, 2, 2};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 0, 1};
        const int ib[] = {1, 0, 0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa - 1),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 0, 1};
        const int ib[] = {1, 0, 1};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 1, 2};
        const int ib[] = {1, 0, 2};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa - 1),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 1, 2};
        const int ib[] = {1, 2, 0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa - 1),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 1, 2};
        const int ib[] = {2, 1, 0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa - 1),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 1, 2};
        const int ib[] = {2, 0, 1};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa - 1),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 0, 1};
        const int ib[] = {1, 0, 1};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 0, 1};
        const int ib[] = {1, 0, 0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib + 1),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa - 1),
                                   std::equal_to<const int>()) == false);
    }
    {
        const int ia[] = {0, 1, 2, 3, 0, 5, 6, 2, 4, 4};
        const int ib[] = {4, 2, 3, 0, 1, 4, 0, 5, 6, 2};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib + 1),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa - 1),
                                   std::equal_to<const int>()) == false);
        comparison_count = 0;
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa - 1),
                                   counting_equals<const int>) == false);
        CHECK( comparison_count > 0 );
        comparison_count = 0;
        CHECK(ranges::is_permutation(RandomAccessIterator<const int*>(ia),
                                   RandomAccessIterator<const int*>(ia + sa),
                                   RandomAccessIterator<const int*>(ib),
                                   RandomAccessIterator<const int*>(ib + sa - 1),
                                   counting_equals<const int>) == false);
        CHECK ( comparison_count == 0 );
    }
    {
        const int ia[] = {0, 1, 2, 3, 0, 5, 6, 2, 4, 4};
        const int ib[] = {4, 2, 3, 0, 1, 4, 0, 5, 6, 0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<const int>()) == false);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa),
                                   std::equal_to<const int>()) == false);
    }

    // Iterator tests, without predicate:
    {
        const int ia[] = {0, 1, 2, 3, 0, 5, 6, 2, 4, 4};
        const int ib[] = {4, 2, 3, 0, 1, 4, 0, 5, 6, 2};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib)) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa)) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib + 1),
                                   ForwardIterator<const int*>(ib + sa)) == false);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                   ForwardIterator<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                   ForwardIterator<const int*>(ib + sa - 1)) == false);
    }

    // Iterator tests, with sentinels:
    {
        const int ia[] = {0, 1, 2, 3, 0, 5, 6, 2, 4, 4};
        const int ib[] = {4, 2, 3, 0, 1, 4, 0, 5, 6, 2};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                    Sentinel<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib)) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                    Sentinel<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                    Sentinel<const int*>(ib + sa)) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                    Sentinel<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib + 1),
                                    Sentinel<const int*>(ib + sa)) == false);
        CHECK(ranges::is_permutation(ForwardIterator<const int*>(ia),
                                    Sentinel<const int*>(ia + sa),
                                   ForwardIterator<const int*>(ib),
                                    Sentinel<const int*>(ib + sa - 1)) == false);
    }

    // common_range tests, with sentinels:
    {
        const int ia[] = {0, 1, 2, 3, 0, 5, 6, 2, 4, 4};
        const int ib[] = {4, 2, 3, 0, 1, 4, 0, 5, 6, 2};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ranges::make_subrange(ForwardIterator<const int*>(ia),
                                    Sentinel<const int*>(ia + sa)),
                                   ForwardIterator<const int*>(ib)) == true);

        CHECK(ranges::is_permutation(ranges::make_subrange(ForwardIterator<const int*>(ia),
                                    Sentinel<const int*>(ia + sa)),
                                   ranges::make_subrange(ForwardIterator<const int*>(ib),
                                    Sentinel<const int*>(ib + sa))) == true);
        CHECK(ranges::is_permutation(ranges::make_subrange(ForwardIterator<const int*>(ia),
                                    Sentinel<const int*>(ia + sa)),
                                   ranges::make_subrange(ForwardIterator<const int*>(ib + 1),
                                    Sentinel<const int*>(ib + sa))) == false);
        CHECK(ranges::is_permutation(ranges::make_subrange(ForwardIterator<const int*>(ia),
                                    Sentinel<const int*>(ia + sa)),
                                   ranges::make_subrange(ForwardIterator<const int*>(ib),
                                    Sentinel<const int*>(ib + sa - 1))) == false);
    }

    // common_range tests, with sentinels, with predicate:
    {
        const int ia[] = {0, 1, 2, 3, 0, 5, 6, 2, 4, 4};
        const int ib[] = {4, 2, 3, 0, 1, 4, 0, 5, 6, 2};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ranges::make_subrange(ForwardIterator<const int*>(ia),
                                    Sentinel<const int*>(ia + sa)),
                                   ForwardIterator<const int*>(ib),
                                   std::equal_to<int const>()) == true);

        CHECK(ranges::is_permutation(ranges::make_subrange(ForwardIterator<const int*>(ia),
                                    Sentinel<const int*>(ia + sa)),
                                   ranges::make_subrange(ForwardIterator<const int*>(ib),
                                    Sentinel<const int*>(ib + sa)),
                                   std::equal_to<int const>()) == true);
        CHECK(ranges::is_permutation(ranges::make_subrange(ForwardIterator<const int*>(ia),
                                    Sentinel<const int*>(ia + sa)),
                                   ranges::make_subrange(ForwardIterator<const int*>(ib + 1),
                                    Sentinel<const int*>(ib + sa)),
                                   std::equal_to<int const>()) == false);
        CHECK(ranges::is_permutation(ranges::make_subrange(ForwardIterator<const int*>(ia),
                                    Sentinel<const int*>(ia + sa)),
                                   ranges::make_subrange(ForwardIterator<const int*>(ib),
                                    Sentinel<const int*>(ib + sa - 1)),
                                   std::equal_to<int const>()) == false);
    }

    // common_range tests, with sentinels, with predicate and projections:
    {
        const S ia[] = {{0}, {1}, {2}, {3}, {0}, {5}, {6}, {2}, {4}, {4}};
        const T ib[] = {{4}, {2}, {3}, {0}, {1}, {4}, {0}, {5}, {6}, {2}};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ia, &ib[0], std::equal_to<int const>(), &S::i, &T::i) == true);
        CHECK(ranges::is_permutation(ia, ib, std::equal_to<int const>(), &S::i, &T::i) == true);
        CHECK(ranges::is_permutation(ranges::make_subrange(ForwardIterator<const S*>(ia),
                                    Sentinel<const S*>(ia + sa)),
                                   ranges::make_subrange(ForwardIterator<const T*>(ib + 1),
                                    Sentinel<const T*>(ib + sa)),
                                   std::equal_to<int const>(), &S::i, &T::i) == false);
        CHECK(ranges::is_permutation(ranges::make_subrange(ForwardIterator<const S*>(ia),
                                    Sentinel<const S*>(ia + sa)),
                                   ranges::make_subrange(ForwardIterator<const T*>(ib),
                                    Sentinel<const T*>(ib + sa - 1)),
                                   std::equal_to<int const>(), &S::i, &T::i) == false);
    }

    // Iterator tests, with sentinels, with predicate and projections:
    {
        const S ia[] = {{0}, {1}, {2}, {3}, {0}, {5}, {6}, {2}, {4}, {4}};
        const T ib[] = {{4}, {2}, {3}, {0}, {1}, {4}, {0}, {5}, {6}, {2}};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        CHECK(ranges::is_permutation(ForwardIterator<const S*>(ia),
                                    Sentinel<const S*>(ia + sa),
                                   ForwardIterator<const T*>(ib),
                                   std::equal_to<int const>(), &S::i, &T::i) == true);

        CHECK(ranges::is_permutation(ForwardIterator<const S*>(ia),
                                    Sentinel<const S*>(ia + sa),
                                   ForwardIterator<const T*>(ib),
                                    Sentinel<const T*>(ib + sa),
                                   std::equal_to<int const>(), &S::i, &T::i) == true);
        CHECK(ranges::is_permutation(ForwardIterator<const S*>(ia),
                                    Sentinel<const S*>(ia + sa),
                                   ForwardIterator<const T*>(ib + 1),
                                    Sentinel<const T*>(ib + sa),
                                   std::equal_to<int const>(), &S::i, &T::i) == false);
        CHECK(ranges::is_permutation(ForwardIterator<const S*>(ia),
                                    Sentinel<const S*>(ia + sa),
                                   ForwardIterator<const T*>(ib),
                                    Sentinel<const T*>(ib + sa - 1),
                                   std::equal_to<int const>(), &S::i, &T::i) == false);
    }

    return ::test_result();
}
