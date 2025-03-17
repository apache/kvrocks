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

#include <memory>
#include <utility>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/is_partitioned.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

struct is_odd
{
    constexpr bool operator()(const int & i) const
    {
        return i & 1;
    }
};

template<class Iter, class Sent = Iter>
void
test_iter()
{
    {
        const int ia[] = {1, 2, 3, 4, 5, 6};
        CHECK(!ranges::is_partitioned(Iter(ranges::begin(ia)),
                                      Sent(ranges::end(ia)),
                                      is_odd()));
    }
    {
        const int ia[] = {1, 3, 5, 2, 4, 6};
        CHECK( ranges::is_partitioned(Iter(ranges::begin(ia)),
                                      Sent(ranges::end(ia)),
                                      is_odd()));
    }
    {
        const int ia[] = {2, 4, 6, 1, 3, 5};
        CHECK(!ranges::is_partitioned(Iter(ranges::begin(ia)),
                                      Sent(ranges::end(ia)),
                                      is_odd()));
    }
    {
        const int ia[] = {1, 3, 5, 2, 4, 6, 7};
        CHECK(!ranges::is_partitioned(Iter(ranges::begin(ia)),
                                      Sent(ranges::end(ia)),
                                      is_odd()));
    }
    {
        const int ia[] = {1, 3, 5, 2, 4, 6, 7};
        CHECK( ranges::is_partitioned(Iter(ranges::begin(ia)),
                                      Sent(ranges::begin(ia)),
                                      is_odd()));
    }
}

template<class Iter, class Sent = Iter>
void
test_range()
{
    {
        const int ia[] = {1, 2, 3, 4, 5, 6};
        CHECK(!ranges::is_partitioned(ranges::make_subrange(Iter(ranges::begin(ia)),
                                                    Sent(ranges::end(ia))),
                                      is_odd()));
    }
    {
        const int ia[] = {1, 3, 5, 2, 4, 6};
        CHECK( ranges::is_partitioned(ranges::make_subrange(Iter(ranges::begin(ia)),
                                                    Sent(ranges::end(ia))),
                                      is_odd()));
    }
    {
        const int ia[] = {2, 4, 6, 1, 3, 5};
        CHECK(!ranges::is_partitioned(ranges::make_subrange(Iter(ranges::begin(ia)),
                                                    Sent(ranges::end(ia))),
                                      is_odd()));
    }
    {
        const int ia[] = {1, 3, 5, 2, 4, 6, 7};
        CHECK(!ranges::is_partitioned(ranges::make_subrange(Iter(ranges::begin(ia)),
                                                    Sent(ranges::end(ia))),
                                      is_odd()));
    }
    {
        const int ia[] = {1, 3, 5, 2, 4, 6, 7};
        CHECK( ranges::is_partitioned(ranges::make_subrange(Iter(ranges::begin(ia)),
                                                    Sent(ranges::begin(ia))),
                                      is_odd()));
    }
}

struct S
{
    int i;
};

int main()
{
    test_iter<InputIterator<const int*> >();
    test_iter<InputIterator<const int*>, Sentinel<const int*>>();

    test_range<InputIterator<const int*> >();
    test_range<InputIterator<const int*>, Sentinel<const int*>>();

    // Test projections
    const S ia[] = {S{1}, S{3}, S{5}, S{2}, S{4}, S{6}};
    CHECK( ranges::is_partitioned(ia, is_odd(), &S::i) );

    {
        using IL = std::initializer_list<int>;
        STATIC_CHECK(ranges::is_partitioned(IL{1, 3, 5, 2, 4, 6}, is_odd()));
        STATIC_CHECK(!ranges::is_partitioned(IL{1, 3, 1, 2, 5, 6}, is_odd()));
    }

    return ::test_result();
}
