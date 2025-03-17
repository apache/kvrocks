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
#include <range/v3/algorithm/partition_point.hpp>
#include <range/v3/view/counted.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"
#include "../array.hpp"

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
        const int ia[] = {2, 4, 6, 8, 10};
        CHECK(ranges::partition_point(Iter(ranges::begin(ia)),
                                      Sent(ranges::end(ia)),
                                      is_odd()) == Iter(ia));
    }
    {
        const int ia[] = {1, 2, 4, 6, 8};
        CHECK(ranges::partition_point(Iter(ranges::begin(ia)),
                                      Sent(ranges::end(ia)),
                                      is_odd()) == Iter(ia + 1));
    }
    {
        const int ia[] = {1, 3, 2, 4, 6};
        CHECK(ranges::partition_point(Iter(ranges::begin(ia)),
                                      Sent(ranges::end(ia)),
                                      is_odd()) == Iter(ia + 2));
    }
    {
        const int ia[] = {1, 3, 5, 2, 4, 6};
        CHECK(ranges::partition_point(Iter(ranges::begin(ia)),
                                      Sent(ranges::end(ia)),
                                      is_odd()) == Iter(ia + 3));
    }
    {
        const int ia[] = {1, 3, 5, 7, 2, 4};
        CHECK(ranges::partition_point(Iter(ranges::begin(ia)),
                                      Sent(ranges::end(ia)),
                                      is_odd()) == Iter(ia + 4));
    }
    {
        const int ia[] = {1, 3, 5, 7, 9, 2};
        CHECK(ranges::partition_point(Iter(ranges::begin(ia)),
                                      Sent(ranges::end(ia)),
                                      is_odd()) == Iter(ia + 5));
    }
    {
        const int ia[] = {1, 3, 5, 7, 9, 11};
        CHECK(ranges::partition_point(Iter(ranges::begin(ia)),
                                      Sent(ranges::end(ia)),
                                      is_odd()) == Iter(ia + 6));
    }
    {
        const int ia[] = {1, 3, 5, 2, 4, 6, 7};
        CHECK(ranges::partition_point(Iter(ranges::begin(ia)),
                                      Sent(ranges::begin(ia)),
                                      is_odd()) == Iter(ia));
    }
}

template<class Iter, class Sent = Iter>
void
test_range()
{
    {
        const int ia[] = {2, 4, 6, 8, 10};
        CHECK(ranges::partition_point(::as_lvalue(ranges::make_subrange(Iter(ranges::begin(ia)),
                                                                Sent(ranges::end(ia)))),
                                      is_odd()) == Iter(ia));
    }
    {
        const int ia[] = {1, 2, 4, 6, 8};
        CHECK(ranges::partition_point(::as_lvalue(ranges::make_subrange(Iter(ranges::begin(ia)),
                                                                Sent(ranges::end(ia)))),
                                      is_odd()) == Iter(ia + 1));
    }
    {
        const int ia[] = {1, 3, 2, 4, 6};
        CHECK(ranges::partition_point(::as_lvalue(ranges::make_subrange(Iter(ranges::begin(ia)),
                                                                Sent(ranges::end(ia)))),
                                      is_odd()) == Iter(ia + 2));
    }
    {
        const int ia[] = {1, 3, 5, 2, 4, 6};
        CHECK(ranges::partition_point(::as_lvalue(ranges::make_subrange(Iter(ranges::begin(ia)),
                                                                Sent(ranges::end(ia)))),
                                      is_odd()) == Iter(ia + 3));
    }
    {
        const int ia[] = {1, 3, 5, 7, 2, 4};
        CHECK(ranges::partition_point(::as_lvalue(ranges::make_subrange(Iter(ranges::begin(ia)),
                                                                Sent(ranges::end(ia)))),
                                      is_odd()) == Iter(ia + 4));
    }
    {
        const int ia[] = {1, 3, 5, 7, 9, 2};
        CHECK(ranges::partition_point(::as_lvalue(ranges::make_subrange(Iter(ranges::begin(ia)),
                                                                Sent(ranges::end(ia)))),
                                      is_odd()) == Iter(ia + 5));
    }
    {
        const int ia[] = {1, 3, 5, 7, 9, 11};
        CHECK(ranges::partition_point(::as_lvalue(ranges::make_subrange(Iter(ranges::begin(ia)),
                                                                Sent(ranges::end(ia)))),
                                      is_odd()) == Iter(ia + 6));
    }
    {
        const int ia[] = {1, 3, 5, 2, 4, 6, 7};
        CHECK(ranges::partition_point(::as_lvalue(ranges::make_subrange(Iter(ranges::begin(ia)),
                                                                Sent(ranges::begin(ia)))),
                                      is_odd()) == Iter(ia));
    }

    // An rvalue range
    {
        const int ia[] = {1, 3, 5, 7, 9, 2};
        CHECK(ranges::partition_point(ranges::make_subrange(Iter(ranges::begin(ia)),
                                                         Sent(ranges::end(ia))),
                                      is_odd()) == Iter(ia + 5));
    }
}

template<class Iter>
void
test_counted()
{
    {
        const int ia[] = {2, 4, 6, 8, 10};
        CHECK(ranges::partition_point(::as_lvalue(ranges::views::counted(Iter(ranges::begin(ia)),
                                                                        ranges::size(ia))),
                                      is_odd()) == ranges::counted_iterator<Iter>(Iter(ia), ranges::size(ia)));
    }
    {
        const int ia[] = {1, 2, 4, 6, 8};
        CHECK(ranges::partition_point(::as_lvalue(ranges::views::counted(Iter(ranges::begin(ia)),
                                                                        ranges::size(ia))),
                                      is_odd()) == ranges::counted_iterator<Iter>(Iter(ia + 1), ranges::size(ia) - 1));
    }
    {
        const int ia[] = {1, 3, 2, 4, 6};
        CHECK(ranges::partition_point(::as_lvalue(ranges::views::counted(Iter(ranges::begin(ia)),
                                                                        ranges::size(ia))),
                                      is_odd()) == ranges::counted_iterator<Iter>(Iter(ia + 2), ranges::size(ia) - 2));
    }
    {
        const int ia[] = {1, 3, 5, 2, 4, 6};
        CHECK(ranges::partition_point(::as_lvalue(ranges::views::counted(Iter(ranges::begin(ia)),
                                                                        ranges::size(ia))),
                                      is_odd()) == ranges::counted_iterator<Iter>(Iter(ia + 3), ranges::size(ia) - 3));
    }
    {
        const int ia[] = {1, 3, 5, 7, 2, 4};
        CHECK(ranges::partition_point(::as_lvalue(ranges::views::counted(Iter(ranges::begin(ia)),
                                                                        ranges::size(ia))),
                                      is_odd()) == ranges::counted_iterator<Iter>(Iter(ia + 4), ranges::size(ia) - 4));
    }
    {
        const int ia[] = {1, 3, 5, 7, 9, 2};
        CHECK(ranges::partition_point(::as_lvalue(ranges::views::counted(Iter(ranges::begin(ia)),
                                                                        ranges::size(ia))),
                                      is_odd()) == ranges::counted_iterator<Iter>(Iter(ia + 5), ranges::size(ia) - 5));
    }
    {
        const int ia[] = {1, 3, 5, 7, 9, 11};
        CHECK(ranges::partition_point(::as_lvalue(ranges::views::counted(Iter(ranges::begin(ia)),
                                                                        ranges::size(ia))),
                                      is_odd()) == ranges::counted_iterator<Iter>(Iter(ia + 6), ranges::size(ia) - 6));
    }
    {
        const int ia[] = {1, 3, 5, 2, 4, 6, 7};
        CHECK(ranges::partition_point(::as_lvalue(ranges::views::counted(Iter(ranges::begin(ia)),
                                                                        0)),
                                      is_odd()) == ranges::counted_iterator<Iter>(Iter(ia), 0));
    }
}

struct S
{
    int i;
};

int main()
{
    test_iter<ForwardIterator<const int*> >();
    test_iter<ForwardIterator<const int*>, Sentinel<const int*>>();

    test_range<ForwardIterator<const int*> >();
    test_range<ForwardIterator<const int*>, Sentinel<const int*>>();

    test_counted<ForwardIterator<const int*> >();

    // Test projections
    const S ia[] = {S{1}, S{3}, S{5}, S{2}, S{4}, S{6}};
    CHECK(ranges::partition_point(ia, is_odd(), &S::i) == ia + 3);

    {
        constexpr test::array<int, 6> a{{1, 3, 5, 2, 4, 6}};
        STATIC_CHECK(ranges::partition_point(a, is_odd()) == ranges::begin(a) + 3);
    }

    return ::test_result();
}
