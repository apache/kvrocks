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
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/partition.hpp>

#include "../array.hpp"
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

RANGES_DIAGNOSTIC_IGNORE_SIGN_CONVERSION

namespace
{
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
        // check mixed
        int ia[] = {1, 2, 3, 4, 5, 6, 7, 8 ,9};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        Iter r = ranges::partition(Iter(ia), Sent(ia + sa), is_odd());
        CHECK(base(r) == ia + 5);
        for (int* i = ia; i < base(r); ++i)
            CHECK(is_odd()(*i));
        for (int* i = base(r); i < ia+sa; ++i)
            CHECK(!is_odd()(*i));
        // check empty
        r = ranges::partition(Iter(ia), Sent(ia), is_odd());
        CHECK(base(r) == ia);
        // check all false
        for (unsigned i = 0; i < sa; ++i)
            ia[i] = 2*i;
        r = ranges::partition(Iter(ia), Sent(ia+sa), is_odd());
        CHECK(base(r) == ia);
        // check all true
        for (unsigned i = 0; i < sa; ++i)
            ia[i] = 2*i+1;
        r = ranges::partition(Iter(ia), Sent(ia+sa), is_odd());
        CHECK(base(r) == ia+sa);
        // check all true but last
        for (unsigned i = 0; i < sa; ++i)
            ia[i] = 2*i+1;
        ia[sa-1] = 10;
        r = ranges::partition(Iter(ia), Sent(ia+sa), is_odd());
        CHECK(base(r) == ia+sa-1);
        for (int* i = ia; i < base(r); ++i)
            CHECK(is_odd()(*i));
        for (int* i = base(r); i < ia+sa; ++i)
            CHECK(!is_odd()(*i));
        // check all true but first
        for (unsigned i = 0; i < sa; ++i)
            ia[i] = 2*i+1;
        ia[0] = 10;
        r = ranges::partition(Iter(ia), Sent(ia+sa), is_odd());
        CHECK(base(r) == ia+sa-1);
        for (int* i = ia; i < base(r); ++i)
            CHECK(is_odd()(*i));
        for (int* i = base(r); i < ia+sa; ++i)
            CHECK(!is_odd()(*i));
        // check all false but last
        for (unsigned i = 0; i < sa; ++i)
            ia[i] = 2*i;
        ia[sa-1] = 11;
        r = ranges::partition(Iter(ia), Sent(ia+sa), is_odd());
        CHECK(base(r) == ia+1);
        for (int* i = ia; i < base(r); ++i)
            CHECK(is_odd()(*i));
        for (int* i = base(r); i < ia+sa; ++i)
            CHECK(!is_odd()(*i));
        // check all false but first
        for (unsigned i = 0; i < sa; ++i)
            ia[i] = 2*i;
        ia[0] = 11;
        r = ranges::partition(Iter(ia), Sent(ia+sa), is_odd());
        CHECK(base(r) == ia+1);
        for (int* i = ia; i < base(r); ++i)
            CHECK(is_odd()(*i));
        for (int* i = base(r); i < ia+sa; ++i)
            CHECK(!is_odd()(*i));
    }

    template<class Iter, class Sent = Iter>
    void
    test_range()
    {
        // check mixed
        int ia[] = {1, 2, 3, 4, 5, 6, 7, 8 ,9};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        Iter r = ranges::partition(::as_lvalue(ranges::make_subrange(Iter(ia), Sent(ia + sa))), is_odd());
        CHECK(base(r) == ia + 5);
        for (int* i = ia; i < base(r); ++i)
            CHECK(is_odd()(*i));
        for (int* i = base(r); i < ia+sa; ++i)
            CHECK(!is_odd()(*i));
        // check empty
        r = ranges::partition(::as_lvalue(ranges::make_subrange(Iter(ia), Sent(ia))), is_odd());
        CHECK(base(r) == ia);
        // check all false
        for (unsigned i = 0; i < sa; ++i)
            ia[i] = 2*i;
        r = ranges::partition(::as_lvalue(ranges::make_subrange(Iter(ia), Sent(ia+sa))), is_odd());
        CHECK(base(r) == ia);
        // check all true
        for (unsigned i = 0; i < sa; ++i)
            ia[i] = 2*i+1;
        r = ranges::partition(::as_lvalue(ranges::make_subrange(Iter(ia), Sent(ia+sa))), is_odd());
        CHECK(base(r) == ia+sa);
        // check all true but last
        for (unsigned i = 0; i < sa; ++i)
            ia[i] = 2*i+1;
        ia[sa-1] = 10;
        r = ranges::partition(::as_lvalue(ranges::make_subrange(Iter(ia), Sent(ia+sa))), is_odd());
        CHECK(base(r) == ia+sa-1);
        for (int* i = ia; i < base(r); ++i)
            CHECK(is_odd()(*i));
        for (int* i = base(r); i < ia+sa; ++i)
            CHECK(!is_odd()(*i));
        // check all true but first
        for (unsigned i = 0; i < sa; ++i)
            ia[i] = 2*i+1;
        ia[0] = 10;
        r = ranges::partition(::as_lvalue(ranges::make_subrange(Iter(ia), Sent(ia+sa))), is_odd());
        CHECK(base(r) == ia+sa-1);
        for (int* i = ia; i < base(r); ++i)
            CHECK(is_odd()(*i));
        for (int* i = base(r); i < ia+sa; ++i)
            CHECK(!is_odd()(*i));
        // check all false but last
        for (unsigned i = 0; i < sa; ++i)
            ia[i] = 2*i;
        ia[sa-1] = 11;
        r = ranges::partition(::as_lvalue(ranges::make_subrange(Iter(ia), Sent(ia+sa))), is_odd());
        CHECK(base(r) == ia+1);
        for (int* i = ia; i < base(r); ++i)
            CHECK(is_odd()(*i));
        for (int* i = base(r); i < ia+sa; ++i)
            CHECK(!is_odd()(*i));
        // check all false but first
        for (unsigned i = 0; i < sa; ++i)
            ia[i] = 2*i;
        ia[0] = 11;
        r = ranges::partition(::as_lvalue(ranges::make_subrange(Iter(ia), Sent(ia+sa))), is_odd());
        CHECK(base(r) == ia+1);
        for (int* i = ia; i < base(r); ++i)
            CHECK(is_odd()(*i));
        for (int* i = base(r); i < ia+sa; ++i)
            CHECK(!is_odd()(*i));
    }

    struct S
    {
        int i;
    };
}

constexpr bool test_constexpr()
{
    using namespace ranges;
    test::array<int, 9> ia{{1, 2, 3, 4, 5, 6, 7, 8, 9}};
    int * r = partition(ia, is_odd());
    STATIC_CHECK_RETURN(r == begin(ia) + 5);
    for(int * i = begin(ia); i < r; ++i)
    {
        STATIC_CHECK_RETURN(is_odd()(*i));
    }
    for(int * i = r; i < end(ia); ++i)
    {
        STATIC_CHECK_RETURN(!is_odd()(*i));
    }
    
    // Test rvalue range
    auto r2 = partition(make_subrange(begin(ia), end(ia)), is_odd());
    STATIC_CHECK_RETURN(r2 == begin(ia) + 5);
    for(int * i = begin(ia); i < r2; ++i)
    {
        STATIC_CHECK_RETURN(is_odd()(*i));
    }
    for(int * i = r2; i < end(ia); ++i)
    {
        STATIC_CHECK_RETURN(!is_odd()(*i));
    }
    return true;
}

int main()
{
    test_iter<ForwardIterator<int*> >();
    test_iter<BidirectionalIterator<int*> >();
    test_iter<RandomAccessIterator<int*> >();
    test_iter<int*>();
    test_iter<ForwardIterator<int*>, Sentinel<int*> >();
    test_iter<BidirectionalIterator<int*>, Sentinel<int*> >();
    test_iter<RandomAccessIterator<int*>, Sentinel<int*> >();

    test_range<ForwardIterator<int*> >();
    test_range<BidirectionalIterator<int*> >();
    test_range<RandomAccessIterator<int*> >();
    test_range<int*>();
    test_range<ForwardIterator<int*>, Sentinel<int*> >();
    test_range<BidirectionalIterator<int*>, Sentinel<int*> >();
    test_range<RandomAccessIterator<int*>, Sentinel<int*> >();

    // Test projections
    S ia[] = {S{1}, S{2}, S{3}, S{4}, S{5}, S{6}, S{7}, S{8} ,S{9}};
    const unsigned sa = sizeof(ia)/sizeof(ia[0]);
    S* r = ranges::partition(ia, is_odd(), &S::i);
    CHECK(r == ia + 5);
    for (S* i = ia; i < r; ++i)
        CHECK(is_odd()(i->i));
    for (S* i = r; i < ia+sa; ++i)
        CHECK(!is_odd()(i->i));

    // Test rvalue range
    auto r2 = ranges::partition(std::move(ia), is_odd(), &S::i);
#ifndef RANGES_WORKAROUND_MSVC_573728
    CHECK(::is_dangling(r2));
#endif // RANGES_WORKAROUND_MSVC_573728
    std::vector<S> vec(ranges::begin(ia), ranges::end(ia));
    auto r3 = ranges::partition(std::move(vec), is_odd(), &S::i);
    CHECK(::is_dangling(r3));

    {
        STATIC_CHECK(test_constexpr());
    }

    return ::test_result();
}
