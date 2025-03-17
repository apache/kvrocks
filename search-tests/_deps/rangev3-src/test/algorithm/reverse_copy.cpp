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

#include <cstring>
#include <utility>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/reverse_copy.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

template<class Iter, class OutIter, class Sent = Iter>
void test()
{
    using P = ranges::reverse_copy_result<Iter, OutIter>;
    // iterators
    {
        const int ia[] = {0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        int ja[sa] = {-1};
        P p0 = ranges::reverse_copy(Iter(ia), Sent(ia), OutIter(ja));
        ::check_equal(ja, {-1});
        CHECK(p0.in == Iter(ia));
        CHECK(base(p0.out) == ja);
        P p1 = ranges::reverse_copy(Iter(ia), Sent(ia+sa), OutIter(ja));
        ::check_equal(ja, {0});
        CHECK(p1.in == Iter(ia+sa));
        CHECK(base(p1.out) == ja+sa);

        const int ib[] = {0, 1};
        const unsigned sb = sizeof(ib)/sizeof(ib[0]);
        int jb[sb] = {-1};
        P p2 = ranges::reverse_copy(Iter(ib), Sent(ib+sb), OutIter(jb));
        ::check_equal(jb, {1, 0});
        CHECK(p2.in == Iter(ib+sb));
        CHECK(base(p2.out) == jb+sb);

        const int ic[] = {0, 1, 2};
        const unsigned sc = sizeof(ic)/sizeof(ic[0]);
        int jc[sc] = {-1};
        P p3 = ranges::reverse_copy(Iter(ic), Sent(ic+sc), OutIter(jc));
        ::check_equal(jc, {2, 1, 0});
        CHECK(p3.in == Iter(ic+sc));
        CHECK(base(p3.out) == jc+sc);

        const int id[] = {0, 1, 2, 3};
        const unsigned sd = sizeof(id)/sizeof(id[0]);
        int jd[sd] = {-1};
        P p4 = ranges::reverse_copy(Iter(id), Sent(id+sd), OutIter(jd));
        ::check_equal(jd, {3, 2, 1, 0});
        CHECK(p4.in == Iter(id+sd));
        CHECK(base(p4.out) == jd+sd);
    }

    // ranges
    {
        const int ia[] = {0};
        const unsigned sa = sizeof(ia)/sizeof(ia[0]);
        int ja[sa] = {-1};
        P p0 = ranges::reverse_copy(ranges::make_subrange(Iter(ia), Sent(ia)), OutIter(ja));
        ::check_equal(ja, {-1});
        CHECK(p0.in == Iter(ia));
        CHECK(base(p0.out) == ja);
        P p1 = ranges::reverse_copy(ranges::make_subrange(Iter(ia), Sent(ia+sa)), OutIter(ja));
        ::check_equal(ja, {0});
        CHECK(p1.in == Iter(ia+sa));
        CHECK(base(p1.out) == ja+sa);

        const int ib[] = {0, 1};
        const unsigned sb = sizeof(ib)/sizeof(ib[0]);
        int jb[sb] = {-1};
        P p2 = ranges::reverse_copy(ranges::make_subrange(Iter(ib), Sent(ib+sb)), OutIter(jb));
        ::check_equal(jb, {1, 0});
        CHECK(p2.in == Iter(ib+sb));
        CHECK(base(p2.out) == jb+sb);

        const int ic[] = {0, 1, 2};
        const unsigned sc = sizeof(ic)/sizeof(ic[0]);
        int jc[sc] = {-1};
        P p3 = ranges::reverse_copy(ranges::make_subrange(Iter(ic), Sent(ic+sc)), OutIter(jc));
        ::check_equal(jc, {2, 1, 0});
        CHECK(p3.in == Iter(ic+sc));
        CHECK(base(p3.out) == jc+sc);

        const int id[] = {0, 1, 2, 3};
        const unsigned sd = sizeof(id)/sizeof(id[0]);
        int jd[sd] = {-1};
        P p4 = ranges::reverse_copy(ranges::make_subrange(Iter(id), Sent(id+sd)), OutIter(jd));
        ::check_equal(jd, {3, 2, 1, 0});
        CHECK(p4.in == Iter(id+sd));
        CHECK(base(p4.out) == jd+sd);

        // test rvalue ranges
        std::memset(jd, 0, sizeof(jd));
        auto p5 = ranges::reverse_copy(::MakeTestRange(Iter(id), Sent(id+sd)), OutIter(jd));
        ::check_equal(jd, {3, 2, 1, 0});
        CHECK(::is_dangling(p5.in));
        CHECK(base(p4.out) == jd+sd);
    }
}

constexpr bool test_constexpr()
{
    using namespace ranges;
    int ia[] = {0, 1, 2, 3, 4};
    int ib[5] = {0};
    constexpr auto sa = ranges::size(ia);
    const auto r = ranges::reverse_copy(ia, ib);
    STATIC_CHECK_RETURN(r.in == ia + sa);
    STATIC_CHECK_RETURN(r.out == ib + sa);
    STATIC_CHECK_RETURN(ia[0] == 0);
    STATIC_CHECK_RETURN(ia[1] == 1);
    STATIC_CHECK_RETURN(ia[2] == 2);
    STATIC_CHECK_RETURN(ia[3] == 3);
    STATIC_CHECK_RETURN(ia[4] == 4);
    STATIC_CHECK_RETURN(ib[0] == 4);
    STATIC_CHECK_RETURN(ib[1] == 3);
    STATIC_CHECK_RETURN(ib[2] == 2);
    STATIC_CHECK_RETURN(ib[3] == 1);
    STATIC_CHECK_RETURN(ib[4] == 0);

    return true;
}

int main()
{
    test<BidirectionalIterator<const int*>, OutputIterator<int*> >();
    test<BidirectionalIterator<const int*>, ForwardIterator<int*> >();
    test<BidirectionalIterator<const int*>, BidirectionalIterator<int*> >();
    test<BidirectionalIterator<const int*>, RandomAccessIterator<int*> >();
    test<BidirectionalIterator<const int*>, int*>();

    test<RandomAccessIterator<const int*>, OutputIterator<int*> >();
    test<RandomAccessIterator<const int*>, ForwardIterator<int*> >();
    test<RandomAccessIterator<const int*>, BidirectionalIterator<int*> >();
    test<RandomAccessIterator<const int*>, RandomAccessIterator<int*> >();
    test<RandomAccessIterator<const int*>, int*>();

    test<const int*, OutputIterator<int*> >();
    test<const int*, ForwardIterator<int*> >();
    test<const int*, BidirectionalIterator<int*> >();
    test<const int*, RandomAccessIterator<int*> >();
    test<const int*, int*>();

    test<BidirectionalIterator<const int*>, OutputIterator<int*>, Sentinel<const int *> >();
    test<BidirectionalIterator<const int*>, ForwardIterator<int*>, Sentinel<const int *> >();
    test<BidirectionalIterator<const int*>, BidirectionalIterator<int*>, Sentinel<const int *> >();
    test<BidirectionalIterator<const int*>, RandomAccessIterator<int*>, Sentinel<const int *> >();
    test<BidirectionalIterator<const int*>, int*>();

    test<RandomAccessIterator<const int*>, OutputIterator<int*>, Sentinel<const int *> >();
    test<RandomAccessIterator<const int*>, ForwardIterator<int*>, Sentinel<const int *> >();
    test<RandomAccessIterator<const int*>, BidirectionalIterator<int*>, Sentinel<const int *> >();
    test<RandomAccessIterator<const int*>, RandomAccessIterator<int*>, Sentinel<const int *> >();
    test<RandomAccessIterator<const int*>, int*>();

    test<const int*, OutputIterator<int*>, Sentinel<const int *> >();
    test<const int*, ForwardIterator<int*>, Sentinel<const int *> >();
    test<const int*, BidirectionalIterator<int*>, Sentinel<const int *> >();
    test<const int*, RandomAccessIterator<int*>, Sentinel<const int *> >();
    test<const int*, int*>();

    {
        STATIC_CHECK(test_constexpr());
    }

    return ::test_result();
}
