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
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/rotate_copy.hpp>
#include "../simple_test.hpp"
#include "../test_iterators.hpp"
#include "../test_utils.hpp"

template<class InIter, class OutIter, typename Sent = InIter>
void test_iter()
{
    int ia[] = {0, 1, 2, 3};
    const unsigned sa = sizeof(ia)/sizeof(ia[0]);
    int ib[sa] = {0};

    ranges::rotate_copy_result<InIter, OutIter> r = ranges::rotate_copy(InIter(ia), InIter(ia), Sent(ia), OutIter(ib));
    CHECK(base(r.in) == ia);
    CHECK(base(r.out) == ib);

    r = ranges::rotate_copy(InIter(ia), InIter(ia), Sent(ia+1), OutIter(ib));
    CHECK(base(r.in) == ia+1);
    CHECK(base(r.out) == ib+1);
    CHECK(ib[0] == 0);

    r = ranges::rotate_copy(InIter(ia), InIter(ia+1), Sent(ia+1), OutIter(ib));
    CHECK(base(r.in) == ia+1);
    CHECK(base(r.out) == ib+1);
    CHECK(ib[0] == 0);

    r = ranges::rotate_copy(InIter(ia), InIter(ia), Sent(ia+2), OutIter(ib));
    CHECK(base(r.in) == ia+2);
    CHECK(base(r.out) == ib+2);
    CHECK(ib[0] == 0);
    CHECK(ib[1] == 1);

    r = ranges::rotate_copy(InIter(ia), InIter(ia+1), Sent(ia+2), OutIter(ib));
    CHECK(base(r.in) == ia+2);
    CHECK(base(r.out) == ib+2);
    CHECK(ib[0] == 1);
    CHECK(ib[1] == 0);

    r = ranges::rotate_copy(InIter(ia), InIter(ia+2), Sent(ia+2), OutIter(ib));
    CHECK(base(r.in) == ia+2);
    CHECK(base(r.out) == ib+2);
    CHECK(ib[0] == 0);
    CHECK(ib[1] == 1);

    r = ranges::rotate_copy(InIter(ia), InIter(ia), Sent(ia+3), OutIter(ib));
    CHECK(base(r.in) == ia+3);
    CHECK(base(r.out) == ib+3);
    CHECK(ib[0] == 0);
    CHECK(ib[1] == 1);
    CHECK(ib[2] == 2);

    r = ranges::rotate_copy(InIter(ia), InIter(ia+1), Sent(ia+3), OutIter(ib));
    CHECK(base(r.in) == ia+3);
    CHECK(base(r.out) == ib+3);
    CHECK(ib[0] == 1);
    CHECK(ib[1] == 2);
    CHECK(ib[2] == 0);

    r = ranges::rotate_copy(InIter(ia), InIter(ia+2), Sent(ia+3), OutIter(ib));
    CHECK(base(r.in) == ia+3);
    CHECK(base(r.out) == ib+3);
    CHECK(ib[0] == 2);
    CHECK(ib[1] == 0);
    CHECK(ib[2] == 1);

    r = ranges::rotate_copy(InIter(ia), InIter(ia+3), Sent(ia+3), OutIter(ib));
    CHECK(base(r.in) == ia+3);
    CHECK(base(r.out) == ib+3);
    CHECK(ib[0] == 0);
    CHECK(ib[1] == 1);
    CHECK(ib[2] == 2);

    r = ranges::rotate_copy(InIter(ia), InIter(ia), Sent(ia+4), OutIter(ib));
    CHECK(base(r.in) == ia+4);
    CHECK(base(r.out) == ib+4);
    CHECK(ib[0] == 0);
    CHECK(ib[1] == 1);
    CHECK(ib[2] == 2);
    CHECK(ib[3] == 3);

    r = ranges::rotate_copy(InIter(ia), InIter(ia+1), Sent(ia+4), OutIter(ib));
    CHECK(base(r.in) == ia+4);
    CHECK(base(r.out) == ib+4);
    CHECK(ib[0] == 1);
    CHECK(ib[1] == 2);
    CHECK(ib[2] == 3);
    CHECK(ib[3] == 0);

    r = ranges::rotate_copy(InIter(ia), InIter(ia+2), Sent(ia+4), OutIter(ib));
    CHECK(base(r.in) == ia+4);
    CHECK(base(r.out) == ib+4);
    CHECK(ib[0] == 2);
    CHECK(ib[1] == 3);
    CHECK(ib[2] == 0);
    CHECK(ib[3] == 1);

    r = ranges::rotate_copy(InIter(ia), InIter(ia+3), Sent(ia+4), OutIter(ib));
    CHECK(base(r.in) == ia+4);
    CHECK(base(r.out) == ib+4);
    CHECK(ib[0] == 3);
    CHECK(ib[1] == 0);
    CHECK(ib[2] == 1);
    CHECK(ib[3] == 2);

    r = ranges::rotate_copy(InIter(ia), InIter(ia+4), Sent(ia+4), OutIter(ib));
    CHECK(base(r.in) == ia+4);
    CHECK(base(r.out) == ib+4);
    CHECK(ib[0] == 0);
    CHECK(ib[1] == 1);
    CHECK(ib[2] == 2);
    CHECK(ib[3] == 3);
}

template<class InIter, class OutIter, typename Sent = InIter>
void test_rng()
{
    int ia[] = {0, 1, 2, 3};
    const unsigned sa = sizeof(ia)/sizeof(ia[0]);
    int ib[sa] = {0};

    ranges::rotate_copy_result<InIter, OutIter> r = ranges::rotate_copy(::as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia))), InIter(ia), OutIter(ib));
    CHECK(base(r.in) == ia);
    CHECK(base(r.out) == ib);

    r = ranges::rotate_copy(::as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+1))), InIter(ia), OutIter(ib));
    CHECK(base(r.in) == ia+1);
    CHECK(base(r.out) == ib+1);
    CHECK(ib[0] == 0);

    r = ranges::rotate_copy(::as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+1))), InIter(ia+1), OutIter(ib));
    CHECK(base(r.in) == ia+1);
    CHECK(base(r.out) == ib+1);
    CHECK(ib[0] == 0);

    r = ranges::rotate_copy(::as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+2))), InIter(ia), OutIter(ib));
    CHECK(base(r.in) == ia+2);
    CHECK(base(r.out) == ib+2);
    CHECK(ib[0] == 0);
    CHECK(ib[1] == 1);

    r = ranges::rotate_copy(::as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+2))), InIter(ia+1), OutIter(ib));
    CHECK(base(r.in) == ia+2);
    CHECK(base(r.out) == ib+2);
    CHECK(ib[0] == 1);
    CHECK(ib[1] == 0);

    r = ranges::rotate_copy(::as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+2))), InIter(ia+2), OutIter(ib));
    CHECK(base(r.in) == ia+2);
    CHECK(base(r.out) == ib+2);
    CHECK(ib[0] == 0);
    CHECK(ib[1] == 1);

    r = ranges::rotate_copy(::as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+3))), InIter(ia), OutIter(ib));
    CHECK(base(r.in) == ia+3);
    CHECK(base(r.out) == ib+3);
    CHECK(ib[0] == 0);
    CHECK(ib[1] == 1);
    CHECK(ib[2] == 2);

    r = ranges::rotate_copy(::as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+3))), InIter(ia+1), OutIter(ib));
    CHECK(base(r.in) == ia+3);
    CHECK(base(r.out) == ib+3);
    CHECK(ib[0] == 1);
    CHECK(ib[1] == 2);
    CHECK(ib[2] == 0);

    r = ranges::rotate_copy(::as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+3))), InIter(ia+2), OutIter(ib));
    CHECK(base(r.in) == ia+3);
    CHECK(base(r.out) == ib+3);
    CHECK(ib[0] == 2);
    CHECK(ib[1] == 0);
    CHECK(ib[2] == 1);

    r = ranges::rotate_copy(::as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+3))), InIter(ia+3), OutIter(ib));
    CHECK(base(r.in) == ia+3);
    CHECK(base(r.out) == ib+3);
    CHECK(ib[0] == 0);
    CHECK(ib[1] == 1);
    CHECK(ib[2] == 2);

    r = ranges::rotate_copy(::as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+4))), InIter(ia), OutIter(ib));
    CHECK(base(r.in) == ia+4);
    CHECK(base(r.out) == ib+4);
    CHECK(ib[0] == 0);
    CHECK(ib[1] == 1);
    CHECK(ib[2] == 2);
    CHECK(ib[3] == 3);

    r = ranges::rotate_copy(::as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+4))), InIter(ia+1), OutIter(ib));
    CHECK(base(r.in) == ia+4);
    CHECK(base(r.out) == ib+4);
    CHECK(ib[0] == 1);
    CHECK(ib[1] == 2);
    CHECK(ib[2] == 3);
    CHECK(ib[3] == 0);

    r = ranges::rotate_copy(::as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+4))), InIter(ia+2), OutIter(ib));
    CHECK(base(r.in) == ia+4);
    CHECK(base(r.out) == ib+4);
    CHECK(ib[0] == 2);
    CHECK(ib[1] == 3);
    CHECK(ib[2] == 0);
    CHECK(ib[3] == 1);

    r = ranges::rotate_copy(::as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+4))), InIter(ia+3), OutIter(ib));
    CHECK(base(r.in) == ia+4);
    CHECK(base(r.out) == ib+4);
    CHECK(ib[0] == 3);
    CHECK(ib[1] == 0);
    CHECK(ib[2] == 1);
    CHECK(ib[3] == 2);

    r = ranges::rotate_copy(::as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+4))), InIter(ia+4), OutIter(ib));
    CHECK(base(r.in) == ia+4);
    CHECK(base(r.out) == ib+4);
    CHECK(ib[0] == 0);
    CHECK(ib[1] == 1);
    CHECK(ib[2] == 2);
    CHECK(ib[3] == 3);
}

template<class InIter, class OutIter, typename Sent = InIter>
void test()
{
    test_iter<InIter, OutIter, Sent>();
    test_rng<InIter, OutIter, Sent>();
}

struct S
{
    int i;
};

constexpr bool test_constexpr()
{
    using namespace ranges;
    int rgi[] = {0, 1, 2, 3, 4, 5};
    int rgo[6] = {0};
    const auto r = ranges::rotate_copy(rgi, rgi + 2, rgo);
    STATIC_CHECK_RETURN(r.in == ranges::end(rgi));
    STATIC_CHECK_RETURN(r.out == ranges::end(rgo));
    STATIC_CHECK_RETURN(rgo[0] == 2);
    STATIC_CHECK_RETURN(rgo[1] == 3);
    STATIC_CHECK_RETURN(rgo[2] == 4);
    STATIC_CHECK_RETURN(rgo[3] == 5);
    STATIC_CHECK_RETURN(rgo[4] == 0);
    STATIC_CHECK_RETURN(rgo[5] == 1);

    return true;
}

int main()
{
    test<ForwardIterator<const int*>, OutputIterator<int*> >();
    test<ForwardIterator<const int*>, ForwardIterator<int*> >();
    test<ForwardIterator<const int*>, BidirectionalIterator<int*> >();
    test<ForwardIterator<const int*>, RandomAccessIterator<int*> >();
    test<ForwardIterator<const int*>, int*>();

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

    test<ForwardIterator<const int*>, OutputIterator<int*>, Sentinel<const int*> >();
    test<ForwardIterator<const int*>, ForwardIterator<int*>, Sentinel<const int*> >();
    test<ForwardIterator<const int*>, BidirectionalIterator<int*>, Sentinel<const int*> >();
    test<ForwardIterator<const int*>, RandomAccessIterator<int*>, Sentinel<const int*> >();
    test<ForwardIterator<const int*>, int*, Sentinel<const int*> >();

    test<BidirectionalIterator<const int*>, OutputIterator<int*>, Sentinel<const int*> >();
    test<BidirectionalIterator<const int*>, ForwardIterator<int*>, Sentinel<const int*> >();
    test<BidirectionalIterator<const int*>, BidirectionalIterator<int*>, Sentinel<const int*> >();
    test<BidirectionalIterator<const int*>, RandomAccessIterator<int*>, Sentinel<const int*> >();
    test<BidirectionalIterator<const int*>, int*, Sentinel<const int*> >();

    test<RandomAccessIterator<const int*>, OutputIterator<int*>, Sentinel<const int*> >();
    test<RandomAccessIterator<const int*>, ForwardIterator<int*>, Sentinel<const int*> >();
    test<RandomAccessIterator<const int*>, BidirectionalIterator<int*>, Sentinel<const int*> >();
    test<RandomAccessIterator<const int*>, RandomAccessIterator<int*>, Sentinel<const int*> >();
    test<RandomAccessIterator<const int*>, int*, Sentinel<const int*> >();

    test<const int*, OutputIterator<int*> >();
    test<const int*, ForwardIterator<int*> >();
    test<const int*, BidirectionalIterator<int*> >();
    test<const int*, RandomAccessIterator<int*> >();
    test<const int*, int*>();

    // test rvalue ranges
    {
        int rgi[] = {0,1,2,3,4,5};
        int rgo[6] = {0};
        auto r = ranges::rotate_copy(std::move(rgi), rgi+2, rgo);
#ifndef RANGES_WORKAROUND_MSVC_573728
        CHECK(::is_dangling(r.in));
#endif // RANGES_WORKAROUND_MSVC_573728
        CHECK(r.out == ranges::end(rgo));
        CHECK(rgo[0] == 2);
        CHECK(rgo[1] == 3);
        CHECK(rgo[2] == 4);
        CHECK(rgo[3] == 5);
        CHECK(rgo[4] == 0);
        CHECK(rgo[5] == 1);
    }

    {
        std::vector<int> rgi{0,1,2,3,4,5};
        int rgo[6] = {0};
        auto r = ranges::rotate_copy(std::move(rgi), rgi.begin()+2, rgo);
        CHECK(::is_dangling(r.in));
        CHECK(r.out == ranges::end(rgo));
        CHECK(rgo[0] == 2);
        CHECK(rgo[1] == 3);
        CHECK(rgo[2] == 4);
        CHECK(rgo[3] == 5);
        CHECK(rgo[4] == 0);
        CHECK(rgo[5] == 1);
    }

    {
        STATIC_CHECK(test_constexpr());
    }

    return ::test_result();
}
