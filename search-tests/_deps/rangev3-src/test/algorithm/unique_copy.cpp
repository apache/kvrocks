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

// Implementation based on the code in libc++
//   http://http://libcxx.llvm.org/

#include <cstring>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/unique_copy.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

struct count_equal
{
    static unsigned count;
    template<class T>
    bool operator()(const T& x, const T& y)
        {++count; return x == y;}
};

unsigned count_equal::count = 0;

template<class InIter, class OutIter, typename Sent = InIter>
void
test_iter()
{
    const int ia[] = {0};
    const unsigned sa = sizeof(ia)/sizeof(ia[0]);
    int ja[sa] = {-1};
    count_equal::count = 0;
    ranges::unique_copy_result<InIter, OutIter> r = ranges::unique_copy(InIter(ia), Sent(ia+sa), OutIter(ja), count_equal());
    CHECK(base(r.in) == ia + sa);
    CHECK(base(r.out) == ja + sa);
    CHECK(ja[0] == 0);
    CHECK(count_equal::count == sa-1);

    const int ib[] = {0, 1};
    const unsigned sb = sizeof(ib)/sizeof(ib[0]);
    int jb[sb] = {-1};
    count_equal::count = 0;
    r = ranges::unique_copy(InIter(ib), Sent(ib+sb), OutIter(jb), count_equal());
    CHECK(base(r.in) == ib + sb);
    CHECK(base(r.out) == jb + sb);
    CHECK(jb[0] == 0);
    CHECK(jb[1] == 1);
    CHECK(count_equal::count == sb-1);

    const int ic[] = {0, 0};
    const unsigned sc = sizeof(ic)/sizeof(ic[0]);
    int jc[sc] = {-1};
    count_equal::count = 0;
    r = ranges::unique_copy(InIter(ic), Sent(ic+sc), OutIter(jc), count_equal());
    CHECK(base(r.in) == ic + sc);
    CHECK(base(r.out) == jc + 1);
    CHECK(jc[0] == 0);
    CHECK(count_equal::count == sc-1);

    const int id[] = {0, 0, 1};
    const unsigned sd = sizeof(id)/sizeof(id[0]);
    int jd[sd] = {-1};
    count_equal::count = 0;
    r = ranges::unique_copy(InIter(id), Sent(id+sd), OutIter(jd), count_equal());
    CHECK(base(r.in) == id + sd);
    CHECK(base(r.out) == jd + 2);
    CHECK(jd[0] == 0);
    CHECK(jd[1] == 1);
    CHECK(count_equal::count == sd-1);

    const int ie[] = {0, 0, 1, 0};
    const unsigned se = sizeof(ie)/sizeof(ie[0]);
    int je[se] = {-1};
    count_equal::count = 0;
    r = ranges::unique_copy(InIter(ie), Sent(ie+se), OutIter(je), count_equal());
    CHECK(base(r.in) == ie + se);
    CHECK(base(r.out) == je + 3);
    CHECK(je[0] == 0);
    CHECK(je[1] == 1);
    CHECK(je[2] == 0);
    CHECK(count_equal::count == se-1);

    const int ig[] = {0, 0, 1, 1};
    const unsigned sg = sizeof(ig)/sizeof(ig[0]);
    int jg[sg] = {-1};
    count_equal::count = 0;
    r = ranges::unique_copy(InIter(ig), Sent(ig+sg), OutIter(jg), count_equal());
    CHECK(base(r.in) == ig + sg);
    CHECK(base(r.out) == jg + 2);
    CHECK(jg[0] == 0);
    CHECK(jg[1] == 1);
    CHECK(count_equal::count == sg-1);

    const int ih[] = {0, 1, 1};
    const unsigned sh = sizeof(ih)/sizeof(ih[0]);
    int jh[sh] = {-1};
    count_equal::count = 0;
    r = ranges::unique_copy(InIter(ih), Sent(ih+sh), OutIter(jh), count_equal());
    CHECK(base(r.in) == ih + sh);
    CHECK(base(r.out) == jh + 2);
    CHECK(jh[0] == 0);
    CHECK(jh[1] == 1);
    CHECK(count_equal::count == sh-1);

    const int ii[] = {0, 1, 1, 1, 2, 2, 2};
    const unsigned si = sizeof(ii)/sizeof(ii[0]);
    int ji[si] = {-1};
    count_equal::count = 0;
    r = ranges::unique_copy(InIter(ii), Sent(ii+si), OutIter(ji), count_equal());
    CHECK(base(r.in) == ii + si);
    CHECK(base(r.out) == ji + 3);
    CHECK(ji[0] == 0);
    CHECK(ji[1] == 1);
    CHECK(ji[2] == 2);
    CHECK(count_equal::count == si-1);
}

template<class InIter, class OutIter, typename Sent = InIter>
void
test_range()
{
    const int ia[] = {0};
    const unsigned sa = sizeof(ia)/sizeof(ia[0]);
    int ja[sa] = {-1};
    count_equal::count = 0;
    ranges::unique_copy_result<InIter, OutIter> r = ranges::unique_copy(::as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+sa))), OutIter(ja), count_equal());
    CHECK(base(r.in) == ia + sa);
    CHECK(base(r.out) == ja + sa);
    CHECK(ja[0] == 0);
    CHECK(count_equal::count == sa-1);

    const int ib[] = {0, 1};
    const unsigned sb = sizeof(ib)/sizeof(ib[0]);
    int jb[sb] = {-1};
    count_equal::count = 0;
    r = ranges::unique_copy(::as_lvalue(ranges::make_subrange(InIter(ib), Sent(ib+sb))), OutIter(jb), count_equal());
    CHECK(base(r.in) == ib + sb);
    CHECK(base(r.out) == jb + sb);
    CHECK(jb[0] == 0);
    CHECK(jb[1] == 1);
    CHECK(count_equal::count == sb-1);

    const int ic[] = {0, 0};
    const unsigned sc = sizeof(ic)/sizeof(ic[0]);
    int jc[sc] = {-1};
    count_equal::count = 0;
    r = ranges::unique_copy(::as_lvalue(ranges::make_subrange(InIter(ic), Sent(ic+sc))), OutIter(jc), count_equal());
    CHECK(base(r.in) == ic + sc);
    CHECK(base(r.out) == jc + 1);
    CHECK(jc[0] == 0);
    CHECK(count_equal::count == sc-1);

    const int id[] = {0, 0, 1};
    const unsigned sd = sizeof(id)/sizeof(id[0]);
    int jd[sd] = {-1};
    count_equal::count = 0;
    r = ranges::unique_copy(::as_lvalue(ranges::make_subrange(InIter(id), Sent(id+sd))), OutIter(jd), count_equal());
    CHECK(base(r.in) == id + sd);
    CHECK(base(r.out) == jd + 2);
    CHECK(jd[0] == 0);
    CHECK(jd[1] == 1);
    CHECK(count_equal::count == sd-1);

    const int ie[] = {0, 0, 1, 0};
    const unsigned se = sizeof(ie)/sizeof(ie[0]);
    int je[se] = {-1};
    count_equal::count = 0;
    r = ranges::unique_copy(::as_lvalue(ranges::make_subrange(InIter(ie), Sent(ie+se))), OutIter(je), count_equal());
    CHECK(base(r.in) == ie + se);
    CHECK(base(r.out) == je + 3);
    CHECK(je[0] == 0);
    CHECK(je[1] == 1);
    CHECK(je[2] == 0);
    CHECK(count_equal::count == se-1);

    const int ig[] = {0, 0, 1, 1};
    const unsigned sg = sizeof(ig)/sizeof(ig[0]);
    int jg[sg] = {-1};
    count_equal::count = 0;
    r = ranges::unique_copy(::as_lvalue(ranges::make_subrange(InIter(ig), Sent(ig+sg))), OutIter(jg), count_equal());
    CHECK(base(r.in) == ig + sg);
    CHECK(base(r.out) == jg + 2);
    CHECK(jg[0] == 0);
    CHECK(jg[1] == 1);
    CHECK(count_equal::count == sg-1);

    const int ih[] = {0, 1, 1};
    const unsigned sh = sizeof(ih)/sizeof(ih[0]);
    int jh[sh] = {-1};
    count_equal::count = 0;
    r = ranges::unique_copy(::as_lvalue(ranges::make_subrange(InIter(ih), Sent(ih+sh))), OutIter(jh), count_equal());
    CHECK(base(r.in) == ih + sh);
    CHECK(base(r.out) == jh + 2);
    CHECK(jh[0] == 0);
    CHECK(jh[1] == 1);
    CHECK(count_equal::count == sh-1);

    const int ii[] = {0, 1, 1, 1, 2, 2, 2};
    const unsigned si = sizeof(ii)/sizeof(ii[0]);
    int ji[si] = {-1};
    count_equal::count = 0;
    r = ranges::unique_copy(::as_lvalue(ranges::make_subrange(InIter(ii), Sent(ii+si))), OutIter(ji), count_equal());
    CHECK(base(r.in) == ii + si);
    CHECK(base(r.out) == ji + 3);
    CHECK(ji[0] == 0);
    CHECK(ji[1] == 1);
    CHECK(ji[2] == 2);
    CHECK(count_equal::count == si-1);
}

template<class InIter, class OutIter>
void test()
{
    using Sent = typename sentinel_type<InIter>::type;
    test_iter<InIter, OutIter>();
    test_iter<InIter, OutIter, Sent>();

    test_range<InIter, OutIter>();
    test_range<InIter, OutIter, Sent>();
}

struct S
{
    int i,j;
};

bool operator==(S l, S r)
{
    return l.i == r.i && l.j == r.j;
}

constexpr bool test_constexpr()
{
    using namespace ranges;
    int a[] = {0, 1, 1, 1, 2, 2, 2};
    int b[] = {0, 0, 0};
    const unsigned sa = sizeof(a) / sizeof(a[0]);
    const unsigned sb = sizeof(b) / sizeof(b[0]);
    const auto r = unique_copy(a, b);
    STATIC_CHECK_RETURN(r.in == a + sa);
    STATIC_CHECK_RETURN(r.out == b + sb);
    STATIC_CHECK_RETURN(a[0] == 0);
    STATIC_CHECK_RETURN(a[1] == 1);
    STATIC_CHECK_RETURN(a[2] == 1);
    STATIC_CHECK_RETURN(a[3] == 1);
    STATIC_CHECK_RETURN(a[4] == 2);
    STATIC_CHECK_RETURN(a[5] == 2);
    STATIC_CHECK_RETURN(a[6] == 2);
    STATIC_CHECK_RETURN(b[0] == 0);
    STATIC_CHECK_RETURN(b[1] == 1);
    STATIC_CHECK_RETURN(b[2] == 2);
    return true;
}

int main()
{
    test<InputIterator<const int*>, OutputIterator<int*> >();
    test<InputIterator<const int*>, ForwardIterator<int*> >();
    test<InputIterator<const int*>, BidirectionalIterator<int*> >();
    test<InputIterator<const int*>, RandomAccessIterator<int*> >();
    test<InputIterator<const int*>, int*>();

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

    test<const int*, OutputIterator<int*> >();
    test<const int*, ForwardIterator<int*> >();
    test<const int*, BidirectionalIterator<int*> >();
    test<const int*, RandomAccessIterator<int*> >();
    test<const int*, int*>();

    // Test projections:
    {
        S const ia[] = {{1,1},{2,2},{3,3},{3,4},{4,5},{5,6},{5,7},{5,8},{6,9},{7,10}};
        S ib[ranges::size(ia)];
        ranges::unique_copy_result<S const *, S *> r = ranges::unique_copy(ia, ib, ranges::equal_to(), &S::i);
        CHECK(r.in == ranges::end(ia));
        CHECK(r.out == ib + 7);
        check_equal(ranges::make_subrange(ib, ib+7), {S{1,1},S{2,2},S{3,3},S{4,5},S{5,6},S{6,9},S{7,10}});
    }

    // Test rvalue ranges:
    {
        S const ia[] = {{1,1},{2,2},{3,3},{3,4},{4,5},{5,6},{5,7},{5,8},{6,9},{7,10}};
        S ib[ranges::size(ia)];
        auto r = ranges::unique_copy(ranges::views::all(ia), ib, ranges::equal_to(), &S::i);
        CHECK(r.in == ranges::end(ia));
        CHECK(r.out == ib + 7);
        check_equal(ranges::make_subrange(ib, ib+7), {S{1,1},S{2,2},S{3,3},S{4,5},S{5,6},S{6,9},S{7,10}});
    }
#ifndef RANGES_WORKAROUND_MSVC_573728
    {
        S const ia[] = {{1,1},{2,2},{3,3},{3,4},{4,5},{5,6},{5,7},{5,8},{6,9},{7,10}};
        S ib[ranges::size(ia)];
        auto r = ranges::unique_copy(std::move(ia), ib, ranges::equal_to(), &S::i);
        CHECK(::is_dangling(r.in));
        CHECK(r.out == ib + 7);
        check_equal(ranges::make_subrange(ib, ib+7), {S{1,1},S{2,2},S{3,3},S{4,5},S{5,6},S{6,9},S{7,10}});
    }
#endif // RANGES_WORKAROUND_MSVC_573728
    {
        std::vector<S> const ia{{1,1},{2,2},{3,3},{3,4},{4,5},{5,6},{5,7},{5,8},{6,9},{7,10}};
        S ib[10];
        RANGES_ENSURE(ranges::size(ia) == ranges::size(ib));
        auto r = ranges::unique_copy(std::move(ia), ib, ranges::equal_to(), &S::i);
        CHECK(::is_dangling(r.in));
        CHECK(r.out == ib + 7);
        check_equal(ranges::make_subrange(ib, ib+7), {S{1,1},S{2,2},S{3,3},S{4,5},S{5,6},S{6,9},S{7,10}});
    }

    {
        STATIC_CHECK(test_constexpr());
    }
    
    return ::test_result();
}
