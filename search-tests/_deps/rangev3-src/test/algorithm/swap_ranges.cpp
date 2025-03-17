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

//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#include <array>
#include <memory>
#include <algorithm>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/swap_ranges.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

template<class Iter1, class Iter2>
void test_iter_3()
{
    int i[3] = {1, 2, 3};
    int j[3] = {4, 5, 6};
    ranges::swap_ranges_result<Iter1, Iter2> r =
        ranges::swap_ranges(Iter1(i), Iter1(i+3), Iter2(j));
    CHECK(base(r.in1) == i+3);
    CHECK(base(r.in2) == j+3);
    CHECK(i[0] == 4);
    CHECK(i[1] == 5);
    CHECK(i[2] == 6);
    CHECK(j[0] == 1);
    CHECK(j[1] == 2);
    CHECK(j[2] == 3);

    using Sent1 = typename sentinel_type<Iter1>::type;
    r = ranges::swap_ranges(Iter1(j), Sent1(j+3), Iter2(i));
    CHECK(base(r.in1) == j+3);
    CHECK(base(r.in2) == i+3);
    CHECK(i[0] == 1);
    CHECK(i[1] == 2);
    CHECK(i[2] == 3);
    CHECK(j[0] == 4);
    CHECK(j[1] == 5);
    CHECK(j[2] == 6);
}

template<class Iter1, class Iter2>
void test_iter_4()
{
    int i[3] = {1, 2, 3};
    int j[4] = {4, 5, 6, 7};
    ranges::swap_ranges_result<Iter1, Iter2> r =
        ranges::swap_ranges(Iter1(i), Iter1(i+3), Iter2(j), Iter2(j+4));
    CHECK(base(r.in1) == i+3);
    CHECK(base(r.in2) == j+3);
    CHECK(i[0] == 4);
    CHECK(i[1] == 5);
    CHECK(i[2] == 6);
    CHECK(j[0] == 1);
    CHECK(j[1] == 2);
    CHECK(j[2] == 3);
    CHECK(j[3] == 7);

    using Sent1 = typename sentinel_type<Iter1>::type;
    using Sent2 = typename sentinel_type<Iter2>::type;
    r = ranges::swap_ranges(Iter1(j), Sent1(j+4), Iter2(i), Sent2(i+3));
    CHECK(base(r.in1) == j+3);
    CHECK(base(r.in2) == i+3);
    CHECK(i[0] == 1);
    CHECK(i[1] == 2);
    CHECK(i[2] == 3);
    CHECK(j[0] == 4);
    CHECK(j[1] == 5);
    CHECK(j[2] == 6);
    CHECK(j[3] == 7);
}

template<class Iter1, class Iter2>
void test_rng_3()
{
    int i[3] = {1, 2, 3};
    int j[3] = {4, 5, 6};
    ranges::swap_ranges_result<Iter1, Iter2> r =
        ranges::swap_ranges(as_lvalue(ranges::make_subrange(Iter1(i), Iter1(i+3))), Iter2(j));
    CHECK(base(r.in1) == i+3);
    CHECK(base(r.in2) == j+3);
    CHECK(i[0] == 4);
    CHECK(i[1] == 5);
    CHECK(i[2] == 6);
    CHECK(j[0] == 1);
    CHECK(j[1] == 2);
    CHECK(j[2] == 3);

    using Sent1 = typename sentinel_type<Iter1>::type;
    r = ranges::swap_ranges(as_lvalue(ranges::make_subrange(Iter1(j), Sent1(j+3))), Iter2(i));
    CHECK(base(r.in1) == j+3);
    CHECK(base(r.in2) == i+3);
    CHECK(i[0] == 1);
    CHECK(i[1] == 2);
    CHECK(i[2] == 3);
    CHECK(j[0] == 4);
    CHECK(j[1] == 5);
    CHECK(j[2] == 6);
}

template<class Iter1, class Iter2>
void test_rng_4()
{
    int i[3] = {1, 2, 3};
    int j[4] = {4, 5, 6, 7};
    ranges::swap_ranges_result<Iter1, Iter2> r = ranges::swap_ranges(
        as_lvalue(ranges::make_subrange(Iter1(i), Iter1(i+3))),
        as_lvalue(ranges::make_subrange(Iter2(j), Iter2(j+4))));
    CHECK(base(r.in1) == i+3);
    CHECK(base(r.in2) == j+3);
    CHECK(i[0] == 4);
    CHECK(i[1] == 5);
    CHECK(i[2] == 6);
    CHECK(j[0] == 1);
    CHECK(j[1] == 2);
    CHECK(j[2] == 3);
    CHECK(j[3] == 7);

    using Sent1 = typename sentinel_type<Iter1>::type;
    using Sent2 = typename sentinel_type<Iter2>::type;
    r = ranges::swap_ranges(
        as_lvalue(ranges::make_subrange(Iter1(j), Sent1(j+4))),
        as_lvalue(ranges::make_subrange(Iter2(i), Sent2(i+3))));
    CHECK(base(r.in1) == j+3);
    CHECK(base(r.in2) == i+3);
    CHECK(i[0] == 1);
    CHECK(i[1] == 2);
    CHECK(i[2] == 3);
    CHECK(j[0] == 4);
    CHECK(j[1] == 5);
    CHECK(j[2] == 6);
    CHECK(j[3] == 7);

    auto r2 = ranges::swap_ranges(
        ranges::make_subrange(Iter1(j), Sent1(j+4)),
        ranges::make_subrange(Iter2(i), Sent2(i+3)));
    CHECK(base(r2.in1) == j+3);
    CHECK(base(r2.in2) == i+3);
    CHECK(i[0] == 4);
    CHECK(i[1] == 5);
    CHECK(i[2] == 6);
    CHECK(j[0] == 1);
    CHECK(j[1] == 2);
    CHECK(j[2] == 3);
    CHECK(j[3] == 7);
}

template<class Iter1, class Iter2>
void test_move_only()
{
    std::unique_ptr<int> i[3];
    for (int k = 0; k < 3; ++k)
        i[k].reset(new int(k+1));
    std::unique_ptr<int> j[3];
    for (int k = 0; k < 3; ++k)
        j[k].reset(new int(k+4));
    ranges::swap_ranges_result<Iter1, Iter2> r =
        ranges::swap_ranges(Iter1(i), Iter1(i+3), Iter2(j));
    CHECK(base(r.in1) == i+3);
    CHECK(base(r.in2) == j+3);
    CHECK(*i[0] == 4);
    CHECK(*i[1] == 5);
    CHECK(*i[2] == 6);
    CHECK(*j[0] == 1);
    CHECK(*j[1] == 2);
    CHECK(*j[2] == 3);
}

template<class Iter1, class Iter2>
void test()
{
    test_iter_3<Iter1, Iter2>();
    test_iter_4<Iter1, Iter2>();
    test_rng_3<Iter1, Iter2>();
    test_rng_4<Iter1, Iter2>();
}

constexpr bool test_constexpr()
{
    using namespace ranges;
    int i[3] = {1, 2, 3};
    int j[3] = {4, 5, 6};
    const auto r = ranges::swap_ranges(i, j);
    STATIC_CHECK_RETURN(r.in1 == i + 3);
    STATIC_CHECK_RETURN(r.in2 == j + 3);
    STATIC_CHECK_RETURN(i[0] == 4);
    STATIC_CHECK_RETURN(i[1] == 5);
    STATIC_CHECK_RETURN(i[2] == 6);
    STATIC_CHECK_RETURN(j[0] == 1);
    STATIC_CHECK_RETURN(j[1] == 2);
    STATIC_CHECK_RETURN(j[2] == 3);
    return true;
}

int main()
{
    test<ForwardIterator<int*>, ForwardIterator<int*> >();
    test<ForwardIterator<int*>, BidirectionalIterator<int*> >();
    test<ForwardIterator<int*>, RandomAccessIterator<int*> >();
    test<ForwardIterator<int*>, int*>();

    test<BidirectionalIterator<int*>, ForwardIterator<int*> >();
    test<BidirectionalIterator<int*>, BidirectionalIterator<int*> >();
    test<BidirectionalIterator<int*>, RandomAccessIterator<int*> >();
    test<BidirectionalIterator<int*>, int*>();

    test<RandomAccessIterator<int*>, ForwardIterator<int*> >();
    test<RandomAccessIterator<int*>, BidirectionalIterator<int*> >();
    test<RandomAccessIterator<int*>, RandomAccessIterator<int*> >();
    test<RandomAccessIterator<int*>, int*>();

    test<int*, ForwardIterator<int*> >();
    test<int*, BidirectionalIterator<int*> >();
    test<int*, RandomAccessIterator<int*> >();
    test<int*, int*>();

    test_move_only<ForwardIterator<std::unique_ptr<int>*>, ForwardIterator<std::unique_ptr<int>*> >();
    test_move_only<ForwardIterator<std::unique_ptr<int>*>, BidirectionalIterator<std::unique_ptr<int>*> >();
    test_move_only<ForwardIterator<std::unique_ptr<int>*>, RandomAccessIterator<std::unique_ptr<int>*> >();
    test_move_only<ForwardIterator<std::unique_ptr<int>*>, std::unique_ptr<int>*>();

    test_move_only<BidirectionalIterator<std::unique_ptr<int>*>, ForwardIterator<std::unique_ptr<int>*> >();
    test_move_only<BidirectionalIterator<std::unique_ptr<int>*>, BidirectionalIterator<std::unique_ptr<int>*> >();
    test_move_only<BidirectionalIterator<std::unique_ptr<int>*>, RandomAccessIterator<std::unique_ptr<int>*> >();
    test_move_only<BidirectionalIterator<std::unique_ptr<int>*>, std::unique_ptr<int>*>();

    test_move_only<RandomAccessIterator<std::unique_ptr<int>*>, ForwardIterator<std::unique_ptr<int>*> >();
    test_move_only<RandomAccessIterator<std::unique_ptr<int>*>, BidirectionalIterator<std::unique_ptr<int>*> >();
    test_move_only<RandomAccessIterator<std::unique_ptr<int>*>, RandomAccessIterator<std::unique_ptr<int>*> >();
    test_move_only<RandomAccessIterator<std::unique_ptr<int>*>, std::unique_ptr<int>*>();

    test_move_only<std::unique_ptr<int>*, ForwardIterator<std::unique_ptr<int>*> >();
    test_move_only<std::unique_ptr<int>*, BidirectionalIterator<std::unique_ptr<int>*> >();
    test_move_only<std::unique_ptr<int>*, RandomAccessIterator<std::unique_ptr<int>*> >();
    test_move_only<std::unique_ptr<int>*, std::unique_ptr<int>*>();

    {
        int a[4] = {1, 2, 3, 4};
        int b[4] = {5, 6, 7, 8};
        ranges::swap_ranges(a, a + 4, b);
        ::check_equal(a, {5, 6, 7, 8});
        ::check_equal(b, {1, 2, 3, 4});
        ranges::swap_ranges(std::array<int, 2>{{3,4}}, a+2);
        ::check_equal(a, {5, 6, 3, 4});
    }

    {
        STATIC_CHECK(test_constexpr());
    }
    
    return ::test_result();
}
