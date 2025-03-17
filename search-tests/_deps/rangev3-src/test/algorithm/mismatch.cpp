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

#include <memory>
#include <algorithm>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/mismatch.hpp>

#include "../array.hpp"
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

RANGES_DIAGNOSTIC_IGNORE_DEPRECATED_DECLARATIONS

template<typename Iter, typename Sent = Iter>
void test_iter()
{
    int ia[] = {0, 1, 2, 2, 0, 1, 2, 3};
    constexpr auto sa = ranges::size(ia);
    int ib[] = {0, 1, 2, 3, 0, 1, 2, 3};
    using Res = ranges::mismatch_result<Iter, Iter>;
    CHECK(ranges::mismatch(Iter(ia), Sent(ia + sa), Iter(ib)) ==
                           Res{Iter(ia+3),Iter(ib+3)});
    CHECK(ranges::mismatch(Iter(ia),Sent(ia + sa),Iter(ib),Sent(ib + sa)) ==
                           Res{Iter(ia+3),Iter(ib+3)});
    CHECK(ranges::mismatch(Iter(ia),Sent(ia + sa),Iter(ib),Sent(ib + 2)) ==
                           Res{Iter(ia+2),Iter(ib+2)});

    CHECK(ranges::mismatch(Iter(ia),Sent(ia + sa),Iter(ib),std::equal_to<int>()) ==
                           Res{Iter(ia+3),Iter(ib+3)});
    CHECK(ranges::mismatch(Iter(ia),Sent(ia + sa),Iter(ib),Sent(ib + sa),std::equal_to<int>()) ==
                           Res{Iter(ia+3),Iter(ib+3)});
    CHECK(ranges::mismatch(Iter(ia), Sent(ia + sa), Iter(ib), Sent(ib + 2), std::equal_to<int>()) ==
                           Res{Iter(ia+2),Iter(ib+2)});
}

template<typename Iter, typename Sent = Iter>
void test_range()
{
    int ia[] = {0, 1, 2, 2, 0, 1, 2, 3};
    constexpr auto sa = ranges::size(ia);
    int ib[] = {0, 1, 2, 3, 0, 1, 2, 3};
    using Res = ranges::mismatch_result<Iter, Iter>;
    auto rng1 = ::MakeTestRange(Iter(ia), Sent(ia + sa));
    CHECK(ranges::mismatch(rng1, Iter(ib)) ==
                           Res{Iter(ia+3),Iter(ib+3)});
    auto r1 = ranges::mismatch(std::move(rng1), Iter(ib));
    CHECK(::is_dangling(r1.in1));
    CHECK(r1.in2 == Iter(ib+3));
    auto rng2 = ::MakeTestRange(Iter(ia),Sent(ia + sa));
    auto rng3 = ::MakeTestRange(Iter(ib),Sent(ib + sa));
    CHECK(ranges::mismatch(rng2,rng3) ==
                           Res{Iter(ia+3),Iter(ib+3)});
    auto r2 = ranges::mismatch(std::move(rng2), std::move(rng3));
    CHECK(::is_dangling(r2.in1));
    CHECK(::is_dangling(r2.in2));
    auto r3 = ranges::mismatch(rng2, std::move(rng3));
    CHECK(r3.in1 == Iter(ia+3));
    CHECK(::is_dangling(r3.in2));
    auto r4 = ranges::mismatch(std::move(rng2), rng3);
    CHECK(::is_dangling(r4.in1));
    CHECK(r4.in2 == Iter(ib+3));
    auto rng4 = ::MakeTestRange(Iter(ia),Sent(ia + sa));
    auto rng5 = ::MakeTestRange(Iter(ib),Sent(ib + 2));
    CHECK(ranges::mismatch(rng4,rng5) ==
                           Res{Iter(ia+2),Iter(ib+2)});

    auto rng6 = ::MakeTestRange(Iter(ia),Sent(ia + sa));
    CHECK(ranges::mismatch(rng6,Iter(ib),std::equal_to<int>()) ==
                           Res{Iter(ia+3),Iter(ib+3)});
    auto rng7 = ::MakeTestRange(Iter(ia),Sent(ia + sa));
    auto rng8 = ::MakeTestRange(Iter(ib),Sent(ib + sa));
    CHECK(ranges::mismatch(rng7,rng8,std::equal_to<int>()) ==
                           Res{Iter(ia+3),Iter(ib+3)});
    auto rng9 = ::MakeTestRange(Iter(ia), Sent(ia + sa));
    auto rng10 = ::MakeTestRange(Iter(ib), Sent(ib + 2));
    CHECK(ranges::mismatch(rng9,rng10,std::equal_to<int>()) ==
                           Res{Iter(ia+2),Iter(ib+2)});
}

struct S
{
    int i;
};

int main()
{
    test_iter<InputIterator<const int*>>();
    test_iter<ForwardIterator<const int*>>();
    test_iter<BidirectionalIterator<const int*>>();
    test_iter<RandomAccessIterator<const int*>>();
    test_iter<const int*>();
    test_iter<InputIterator<const int*>, Sentinel<const int*>>();
    test_iter<ForwardIterator<const int*>, Sentinel<const int*>>();
    test_iter<BidirectionalIterator<const int*>, Sentinel<const int*>>();
    test_iter<RandomAccessIterator<const int*>, Sentinel<const int*>>();

    test_range<InputIterator<const int*>>();
    test_range<ForwardIterator<const int*>>();
    test_range<BidirectionalIterator<const int*>>();
    test_range<RandomAccessIterator<const int*>>();
    test_range<const int*>();
    test_range<InputIterator<const int*>, Sentinel<const int*>>();
    test_range<ForwardIterator<const int*>, Sentinel<const int*>>();
    test_range<BidirectionalIterator<const int*>, Sentinel<const int*>>();
    test_range<RandomAccessIterator<const int*>, Sentinel<const int*>>();

    // Works with projections?
    S s1[] = {S{1},S{2},S{3},S{4},S{-4},S{5},S{6},S{40},S{7},S{8},S{9}};
    int const i1[] = {1,2,3,4,5,6,7,8,9};
    ranges::mismatch_result<S const *, int const *> ps1
        = ranges::mismatch(s1, i1, std::equal_to<int>(), &S::i);
    CHECK(ps1.in1->i == -4);
    CHECK(*ps1.in2 == 5);

    S s2[] = {S{1},S{2},S{3},S{4},S{5},S{6},S{40},S{7},S{8},S{9}};
    ranges::mismatch_result<S const *, S const *> ps2
        = ranges::mismatch(s1, s2, std::equal_to<int>(), &S::i, &S::i);
    CHECK(ps2.in1->i == -4);
    CHECK(ps2.in2->i == 5);

    constexpr auto r1 = test::array<int, 11>{{1, 2, 3, 4, -4, 5, 6, 40, 7, 8, 9}};
    constexpr auto r11 = test::array<int, 9>{{1, 2, 3, 4, 5, 6, 7, 8, 9}};
    constexpr auto r2 = test::array<int, 10>{{1, 2, 3, 4, 5, 6, 40, 7, 8, 9}};
    STATIC_CHECK(*ranges::mismatch(r1, r11, std::equal_to<int>{}).in1 == -4);
    STATIC_CHECK(*ranges::mismatch(r1, r11, std::equal_to<int>{}).in2 == 5);
    STATIC_CHECK(*ranges::mismatch(r1, r2, std::equal_to<int>{}).in1 == -4);
    STATIC_CHECK(*ranges::mismatch(r1, r2, std::equal_to<int>{}).in2 == 5);

    return test_result();
}
