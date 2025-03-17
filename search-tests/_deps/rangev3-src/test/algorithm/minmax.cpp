// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//  Copyright Casey Carter 2015
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

#include <range/v3/algorithm/minmax.hpp>
#include <range/v3/view/subrange.hpp>
#include <memory>
#include <numeric>
#include <random>
#include <algorithm>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

RANGES_DIAGNOSTIC_IGNORE_GLOBAL_CONSTRUCTORS

namespace
{
    std::mt19937 gen;

    template<class Iter, class Sent = Iter>
    void
    test_iter(Iter first, Sent last)
    {
        RANGES_ENSURE(first != last);
        auto rng = ranges::make_subrange(first, last);
        auto res = ranges::minmax(rng);
        for (Iter i = first; i != last; ++i) {
            CHECK(!(*i < res.min));
            CHECK(!(res.max < *i));
        }
    }

    template<class Iter, class Sent = Iter>
    void
    test_iter(unsigned N)
    {
        RANGES_ENSURE(N > 0);
        std::unique_ptr<int[]> a{new int[N]};
        std::iota(a.get(), a.get()+N, 0);
        std::shuffle(a.get(), a.get()+N, gen);
        test_iter(Iter(a.get()), Sent(a.get()+N));
    }

    template<class Iter, class Sent = Iter>
    void
    test_iter()
    {
        test_iter<Iter, Sent>(1);
        test_iter<Iter, Sent>(2);
        test_iter<Iter, Sent>(3);
        test_iter<Iter, Sent>(10);
        test_iter<Iter, Sent>(1000);
    }

    template<class Iter, class Sent = Iter>
    void
    test_iter_comp(Iter first, Sent last)
    {
        RANGES_ENSURE(first != last);
        typedef std::greater<int> Compare;
        Compare comp;
        auto rng = ranges::make_subrange(first, last);
        auto res = ranges::minmax(rng, comp);
        for (Iter i = first; i != last; ++i) {
            CHECK(!comp(*i, res.min));
            CHECK(!comp(res.max, *i));
        }
    }

    template<class Iter, class Sent = Iter>
    void
    test_iter_comp(unsigned N)
    {
        RANGES_ENSURE(N > 0);
        std::unique_ptr<int[]> a{new int[N]};
        std::iota(a.get(), a.get()+N, 0);
        std::shuffle(a.get(), a.get()+N, gen);
        test_iter_comp(Iter(a.get()), Sent(a.get()+N));
    }

    template<class Iter, class Sent = Iter>
    void
    test_iter_comp()
    {
        test_iter_comp<Iter, Sent>(1);
        test_iter_comp<Iter, Sent>(2);
        test_iter_comp<Iter, Sent>(3);
        test_iter_comp<Iter, Sent>(10);
        test_iter_comp<Iter, Sent>(1000);
    }

    struct S
    {
        int value;
        int index;
    };
}

int main()
{
    test_iter<InputIterator<const int*> >();
    test_iter<ForwardIterator<const int*> >();
    test_iter<BidirectionalIterator<const int*> >();
    test_iter<RandomAccessIterator<const int*> >();
    test_iter<const int*>();
    test_iter<InputIterator<const int*>, Sentinel<const int*>>();
    test_iter<ForwardIterator<const int*>, Sentinel<const int*>>();
    test_iter<BidirectionalIterator<const int*>, Sentinel<const int*>>();
    test_iter<RandomAccessIterator<const int*>, Sentinel<const int*>>();

    test_iter<InputIterator<const int*> >();
    test_iter<ForwardIterator<const int*> >();
    test_iter<BidirectionalIterator<const int*> >();
    test_iter<RandomAccessIterator<const int*> >();
    test_iter<const int*>();
    test_iter<InputIterator<const int*>, Sentinel<const int*>>();
    test_iter<ForwardIterator<const int*>, Sentinel<const int*>>();
    test_iter<BidirectionalIterator<const int*>, Sentinel<const int*>>();
    test_iter<RandomAccessIterator<const int*>, Sentinel<const int*>>();

    test_iter_comp<InputIterator<const int*> >();
    test_iter_comp<ForwardIterator<const int*> >();
    test_iter_comp<BidirectionalIterator<const int*> >();
    test_iter_comp<RandomAccessIterator<const int*> >();
    test_iter_comp<const int*>();
    test_iter_comp<InputIterator<const int*>, Sentinel<const int*>>();
    test_iter_comp<ForwardIterator<const int*>, Sentinel<const int*>>();
    test_iter_comp<BidirectionalIterator<const int*>, Sentinel<const int*>>();
    test_iter_comp<RandomAccessIterator<const int*>, Sentinel<const int*>>();

    test_iter_comp<InputIterator<const int*> >();
    test_iter_comp<ForwardIterator<const int*> >();
    test_iter_comp<BidirectionalIterator<const int*> >();
    test_iter_comp<RandomAccessIterator<const int*> >();
    test_iter_comp<const int*>();
    test_iter_comp<InputIterator<const int*>, Sentinel<const int*>>();
    test_iter_comp<ForwardIterator<const int*>, Sentinel<const int*>>();
    test_iter_comp<BidirectionalIterator<const int*>, Sentinel<const int*>>();
    test_iter_comp<RandomAccessIterator<const int*>, Sentinel<const int*>>();

    // Works with projections?
    S s[] = {S{1,0},S{2,1},S{3,2},S{4,3},S{-4,4},S{40,5},S{-4,6},S{40,7},S{7,8},S{8,9},S{9,10}};
    auto res = ranges::minmax(s, std::less<int>{}, &S::value);
    CHECK(res.min.value == -4);
    CHECK(res.min.index == 4);
    CHECK(res.max.value == 40);
    CHECK(res.max.index == 7);

    // Works with initializer_lists? (Regression test for #1004)
    CHECK(ranges::minmax({4,3,1,2,6,5}) == ranges::minmax_result<int>{1,6});

    return test_result();
}
