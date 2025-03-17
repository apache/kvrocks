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

#include <range/v3/algorithm/min.hpp>
#include <range/v3/view/subrange.hpp>
#include <memory>
#include <random>
#include <numeric>
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
        auto v1 = ranges::min(rng);
        for (Iter i = first; i != last; ++i)
            CHECK(!(*i < v1));
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
        auto rng = ranges::make_subrange(first, last);
        auto v = ranges::min(rng, std::greater<int>());
        for (Iter i = first; i != last; ++i)
            CHECK(!std::greater<int>()(*i, v));
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
        int i;
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
    S s[] = {S{1},S{2},S{3},S{4},S{-4},S{5},S{6},S{7},S{8},S{9}};
    S v = ranges::min(s, std::less<int>{}, &S::i);
    CHECK(v.i == -4);

    // Works with initializer_lists? (Regression test for #1004)
    CHECK(ranges::min({4,3,1,2,6,5}) == 1);

    return test_result();
}
