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
#include <numeric>
#include <random>
#include <algorithm>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/minmax_element.hpp>

#include "../array.hpp"
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
        ranges::minmax_element_result<Iter> p = ranges::minmax_element(first, last);
        if (first != last)
        {
            for (Iter j = first; j != last; ++j)
            {
                CHECK(!(*j < *p.min));
                CHECK(!(*p.max < *j));
            }
        }
        else
        {
            CHECK(p.min == last);
            CHECK(p.max == last);
        }

        auto rng = ::MakeTestRange(first, last);
        p = ranges::minmax_element(rng);
        if (first != last)
        {
            for (Iter j = first; j != last; ++j)
            {
                CHECK(!(*j < *p.min));
                CHECK(!(*p.max < *j));
            }
        }
        else
        {
            CHECK(p.min == last);
            CHECK(p.max == last);
        }

        auto res = ranges::minmax_element(std::move(rng));
        if (first != last)
        {
            for (Iter j = first; j != last; ++j)
            {
                CHECK(is_dangling(res.min));
                CHECK(!(*p.max < *j));
            }
        }
        else
        {
            CHECK(is_dangling(res.min));
            CHECK(is_dangling(res.max));
        }
    }

    template<class Iter, class Sent = Iter>
    void
    test_iter(unsigned N)
    {
        std::unique_ptr<int[]> a{new int[N]};
        std::iota(a.get(), a.get()+N, 0);
        std::shuffle(a.get(), a.get()+N, gen);
        test_iter(Iter(a.get()), Sent(a.get()+N));
    }

    template<class Iter, class Sent = Iter>
    void
    test_iter()
    {
        test_iter<Iter, Sent>(0);
        test_iter<Iter, Sent>(1);
        test_iter<Iter, Sent>(2);
        test_iter<Iter, Sent>(3);
        test_iter<Iter, Sent>(10);
        test_iter<Iter, Sent>(1000);
        {
            const unsigned N = 100;
            std::unique_ptr<int[]> a{new int[N]};
            std::fill_n(a.get(), N, 5);
            std::shuffle(a.get(), a.get()+N, gen);
            ranges::minmax_element_result<Iter> p = ranges::minmax_element(Iter(a.get()), Sent(a.get()+N));
            CHECK(base(p.min) == a.get());
            CHECK(base(p.max) == a.get()+N-1);
        }
    }

    template<class Iter, class Sent = Iter>
    void
    test_iter_comp(Iter first, Sent last)
    {
        typedef std::greater<int> Compare;
        Compare comp;
        ranges::minmax_element_result<Iter> p = ranges::minmax_element(first, last, comp);
        if (first != last)
        {
            for (Iter j = first; j != last; ++j)
            {
                CHECK(!comp(*j, *p.min));
                CHECK(!comp(*p.max, *j));
            }
        }
        else
        {
            CHECK(p.min == last);
            CHECK(p.max == last);
        }

        auto rng = ::MakeTestRange(first, last);
        p = ranges::minmax_element(rng, comp);
        if (first != last)
        {
            for (Iter j = first; j != last; ++j)
            {
                CHECK(!comp(*j, *p.min));
                CHECK(!comp(*p.max, *j));
            }
        }
        else
        {
            CHECK(p.min == last);
            CHECK(p.max == last);
        }

        auto res = ranges::minmax_element(std::move(rng), comp);
        CHECK(is_dangling(res.min));
        CHECK(is_dangling(res.max));
    }

    template<class Iter, class Sent = Iter>
    void
    test_iter_comp(unsigned N)
    {
        std::unique_ptr<int[]> a{new int[N]};
        std::iota(a.get(), a.get()+N, 0);
        std::shuffle(a.get(), a.get()+N, gen);
        test_iter_comp(Iter(a.get()), Sent(a.get()+N));
    }

    template<class Iter, class Sent = Iter>
    void
    test_iter_comp()
    {
        test_iter_comp<Iter, Sent>(0);
        test_iter_comp<Iter, Sent>(1);
        test_iter_comp<Iter, Sent>(2);
        test_iter_comp<Iter, Sent>(3);
        test_iter_comp<Iter, Sent>(10);
        test_iter_comp<Iter, Sent>(1000);
        {
            const unsigned N = 100;
            std::unique_ptr<int[]> a{new int[N]};
            std::fill_n(a.get(), N, 5);
            std::shuffle(a.get(), a.get()+N, gen);
            typedef std::greater<int> Compare;
            Compare comp;
            ranges::minmax_element_result<Iter> p = ranges::minmax_element(Iter(a.get()), Sent(a.get()+N), comp);
            CHECK(base(p.min) == a.get());
            CHECK(base(p.max) == a.get()+N-1);
        }
    }

    struct S
    {
        int i;
    };
}

int main()
{
    test_iter<ForwardIterator<const int*> >();
    test_iter<BidirectionalIterator<const int*> >();
    test_iter<RandomAccessIterator<const int*> >();
    test_iter<const int*>();
    test_iter<ForwardIterator<const int*>, Sentinel<const int*>>();
    test_iter<BidirectionalIterator<const int*>, Sentinel<const int*>>();
    test_iter<RandomAccessIterator<const int*>, Sentinel<const int*>>();

    test_iter<ForwardIterator<const int*> >();
    test_iter<BidirectionalIterator<const int*> >();
    test_iter<RandomAccessIterator<const int*> >();
    test_iter<const int*>();
    test_iter<ForwardIterator<const int*>, Sentinel<const int*>>();
    test_iter<BidirectionalIterator<const int*>, Sentinel<const int*>>();
    test_iter<RandomAccessIterator<const int*>, Sentinel<const int*>>();

    test_iter_comp<ForwardIterator<const int*> >();
    test_iter_comp<BidirectionalIterator<const int*> >();
    test_iter_comp<RandomAccessIterator<const int*> >();
    test_iter_comp<const int*>();
    test_iter_comp<ForwardIterator<const int*>, Sentinel<const int*>>();
    test_iter_comp<BidirectionalIterator<const int*>, Sentinel<const int*>>();
    test_iter_comp<RandomAccessIterator<const int*>, Sentinel<const int*>>();

    test_iter_comp<ForwardIterator<const int*> >();
    test_iter_comp<BidirectionalIterator<const int*> >();
    test_iter_comp<RandomAccessIterator<const int*> >();
    test_iter_comp<const int*>();
    test_iter_comp<ForwardIterator<const int*>, Sentinel<const int*>>();
    test_iter_comp<BidirectionalIterator<const int*>, Sentinel<const int*>>();
    test_iter_comp<RandomAccessIterator<const int*>, Sentinel<const int*>>();

    // Works with projections?
    S s[] = {S{1},S{2},S{3},S{4},S{-4},S{5},S{6},S{40},S{7},S{8},S{9}};
    ranges::minmax_element_result<S const *> ps = ranges::minmax_element(s, std::less<int>{}, &S::i);
    CHECK(ps.min->i == -4);
    CHECK(ps.max->i == 40);

    {
        constexpr auto a = test::array<int, 10>{{1, 2, 3, 4, -4, 5, 6, 40, 8, 9}};
        STATIC_CHECK(ranges::minmax_element(a).min == ranges::begin(a) + 4);
        STATIC_CHECK(ranges::minmax_element(a).max == ranges::begin(a) + 7);
    }

    return test_result();
}
