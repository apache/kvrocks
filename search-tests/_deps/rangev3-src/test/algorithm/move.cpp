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
#include <range/v3/algorithm/move.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

template<typename InIter, typename OutIter, typename Sent = InIter>
void
test()
{
    {
        const int N = 1000;
        int ia[N];
        for(int i = 0; i < N; ++i)
            ia[i] = i;
        int ib[N] = {0};

        ranges::move_result<InIter, OutIter> r = ranges::move(InIter(ia), Sent(ia+N), OutIter(ib));
        CHECK(base(r.in) == ia+N);
        CHECK(base(r.out) == ib+N);
        for(int i = 0; i < N; ++i)
            CHECK(ia[i] == ib[i]);
    }

    {
        const int N = 1000;
        int ia[N];
        for(int i = 0; i < N; ++i)
            ia[i] = i;
        int ib[N] = {0};

        ranges::move_result<InIter, OutIter> r = ranges::move(as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+N))), OutIter(ib));
        CHECK(base(r.in) == ia+N);
        CHECK(base(r.out) == ib+N);
        for(int i = 0; i < N; ++i)
            CHECK(ia[i] == ib[i]);
    }
}

template<typename InIter, typename OutIter, typename Sent = InIter>
constexpr bool test_constexpr()
{
    {
        constexpr int N = 1000;
        int ia[N]{1};
        for(int i = 0; i < N; ++i)
            ia[i] = i;
        int ib[N] = {0};

        const auto r = ranges::move(InIter(ia), Sent(ia + N), OutIter(ib));
        if(base(r.in) != ia + N)
        {
            return false;
        }
        if(base(r.out) != ib + N)
        {
            return false;
        }
        for(int i = 0; i < N; ++i)
            if(ia[i] != ib[i])
            {
                return false;
            }
    }

    {
        constexpr int N = 1000;
        int ia[N]{1};
        for(int i = 0; i < N; ++i)
            ia[i] = i;
        int ib[N] = {0};

        const auto r = ranges::move(
            as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia + N))), OutIter(ib));
        if(base(r.in) != ia + N)
        {
            return false;
        }
        if(base(r.out) != ib + N)
        {
            return false;
        }
        for(int i = 0; i < N; ++i)
            if(ia[i] != ib[i])
            {
                return false;
            }
    }

    return true;
}

struct S
{
    std::unique_ptr<int> p;
};

template<typename InIter, typename OutIter, typename Sent = InIter>
void
test1()
{
    {
        const int N = 100;
        std::unique_ptr<int> ia[N];
        for(int i = 0; i < N; ++i)
            ia[i].reset(new int(i));
        std::unique_ptr<int> ib[N];

        ranges::move_result<InIter, OutIter> r = ranges::move(InIter(ia), Sent(ia+N), OutIter(ib));
        CHECK(base(r.in) == ia+N);
        CHECK(base(r.out) == ib+N);
        for(int i = 0; i < N; ++i)
        {
            CHECK(ia[i].get() == nullptr);
            CHECK(*ib[i] == i);
        }
    }

    {
        const int N = 100;
        std::unique_ptr<int> ia[N];
        for(int i = 0; i < N; ++i)
            ia[i].reset(new int(i));
        std::unique_ptr<int> ib[N];

        ranges::move_result<InIter, OutIter> r = ranges::move(as_lvalue(ranges::make_subrange(InIter(ia), Sent(ia+N))), OutIter(ib));
        CHECK(base(r.in) == ia+N);
        CHECK(base(r.out) == ib+N);
        for(int i = 0; i < N; ++i)
        {
            CHECK(ia[i].get() == nullptr);
            CHECK(*ib[i] == i);
        }

        ranges::move(ib, ib+N, ia);

        auto r2 = ranges::move(ranges::make_subrange(InIter(ia), Sent(ia+N)), OutIter(ib));
        CHECK(base(r2.in) == ia+N);
        CHECK(base(r2.out) == ib+N);
        for(int i = 0; i < N; ++i)
        {
            CHECK(ia[i].get() == nullptr);
            CHECK(*ib[i] == i);
        }
    }
}

int main()
{
    test<InputIterator<const int*>, OutputIterator<int*> >();
    test<InputIterator<const int*>, InputIterator<int*> >();
    test<InputIterator<const int*>, ForwardIterator<int*> >();
    test<InputIterator<const int*>, BidirectionalIterator<int*> >();
    test<InputIterator<const int*>, RandomAccessIterator<int*> >();
    test<InputIterator<const int*>, int*>();

    test<ForwardIterator<const int*>, OutputIterator<int*> >();
    test<ForwardIterator<const int*>, InputIterator<int*> >();
    test<ForwardIterator<const int*>, ForwardIterator<int*> >();
    test<ForwardIterator<const int*>, BidirectionalIterator<int*> >();
    test<ForwardIterator<const int*>, RandomAccessIterator<int*> >();
    test<ForwardIterator<const int*>, int*>();

    test<BidirectionalIterator<const int*>, OutputIterator<int*> >();
    test<BidirectionalIterator<const int*>, InputIterator<int*> >();
    test<BidirectionalIterator<const int*>, ForwardIterator<int*> >();
    test<BidirectionalIterator<const int*>, BidirectionalIterator<int*> >();
    test<BidirectionalIterator<const int*>, RandomAccessIterator<int*> >();
    test<BidirectionalIterator<const int*>, int*>();

    test<RandomAccessIterator<const int*>, OutputIterator<int*> >();
    test<RandomAccessIterator<const int*>, InputIterator<int*> >();
    test<RandomAccessIterator<const int*>, ForwardIterator<int*> >();
    test<RandomAccessIterator<const int*>, BidirectionalIterator<int*> >();
    test<RandomAccessIterator<const int*>, RandomAccessIterator<int*> >();
    test<RandomAccessIterator<const int*>, int*>();

    test<const int*, OutputIterator<int*> >();
    test<const int*, InputIterator<int*> >();
    test<const int*, ForwardIterator<int*> >();
    test<const int*, BidirectionalIterator<int*> >();
    test<const int*, RandomAccessIterator<int*> >();
    test<const int*, int*>();

    test<InputIterator<const int*>, OutputIterator<int*>, Sentinel<const int*>>();
    test<InputIterator<const int*>, InputIterator<int*>, Sentinel<const int*> >();
    test<InputIterator<const int*>, ForwardIterator<int*>, Sentinel<const int*> >();
    test<InputIterator<const int*>, BidirectionalIterator<int*>, Sentinel<const int*> >();
    test<InputIterator<const int*>, RandomAccessIterator<int*>, Sentinel<const int*> >();

    test<ForwardIterator<const int*>, OutputIterator<int*>, Sentinel<const int*> >();
    test<ForwardIterator<const int*>, InputIterator<int*>, Sentinel<const int*> >();
    test<ForwardIterator<const int*>, ForwardIterator<int*>, Sentinel<const int*> >();
    test<ForwardIterator<const int*>, BidirectionalIterator<int*>, Sentinel<const int*> >();
    test<ForwardIterator<const int*>, RandomAccessIterator<int*>, Sentinel<const int*> >();

    test<BidirectionalIterator<const int*>, OutputIterator<int*>, Sentinel<const int*> >();
    test<BidirectionalIterator<const int*>, InputIterator<int*>, Sentinel<const int*> >();
    test<BidirectionalIterator<const int*>, ForwardIterator<int*>, Sentinel<const int*> >();
    test<BidirectionalIterator<const int*>, BidirectionalIterator<int*>, Sentinel<const int*> >();
    test<BidirectionalIterator<const int*>, RandomAccessIterator<int*>, Sentinel<const int*> >();

    test<RandomAccessIterator<const int*>, OutputIterator<int*>, Sentinel<const int*> >();
    test<RandomAccessIterator<const int*>, InputIterator<int*>, Sentinel<const int*> >();
    test<RandomAccessIterator<const int*>, ForwardIterator<int*>, Sentinel<const int*> >();
    test<RandomAccessIterator<const int*>, BidirectionalIterator<int*>, Sentinel<const int*> >();
    test<RandomAccessIterator<const int*>, RandomAccessIterator<int*>, Sentinel<const int*> >();

    test1<InputIterator<std::unique_ptr<int>*>, OutputIterator<std::unique_ptr<int>*> >();
    test1<InputIterator<std::unique_ptr<int>*>, InputIterator<std::unique_ptr<int>*> >();
    test1<InputIterator<std::unique_ptr<int>*>, ForwardIterator<std::unique_ptr<int>*> >();
    test1<InputIterator<std::unique_ptr<int>*>, BidirectionalIterator<std::unique_ptr<int>*> >();
    test1<InputIterator<std::unique_ptr<int>*>, RandomAccessIterator<std::unique_ptr<int>*> >();
    test1<InputIterator<std::unique_ptr<int>*>, std::unique_ptr<int>*>();

    test1<ForwardIterator<std::unique_ptr<int>*>, OutputIterator<std::unique_ptr<int>*> >();
    test1<ForwardIterator<std::unique_ptr<int>*>, InputIterator<std::unique_ptr<int>*> >();
    test1<ForwardIterator<std::unique_ptr<int>*>, ForwardIterator<std::unique_ptr<int>*> >();
    test1<ForwardIterator<std::unique_ptr<int>*>, BidirectionalIterator<std::unique_ptr<int>*> >();
    test1<ForwardIterator<std::unique_ptr<int>*>, RandomAccessIterator<std::unique_ptr<int>*> >();
    test1<ForwardIterator<std::unique_ptr<int>*>, std::unique_ptr<int>*>();

    test1<BidirectionalIterator<std::unique_ptr<int>*>, OutputIterator<std::unique_ptr<int>*> >();
    test1<BidirectionalIterator<std::unique_ptr<int>*>, InputIterator<std::unique_ptr<int>*> >();
    test1<BidirectionalIterator<std::unique_ptr<int>*>, ForwardIterator<std::unique_ptr<int>*> >();
    test1<BidirectionalIterator<std::unique_ptr<int>*>, BidirectionalIterator<std::unique_ptr<int>*> >();
    test1<BidirectionalIterator<std::unique_ptr<int>*>, RandomAccessIterator<std::unique_ptr<int>*> >();
    test1<BidirectionalIterator<std::unique_ptr<int>*>, std::unique_ptr<int>*>();

    test1<RandomAccessIterator<std::unique_ptr<int>*>, OutputIterator<std::unique_ptr<int>*> >();
    test1<RandomAccessIterator<std::unique_ptr<int>*>, InputIterator<std::unique_ptr<int>*> >();
    test1<RandomAccessIterator<std::unique_ptr<int>*>, ForwardIterator<std::unique_ptr<int>*> >();
    test1<RandomAccessIterator<std::unique_ptr<int>*>, BidirectionalIterator<std::unique_ptr<int>*> >();
    test1<RandomAccessIterator<std::unique_ptr<int>*>, RandomAccessIterator<std::unique_ptr<int>*> >();
    test1<RandomAccessIterator<std::unique_ptr<int>*>, std::unique_ptr<int>*>();

    test1<std::unique_ptr<int>*, OutputIterator<std::unique_ptr<int>*> >();
    test1<std::unique_ptr<int>*, InputIterator<std::unique_ptr<int>*> >();
    test1<std::unique_ptr<int>*, ForwardIterator<std::unique_ptr<int>*> >();
    test1<std::unique_ptr<int>*, BidirectionalIterator<std::unique_ptr<int>*> >();
    test1<std::unique_ptr<int>*, RandomAccessIterator<std::unique_ptr<int>*> >();
    test1<std::unique_ptr<int>*, std::unique_ptr<int>*>();

    test1<InputIterator<std::unique_ptr<int>*>, OutputIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<InputIterator<std::unique_ptr<int>*>, InputIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<InputIterator<std::unique_ptr<int>*>, ForwardIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<InputIterator<std::unique_ptr<int>*>, BidirectionalIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<InputIterator<std::unique_ptr<int>*>, RandomAccessIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<InputIterator<std::unique_ptr<int>*>, std::unique_ptr<int>*>();

    test1<ForwardIterator<std::unique_ptr<int>*>, OutputIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<ForwardIterator<std::unique_ptr<int>*>, InputIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<ForwardIterator<std::unique_ptr<int>*>, ForwardIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<ForwardIterator<std::unique_ptr<int>*>, BidirectionalIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<ForwardIterator<std::unique_ptr<int>*>, RandomAccessIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<ForwardIterator<std::unique_ptr<int>*>, std::unique_ptr<int>*>();

    test1<BidirectionalIterator<std::unique_ptr<int>*>, OutputIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<BidirectionalIterator<std::unique_ptr<int>*>, InputIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<BidirectionalIterator<std::unique_ptr<int>*>, ForwardIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<BidirectionalIterator<std::unique_ptr<int>*>, BidirectionalIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<BidirectionalIterator<std::unique_ptr<int>*>, RandomAccessIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<BidirectionalIterator<std::unique_ptr<int>*>, std::unique_ptr<int>*>();

    test1<RandomAccessIterator<std::unique_ptr<int>*>, OutputIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<RandomAccessIterator<std::unique_ptr<int>*>, InputIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<RandomAccessIterator<std::unique_ptr<int>*>, ForwardIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<RandomAccessIterator<std::unique_ptr<int>*>, BidirectionalIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();
    test1<RandomAccessIterator<std::unique_ptr<int>*>, RandomAccessIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*> >();

    {
        STATIC_CHECK(test_constexpr<InputIterator<const int *>, OutputIterator<int *>>());
        STATIC_CHECK(test_constexpr<InputIterator<const int *>, InputIterator<int *>>());
        STATIC_CHECK(
            test_constexpr<InputIterator<const int *>, ForwardIterator<int *>>());
        STATIC_CHECK(
            test_constexpr<InputIterator<const int *>, BidirectionalIterator<int *>>());
        STATIC_CHECK(
            test_constexpr<InputIterator<const int *>, RandomAccessIterator<int *>>());
        STATIC_CHECK(test_constexpr<InputIterator<const int *>, int *>());

        STATIC_CHECK(
            test_constexpr<ForwardIterator<const int *>, OutputIterator<int *>>());
        STATIC_CHECK(
            test_constexpr<ForwardIterator<const int *>, InputIterator<int *>>());
        STATIC_CHECK(
            test_constexpr<ForwardIterator<const int *>, ForwardIterator<int *>>());
        STATIC_CHECK(
            test_constexpr<ForwardIterator<const int *>, BidirectionalIterator<int *>>());
        STATIC_CHECK(
            test_constexpr<ForwardIterator<const int *>, RandomAccessIterator<int *>>());
        STATIC_CHECK(test_constexpr<ForwardIterator<const int *>, int *>());

        STATIC_CHECK(
            test_constexpr<BidirectionalIterator<const int *>, OutputIterator<int *>>());
        STATIC_CHECK(
            test_constexpr<BidirectionalIterator<const int *>, InputIterator<int *>>());
        STATIC_CHECK(
            test_constexpr<BidirectionalIterator<const int *>, ForwardIterator<int *>>());
        STATIC_CHECK(test_constexpr<BidirectionalIterator<const int *>,
                                    BidirectionalIterator<int *>>());
        STATIC_CHECK(test_constexpr<BidirectionalIterator<const int *>,
                                    RandomAccessIterator<int *>>());
        STATIC_CHECK(test_constexpr<BidirectionalIterator<const int *>, int *>());

        STATIC_CHECK(
            test_constexpr<RandomAccessIterator<const int *>, OutputIterator<int *>>());
        STATIC_CHECK(
            test_constexpr<RandomAccessIterator<const int *>, InputIterator<int *>>());
        STATIC_CHECK(
            test_constexpr<RandomAccessIterator<const int *>, ForwardIterator<int *>>());
        STATIC_CHECK(test_constexpr<RandomAccessIterator<const int *>,
                                    BidirectionalIterator<int *>>());
        STATIC_CHECK(test_constexpr<RandomAccessIterator<const int *>,
                                    RandomAccessIterator<int *>>());
        STATIC_CHECK(test_constexpr<RandomAccessIterator<const int *>, int *>());

        STATIC_CHECK(test_constexpr<const int *, OutputIterator<int *>>());
        STATIC_CHECK(test_constexpr<const int *, InputIterator<int *>>());
        STATIC_CHECK(test_constexpr<const int *, ForwardIterator<int *>>());
        STATIC_CHECK(test_constexpr<const int *, BidirectionalIterator<int *>>());
        STATIC_CHECK(test_constexpr<const int *, RandomAccessIterator<int *>>());
        STATIC_CHECK(test_constexpr<const int *, int *>());

        STATIC_CHECK(test_constexpr<InputIterator<const int *>,
                                    OutputIterator<int *>,
                                    Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<InputIterator<const int *>,
                                    InputIterator<int *>,
                                    Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<InputIterator<const int *>,
                                    ForwardIterator<int *>,
                                    Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<InputIterator<const int *>,
                                    BidirectionalIterator<int *>,
                                    Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<InputIterator<const int *>,
                                    RandomAccessIterator<int *>,
                                    Sentinel<const int *>>());

        STATIC_CHECK(test_constexpr<ForwardIterator<const int *>,
                                    OutputIterator<int *>,
                                    Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<ForwardIterator<const int *>,
                                    InputIterator<int *>,
                                    Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<ForwardIterator<const int *>,
                                    ForwardIterator<int *>,
                                    Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<ForwardIterator<const int *>,
                                    BidirectionalIterator<int *>,
                                    Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<ForwardIterator<const int *>,
                                    RandomAccessIterator<int *>,
                                    Sentinel<const int *>>());

        STATIC_CHECK(test_constexpr<BidirectionalIterator<const int *>,
                                    OutputIterator<int *>,
                                    Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<BidirectionalIterator<const int *>,
                                    InputIterator<int *>,
                                    Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<BidirectionalIterator<const int *>,
                                    ForwardIterator<int *>,
                                    Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<BidirectionalIterator<const int *>,
                                    BidirectionalIterator<int *>,
                                    Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<BidirectionalIterator<const int *>,
                                    RandomAccessIterator<int *>,
                                    Sentinel<const int *>>());

        STATIC_CHECK(test_constexpr<RandomAccessIterator<const int *>,
                                    OutputIterator<int *>,
                                    Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<RandomAccessIterator<const int *>,
                                    InputIterator<int *>,
                                    Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<RandomAccessIterator<const int *>,
                                    ForwardIterator<int *>,
                                    Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<RandomAccessIterator<const int *>,
                                    BidirectionalIterator<int *>,
                                    Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<RandomAccessIterator<const int *>,
                                    RandomAccessIterator<int *>,
                                    Sentinel<const int *>>());
    }

    return test_result();
}
