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
#include <random>
#include <algorithm>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/partial_sort.hpp>

#include "../array.hpp"
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

RANGES_DIAGNOSTIC_IGNORE_GLOBAL_CONSTRUCTORS
RANGES_DIAGNOSTIC_IGNORE_SIGN_CONVERSION

namespace
{
    std::mt19937 gen;

    struct indirect_less
    {
        template<class P>
        bool operator()(const P& x, const P& y)
            {return *x < *y;}
    };

    void
    test_larger_sorts(int N, int M)
    {
        RANGES_ENSURE(N > 0);
        RANGES_ENSURE(M >= 0 && M <= N);
        int* array = new int[N];
        for(int i = 0; i < N; ++i)
            array[i] = i;

        using I = RandomAccessIterator<int*>;
        using S = Sentinel<int*>;

        std::shuffle(array, array+N, gen);
        int *res = ranges::partial_sort(array, array+M, array+N);
        CHECK(res == array+N);
        for(int i = 0; i < M; ++i)
            CHECK(array[i] == i);

        std::shuffle(array, array+N, gen);
        I res2 = ranges::partial_sort(I{array}, I{array+M}, S{array+N});
        CHECK(res2.base() == array+N);
        for(int i = 0; i < M; ++i)
            CHECK(array[i] == i);

        std::shuffle(array, array+N, gen);
        res = ranges::partial_sort(ranges::make_subrange(array, array+N), array+M);
        CHECK(res == array+N);
        for(int i = 0; i < M; ++i)
            CHECK(array[i] == i);

        std::shuffle(array, array+N, gen);
        res2 = ranges::partial_sort(ranges::make_subrange(I{array}, S{array+N}), I{array+M});
        CHECK(res2.base() == array+N);
        for(int i = 0; i < M; ++i)
            CHECK(array[i] == i);

        std::shuffle(array, array+N, gen);
        auto res3 = ranges::partial_sort(::MakeTestRange(array, array+N), array+M);
        CHECK(::is_dangling(res3));
        for(int i = 0; i < M; ++i)
            CHECK(array[i] == i);

        std::shuffle(array, array+N, gen);
        auto res4 = ranges::partial_sort(::MakeTestRange(I{array}, S{array+N}), I{array+M});
        CHECK(::is_dangling(res4));
        for(int i = 0; i < M; ++i)
            CHECK(array[i] == i);

        std::shuffle(array, array+N, gen);
        res = ranges::partial_sort(array, array+M, array+N, std::greater<int>());
        CHECK(res == array+N);
        for(int i = 0; i < M; ++i)
            CHECK(array[i] == N-i-1);

        std::shuffle(array, array+N, gen);
        res2 = ranges::partial_sort(I{array}, I{array+M}, S{array+N}, std::greater<int>());
        CHECK(res2.base() == array+N);
        for(int i = 0; i < M; ++i)
            CHECK(array[i] == N-i-1);

        std::shuffle(array, array+N, gen);
        res = ranges::partial_sort(ranges::make_subrange(array, array+N), array+M, std::greater<int>());
        CHECK(res == array+N);
        for(int i = 0; i < M; ++i)
            CHECK(array[i] == N-i-1);

        std::shuffle(array, array+N, gen);
        res2 = ranges::partial_sort(ranges::make_subrange(I{array}, S{array+N}), I{array+M}, std::greater<int>());
        CHECK(res2.base() == array+N);
        for(int i = 0; i < M; ++i)
            CHECK(array[i] == N-i-1);

        delete [] array;
    }

    void
    test_larger_sorts(int N)
    {
        test_larger_sorts(N, 0);
        test_larger_sorts(N, 1);
        test_larger_sorts(N, 2);
        test_larger_sorts(N, 3);
        test_larger_sorts(N, N/2-1);
        test_larger_sorts(N, N/2);
        test_larger_sorts(N, N/2+1);
        test_larger_sorts(N, N-2);
        test_larger_sorts(N, N-1);
        test_larger_sorts(N, N);
    }

    struct S
    {
        int i, j;
    };
}

constexpr bool test_constexpr()
{
    test::array<std::size_t, 10> v{{0}};
    for(std::size_t i = 0; i < v.size(); ++i)
    {
        v[i] = v.size() - i - 1;
    }
    ranges::partial_sort(v, v.begin() + v.size() / 2, ranges::less{});
    for(size_t i = 0; i < v.size() / 2; ++i)
    {
        STATIC_CHECK_RETURN(v[i] == i);
    }
    return true;
}

int main()
{
    int i = 0;
    int * res = ranges::partial_sort(&i, &i, &i);
    CHECK(i == 0);
    CHECK(res == &i);
    test_larger_sorts(10);
    test_larger_sorts(256);
    test_larger_sorts(257);
    test_larger_sorts(499);
    test_larger_sorts(500);
    test_larger_sorts(997);
    test_larger_sorts(1000);
    test_larger_sorts(1009);

    // Check move-only types
    {
        std::vector<std::unique_ptr<int> > v(1000);
        for(int j = 0; j < (int)v.size(); ++j)
            v[j].reset(new int((int)v.size() - j - 1));
        ranges::partial_sort(v, v.begin() + v.size()/2, indirect_less());
        for(int j = 0; j < (int)v.size()/2; ++j)
            CHECK(*v[j] == j);
    }

    // Check projections
    {
        std::vector<S> v(1000, S{});
        for(int j = 0; (std::size_t)j < v.size(); ++j)
        {
            v[j].i = (int)v.size() - j - 1;
            v[j].j = j;
        }
        ranges::partial_sort(v, v.begin() + v.size()/2, std::less<int>{}, &S::i);
        for(int j = 0; (std::size_t)j < v.size()/2; ++j)
        {
            CHECK(v[j].i == j);
            CHECK((std::size_t)v[j].j == v.size() - j - 1);
        }
    }

    {
        STATIC_CHECK(test_constexpr());
    }

    return ::test_result();
}
