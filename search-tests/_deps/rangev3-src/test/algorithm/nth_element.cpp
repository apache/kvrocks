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
#include <range/v3/core.hpp>
#include <range/v3/algorithm/nth_element.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

RANGES_DIAGNOSTIC_IGNORE_GLOBAL_CONSTRUCTORS
RANGES_DIAGNOSTIC_IGNORE_SIGN_CONVERSION

namespace
{
    std::mt19937 gen;

    void
    test_one(unsigned N, unsigned M)
    {
        RANGES_ENSURE(N != 0);
        RANGES_ENSURE(M < N);
        std::unique_ptr<int[]> array{new int[N]};
        for (int i = 0; (unsigned)i < N; ++i)
            array[i] = i;
        std::shuffle(array.get(), array.get()+N, gen);
        CHECK(ranges::nth_element(array.get(), array.get()+M, array.get()+N) == array.get()+N);
        CHECK((unsigned)array[M] == M);
        std::shuffle(array.get(), array.get()+N, gen);
        CHECK(ranges::nth_element(::as_lvalue(ranges::make_subrange(array.get(), array.get()+N)), array.get()+M) == array.get()+N);
        CHECK((unsigned)array[M] == M);
        std::shuffle(array.get(), array.get()+N, gen);
        CHECK(ranges::nth_element(ranges::make_subrange(array.get(), array.get()+N), array.get()+M) == array.get()+N);
        CHECK((unsigned)array[M] == M);
        ranges::nth_element(array.get(), array.get()+N, array.get()+N); // first, last, end
    }

    void
    test(unsigned N)
    {
        test_one(N, 0);
        test_one(N, 1);
        test_one(N, 2);
        test_one(N, 3);
        test_one(N, N/2-1);
        test_one(N, N/2);
        test_one(N, N/2+1);
        test_one(N, N-3);
        test_one(N, N-2);
        test_one(N, N-1);
    }

    struct S
    {
        int i,j;
    };
}

int main()
{
    int d = 0;
    ranges::nth_element(&d, &d, &d);
    CHECK(d == 0);
    test(256);
    test(257);
    test(499);
    test(500);
    test(997);
    test(1000);
    test(1009);

    // Works with projections?
    const int N = 257;
    const int M = 56;
    S ia[N];
    for(int i = 0; i < N; ++i)
        ia[i].i = ia[i].j = i;
    std::shuffle(ia, ia+N, gen);
    ranges::nth_element(ia, ia+M, std::less<int>(), &S::i);
    CHECK(ia[M].i == M);
    CHECK(ia[M].j == M);

    return test_result();
}
