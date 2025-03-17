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

#include <memory>
#include <random>
#include <algorithm>
#include <functional>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/heap_algorithm.hpp>

#include "../array.hpp"
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

RANGES_DIAGNOSTIC_IGNORE_GLOBAL_CONSTRUCTORS
RANGES_DIAGNOSTIC_IGNORE_SIGN_CONVERSION

namespace
{
    std::mt19937 gen;

    void test_1(int N)
    {
        int* ia = new int[N];
        for (int i = 0; i < N; ++i)
            ia[i] = i;
        std::shuffle(ia, ia+N, gen);
        std::make_heap(ia, ia+N);
        for (int i = N; i > 0; --i)
        {
            CHECK(ranges::pop_heap(ia, ia+i) == ia+i);
            CHECK(std::is_heap(ia, ia+i-1));
        }
        CHECK(ranges::pop_heap(ia, ia) == ia);
        delete[] ia;
    }

    void test_2(int N)
    {
        int* ia = new int[N];
        for (int i = 0; i < N; ++i)
            ia[i] = i;
        std::shuffle(ia, ia+N, gen);
        std::make_heap(ia, ia+N);
        for (int i = N; i > 0; --i)
        {
            CHECK(ranges::pop_heap(ia, Sentinel<int*>(ia+i)) == ia+i);
            CHECK(std::is_heap(ia, ia+i-1));
        }
        CHECK(ranges::pop_heap(ia, ia) == ia);
        delete[] ia;
    }

    void test_3(int N)
    {
        int* ia = new int[N];
        for (int i = 0; i < N; ++i)
            ia[i] = i;
        std::shuffle(ia, ia+N, gen);
        std::make_heap(ia, ia+N);
        for (int i = N; i > 0; --i)
        {
            CHECK(ranges::pop_heap(::as_lvalue(ranges::make_subrange(ia, ia+i))) == ia+i);
            CHECK(std::is_heap(ia, ia+i-1));
        }
        std::shuffle(ia, ia+N, gen);
        std::make_heap(ia, ia+N);
        for (int i = N; i > 0; --i)
        {
            CHECK(ranges::pop_heap(ranges::make_subrange(ia, ia+i)) == ia+i);
            CHECK(std::is_heap(ia, ia+i-1));
        }
        CHECK(ranges::pop_heap(ia, ia) == ia);
        delete[] ia;
    }

    void test_4(int N)
    {
        int* ia = new int[N];
        for (int i = 0; i < N; ++i)
            ia[i] = i;
        std::shuffle(ia, ia+N, gen);
        std::make_heap(ia, ia+N);
        for (int i = N; i > 0; --i)
        {
            CHECK(ranges::pop_heap(::as_lvalue(ranges::make_subrange(ia, Sentinel<int*>(ia+i)))) == ia+i);
            CHECK(std::is_heap(ia, ia+i-1));
        }
        std::shuffle(ia, ia+N, gen);
        std::make_heap(ia, ia+N);
        for (int i = N; i > 0; --i)
        {
            CHECK(ranges::pop_heap(ranges::make_subrange(ia, Sentinel<int*>(ia+i))) == ia+i);
            CHECK(std::is_heap(ia, ia+i-1));
        }
        CHECK(ranges::pop_heap(ia, ia) == ia);
        delete[] ia;
    }

    void test_5(int N)
    {
        int* ia = new int[N];
        for (int i = 0; i < N; ++i)
            ia[i] = i;
        std::shuffle(ia, ia+N, gen);
        std::make_heap(ia, ia+N, std::greater<int>());
        for (int i = N; i > 0; --i)
        {
            CHECK(ranges::pop_heap(ia, ia+i, std::greater<int>()) == ia+i);
            CHECK(std::is_heap(ia, ia+i-1, std::greater<int>()));
        }
        CHECK(ranges::pop_heap(ia, ia, std::greater<int>()) == ia);
        delete[] ia;
    }

    void test_6(int N)
    {
        int* ia = new int[N];
        for (int i = 0; i < N; ++i)
            ia[i] = i;
        std::shuffle(ia, ia+N, gen);
        std::make_heap(ia, ia+N, std::greater<int>());
        for (int i = N; i > 0; --i)
        {
            CHECK(ranges::pop_heap(ia, Sentinel<int*>(ia+i), std::greater<int>()) == ia+i);
            CHECK(std::is_heap(ia, ia+i-1, std::greater<int>()));
        }
        CHECK(ranges::pop_heap(ia, Sentinel<int*>(ia), std::greater<int>()) == ia);
        delete[] ia;
    }

    void test_7(int N)
    {
        int* ia = new int[N];
        for (int i = 0; i < N; ++i)
            ia[i] = i;
        std::shuffle(ia, ia+N, gen);
        std::make_heap(ia, ia+N, std::greater<int>());
        for (int i = N; i > 0; --i)
        {
            CHECK(ranges::pop_heap(::as_lvalue(ranges::make_subrange(ia, ia+i)), std::greater<int>()) == ia+i);
            CHECK(std::is_heap(ia, ia+i-1, std::greater<int>()));
        }
        std::shuffle(ia, ia+N, gen);
        std::make_heap(ia, ia+N, std::greater<int>());
        for (int i = N; i > 0; --i)
        {
            CHECK(ranges::pop_heap(ranges::make_subrange(ia, ia+i), std::greater<int>()) == ia+i);
            CHECK(std::is_heap(ia, ia+i-1, std::greater<int>()));
        }
        CHECK(ranges::pop_heap(ia, ia, std::greater<int>()) == ia);
        delete[] ia;
    }

    void test_8(int N)
    {
        int* ia = new int[N];
        for (int i = 0; i < N; ++i)
            ia[i] = i;
        std::shuffle(ia, ia+N, gen);
        std::make_heap(ia, ia+N, std::greater<int>());
        for (int i = N; i > 0; --i)
        {
            CHECK(ranges::pop_heap(::as_lvalue(ranges::make_subrange(ia, Sentinel<int*>(ia+i))), std::greater<int>()) == ia+i);
            CHECK(std::is_heap(ia, ia+i-1, std::greater<int>()));
        }
        std::shuffle(ia, ia+N, gen);
        std::make_heap(ia, ia+N, std::greater<int>());
        for (int i = N; i > 0; --i)
        {
            CHECK(ranges::pop_heap(ranges::make_subrange(ia, Sentinel<int*>(ia+i)), std::greater<int>()) == ia+i);
            CHECK(std::is_heap(ia, ia+i-1, std::greater<int>()));
        }
        CHECK(ranges::pop_heap(ia, Sentinel<int*>(ia), std::greater<int>()) == ia);
        delete[] ia;
    }

    struct indirect_less
    {
        template<class P>
        bool operator()(const P& x, const P& y)
            {return *x < *y;}
    };

    void test_9(int N)
    {
        std::unique_ptr<int>* ia = new std::unique_ptr<int>[N];
        for (int i = 0; i < N; ++i)
            ia[i].reset(new int(i));
        std::shuffle(ia, ia+N, gen);
        std::make_heap(ia, ia+N, indirect_less());
        for (int i = N; i > 0; --i)
        {
            CHECK(ranges::pop_heap(ia, ia+i, indirect_less()) == ia+i);
            CHECK(std::is_heap(ia, ia+i-1, indirect_less()));
        }
        delete[] ia;
    }

    template<typename T>
    struct construct
    {
        template<typename ...Us>
        T operator()(Us &&... us) const
        {
            return T{((Us &&)us)...};
        }
    };

    struct S
    {
        int i;
    };

    void test_10(int N)
    {
        int* ia = new int[N];
        S* ib = new S[N];
        for (int i = 0; i < N; ++i)
            ia[i] = i;
        std::shuffle(ia, ia+N, gen);
        std::make_heap(ia, ia+N);
        std::transform(ia, ia+N, ib, construct<S>());
        for (int i = N; i > 0; --i)
        {
            CHECK(ranges::pop_heap(ib, ib+i, std::less<int>(), &S::i) == ib+i);
            std::transform(ib, ib+i, ia, std::mem_fn(&S::i));
            CHECK(std::is_heap(ia, ia+i-1));
        }
        CHECK(ranges::pop_heap(ib, ib, std::less<int>(), &S::i) == ib);
        delete[] ia;
        delete[] ib;
    }
}

constexpr bool test_constexpr()
{
    using namespace ranges;
    constexpr int N = 100;
    test::array<int, N> ia{{0}};
    for(int i = 0; i < N; ++i)
        ia[i] = i;
    make_heap(ia);
    for(int i = N; i > 0; --i)
    {
        STATIC_CHECK_RETURN(pop_heap(make_subrange(begin(ia), begin(ia) + i), less{}) == begin(ia) + i);
        STATIC_CHECK_RETURN(is_heap(begin(ia), begin(ia) + i - 1));
    }
    STATIC_CHECK_RETURN(pop_heap(make_subrange(begin(ia), begin(ia)), less{}) == begin(ia));
    return true;
}

int main()
{
    test_1(1000);
    test_2(1000);
    test_3(1000);
    test_4(1000);
    test_5(1000);
    test_6(1000);
    test_7(1000);
    test_8(1000);
    test_9(1000);
    test_10(1000);

    {
        STATIC_CHECK(test_constexpr());
    }

    return test_result();
}
