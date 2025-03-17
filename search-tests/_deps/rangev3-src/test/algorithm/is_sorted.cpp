// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//  Copyright Gonzalo Brito Gadeschi 2014
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

#include <range/v3/core.hpp>
#include <range/v3/algorithm/is_sorted.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

/// Calls the iterator interface of the algorithm
template<class Iter>
struct iter_call
{
    using begin_t = Iter;
    using sentinel_t = typename sentinel_type<Iter>::type;

    template<class B, class E, class... Args>
    auto operator()(B &&b, E &&e, Args &&... args)
     -> decltype(ranges::is_sorted(begin_t{b}, sentinel_t{e},
                                   std::forward<Args>(args)...))
    {
        return ranges::is_sorted(begin_t{b}, sentinel_t{e}, std::forward<Args>(args)...);
    }
};

/// Calls the range interface of the algorithm
template<class Iter>
struct range_call
{
    using begin_t = Iter;
    using sentinel_t = typename sentinel_type<Iter>::type;

    template<class B, class E>
    auto operator()(B && b, E && e)
        -> decltype(ranges::is_sorted(ranges::make_subrange(begin_t{b}, sentinel_t{e})))
    {
        return ranges::is_sorted(ranges::make_subrange(begin_t{b}, sentinel_t{e}));
    }
    template<class B, class E, class A0>
    auto operator()(B && b, E && e, A0 && a0)
        -> decltype(ranges::is_sorted(ranges::make_subrange(begin_t{b}, sentinel_t{e}),
                                      static_cast<A0 &&>(a0)))
    {
        return ranges::is_sorted(ranges::make_subrange(begin_t{b}, sentinel_t{e}),
                                 static_cast<A0 &&>(a0));
    }
    template<class B, class E, class A0, class A1>
    auto operator()(B && b, E && e, A0 && a0, A1 && a1)
        -> decltype(ranges::is_sorted(ranges::make_subrange(begin_t{b}, sentinel_t{e}),
                                      static_cast<A0 &&>(a0), static_cast<A1 &&>(a1)))
    {
        return ranges::is_sorted(ranges::make_subrange(begin_t{b}, sentinel_t{e}),
                                 static_cast<A0 &&>(a0),
                                 static_cast<A1 &&>(a1));
    }
};

template<class Fun>
void test()
{
    {
        int a[] = {0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a));
        CHECK(Fun{}(a, a + sa));
    }

    {
        int a[] = {0, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa));
    }
    {
        int a[] = {0, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa));
    }
    {
        int a[] = {1, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa));
    }
    {
        int a[] = {1, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa));
    }

    {
        int a[] = {0, 0, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa));
    }
    {
        int a[] = {0, 0, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa));
    }
    {
        int a[] = {0, 1, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa));
    }
    {
        int a[] = {0, 1, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa));
    }
    {
        int a[] = {1, 0, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa));
    }
    {
        int a[] = {1, 0, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa));
    }
    {
        int a[] = {1, 1, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa));
    }
    {
        int a[] = {1, 1, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa));
    }

    {
        int a[] = {0, 0, 0, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa));
    }
    {
        int a[] = {0, 0, 0, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa));
    }
    {
        int a[] = {0, 0, 1, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa));
    }
    {
        int a[] = {0, 0, 1, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa));
    }
    {
        int a[] = {0, 1, 0, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa));
    }
    {
        int a[] = {0, 1, 0, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa));
    }
    {
        int a[] = {0, 1, 1, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa));
    }
    {
        int a[] = {0, 1, 1, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa));
    }
    {
        int a[] = {1, 0, 0, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa));
    }
    {
        int a[] = {1, 0, 0, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa));
    }
    {
        int a[] = {1, 0, 1, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa));
    }
    {
        int a[] = {1, 0, 1, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa));
    }
    {
        int a[] = {1, 1, 0, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa));
    }
    {
        int a[] = {1, 1, 0, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa));
    }
    {
        int a[] = {1, 1, 1, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa));
    }
    {
        int a[] = {1, 1, 1, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa));
    }

    {
        int a[] = {0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a));
        CHECK(Fun{}(a, a + sa, std::greater<int>()));
    }

    {
        int a[] = {0, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {0, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {1, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {1, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa, std::greater<int>()));
    }

    {
        int a[] = {0, 0, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {0, 0, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {0, 1, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {0, 1, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {1, 0, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {1, 0, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {1, 1, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {1, 1, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa, std::greater<int>()));
    }

    {
        int a[] = {0, 0, 0, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {0, 0, 0, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {0, 0, 1, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {0, 0, 1, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {0, 1, 0, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {0, 1, 0, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {0, 1, 1, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {0, 1, 1, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {1, 0, 0, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {1, 0, 0, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {1, 0, 1, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {1, 0, 1, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {1, 1, 0, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {1, 1, 0, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(!Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {1, 1, 1, 0};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa, std::greater<int>()));
    }
    {
        int a[] = {1, 1, 1, 1};
        unsigned sa = sizeof(a) / sizeof(a[0]);
        CHECK(Fun{}(a, a + sa, std::greater<int>()));
    }
}

struct A { int a; };

int main()
{
    test<iter_call<ForwardIterator<const int *>>>();
    test<iter_call<BidirectionalIterator<const int *>>>();
    test<iter_call<RandomAccessIterator<const int *>>>();
    test<iter_call<const int *>>();

    test<range_call<ForwardIterator<const int *>>>();
    test<range_call<BidirectionalIterator<const int *>>>();
    test<range_call<RandomAccessIterator<const int *>>>();
    test<range_call<const int *>>();

    /// Projection test:
    {
        A as[] = {{0}, {1}, {2}, {3}, {4}};
        CHECK(ranges::is_sorted(as, std::less<int>{}, &A::a));
        CHECK(!ranges::is_sorted(as, std::greater<int>{}, &A::a));
    }

    {
        using IL = std::initializer_list<int>;
        STATIC_CHECK(ranges::is_sorted(IL{0, 1, 2, 3}));
        STATIC_CHECK(ranges::is_sorted(IL{0, 1, 2, 3}, std::less<>{}));
        STATIC_CHECK(!ranges::is_sorted(IL{3, 2, 1, 0}));
        STATIC_CHECK(ranges::is_sorted(IL{3, 2, 1, 0}, std::greater<>{}));
    }

    return ::test_result();
}
