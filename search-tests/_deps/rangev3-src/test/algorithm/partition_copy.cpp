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

#include <tuple>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/partition_copy.hpp>
#include <range/v3/view/counted.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

struct is_odd
{
    constexpr bool operator()(const int & i) const
    {
        return i & 1;
    }
};

template<class Iter, class Sent = Iter>
void
test_iter()
{
    const int ia[] = {1, 2, 3, 4, 6, 8, 5, 7};
    int r1[10] = {0};
    int r2[10] = {0};
    typedef ranges::partition_copy_result<Iter, OutputIterator<int*>,  int*> P;
    P p = ranges::partition_copy(Iter(std::begin(ia)),
                                 Sent(std::end(ia)),
                                 OutputIterator<int*>(r1), r2, is_odd());
    CHECK(p.in == Iter(std::end(ia)));
    CHECK(p.out1.base() == r1 + 4);
    CHECK(r1[0] == 1);
    CHECK(r1[1] == 3);
    CHECK(r1[2] == 5);
    CHECK(r1[3] == 7);
    CHECK(p.out2 == r2 + 4);
    CHECK(r2[0] == 2);
    CHECK(r2[1] == 4);
    CHECK(r2[2] == 6);
    CHECK(r2[3] == 8);
}

template<class Iter, class Sent = Iter>
void
test_range()
{
    const int ia[] = {1, 2, 3, 4, 6, 8, 5, 7};
    int r1[10] = {0};
    int r2[10] = {0};
    typedef ranges::partition_copy_result<Iter, OutputIterator<int*>,  int*> P;
    P p = ranges::partition_copy(::as_lvalue(ranges::make_subrange(Iter(std::begin(ia)),
                                                           Sent(std::end(ia)))),
                                 OutputIterator<int*>(r1), r2, is_odd());
    CHECK(p.in == Iter(std::end(ia)));
    CHECK(p.out1.base() == r1 + 4);
    CHECK(r1[0] == 1);
    CHECK(r1[1] == 3);
    CHECK(r1[2] == 5);
    CHECK(r1[3] == 7);
    CHECK(p.out2 == r2 + 4);
    CHECK(r2[0] == 2);
    CHECK(r2[1] == 4);
    CHECK(r2[2] == 6);
    CHECK(r2[3] == 8);
}

struct S
{
    int i;
};

void test_proj()
{
    // Test projections
    const S ia[] = {S{1}, S{2}, S{3}, S{4}, S{6}, S{8}, S{5}, S{7}};
    S r1[10] = {S{0}};
    S r2[10] = {S{0}};
    typedef ranges::partition_copy_result<S const *, S*,  S*> P;
    P p = ranges::partition_copy(ia, r1, r2, is_odd(), &S::i);
    CHECK(p.in == std::end(ia));
    CHECK(p.out1 == r1 + 4);
    CHECK(r1[0].i == 1);
    CHECK(r1[1].i == 3);
    CHECK(r1[2].i == 5);
    CHECK(r1[3].i == 7);
    CHECK(p.out2 == r2 + 4);
    CHECK(r2[0].i == 2);
    CHECK(r2[1].i == 4);
    CHECK(r2[2].i == 6);
    CHECK(r2[3].i == 8);
}

void test_rvalue()
{
    // Test rvalue ranges
    const S ia[] = {S{1}, S{2}, S{3}, S{4}, S{6}, S{8}, S{5}, S{7}};
    S r1[10] = {};
    S r2[10] = {};
    auto p = ranges::partition_copy(std::move(ia), r1, r2, is_odd(), &S::i);
#ifndef RANGES_WORKAROUND_MSVC_573728
    CHECK(::is_dangling(p.in));
#endif
    CHECK(p.out1 == r1 + 4);
    CHECK(r1[0].i == 1);
    CHECK(r1[1].i == 3);
    CHECK(r1[2].i == 5);
    CHECK(r1[3].i == 7);
    CHECK(p.out2 == r2 + 4);
    CHECK(r2[0].i == 2);
    CHECK(r2[1].i == 4);
    CHECK(r2[2].i == 6);
    CHECK(r2[3].i == 8);

    std::fill(r1 + 0, r1 + 10, S{});
    std::fill(r2 + 0, r2 + 10, S{});
    std::vector<S> vec(ranges::begin(ia), ranges::end(ia));
    auto q = ranges::partition_copy(std::move(vec), r1, r2, is_odd(), &S::i);
#ifndef RANGES_WORKAROUND_MSVC_573728
    CHECK(::is_dangling(q.in));
#endif
    CHECK(q.out1 == r1 + 4);
    CHECK(r1[0].i == 1);
    CHECK(r1[1].i == 3);
    CHECK(r1[2].i == 5);
    CHECK(r1[3].i == 7);
    CHECK(q.out2 == r2 + 4);
    CHECK(r2[0].i == 2);
    CHECK(r2[1].i == 4);
    CHECK(r2[2].i == 6);
    CHECK(r2[3].i == 8);
}

constexpr bool test_constexpr()
{
    using namespace ranges;
    const int ia[] = {1, 2, 3, 4, 6, 8, 5, 7};
    int r1[10] = {0};
    int r2[10] = {0};
    const auto p = partition_copy(ia, r1, r2, is_odd());
    STATIC_CHECK_RETURN(p.in == std::end(ia));
    STATIC_CHECK_RETURN(p.out1 == r1 + 4);
    STATIC_CHECK_RETURN(r1[0] == 1);
    STATIC_CHECK_RETURN(r1[1] == 3);
    STATIC_CHECK_RETURN(r1[2] == 5);
    STATIC_CHECK_RETURN(r1[3] == 7);
    STATIC_CHECK_RETURN(p.out2 == r2 + 4);
    STATIC_CHECK_RETURN(r2[0] == 2);
    STATIC_CHECK_RETURN(r2[1] == 4);
    STATIC_CHECK_RETURN(r2[2] == 6);
    STATIC_CHECK_RETURN(r2[3] == 8);
    return true;
}

int main()
{
    test_iter<InputIterator<const int*> >();
    test_iter<InputIterator<const int*>, Sentinel<const int*>>();

    test_range<InputIterator<const int*> >();
    test_range<InputIterator<const int*>, Sentinel<const int*>>();

    test_proj();
    test_rvalue();

    {
        STATIC_CHECK(test_constexpr());
    }

    return ::test_result();
}
