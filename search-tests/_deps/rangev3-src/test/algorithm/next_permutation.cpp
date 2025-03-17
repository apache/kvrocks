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

#include <cstring>
#include <utility>
#include <algorithm>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/permutation.hpp>
#include <range/v3/algorithm/equal.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

int factorial(int x)
{
    int r = 1;
    for (; x; --x)
        r *= x;
    return r;
}

template<typename Iter, typename Sent = Iter>
void test_iter()
{
    int ia[] = {1, 2, 3, 4, 5, 6};
    const int sa = sizeof(ia)/sizeof(ia[0]);
    int prev[sa];
    for (int e = 0; e <= sa; ++e)
    {
        int count = 0;
        bool x;
        do
        {
            std::copy(ia, ia+e, prev);
            x = ranges::next_permutation(Iter(ia), Sent(ia+e));
            if(e > 1)
            {
                if(x)
                    CHECK(std::lexicographical_compare(prev, prev+e, ia, ia+e));
                else
                    CHECK(std::lexicographical_compare(ia, ia+e, prev, prev+e));
            }
            ++count;
        } while(x);
        CHECK(count == factorial(e));
    }
}

template<typename Iter, typename Sent = Iter>
void test_range()
{
    int ia[] = {1, 2, 3, 4, 5, 6};
    const int sa = sizeof(ia)/sizeof(ia[0]);
    int prev[sa];
    for (int e = 0; e <= sa; ++e)
    {
        int count = 0;
        bool x;
        do
        {
            std::copy(ia, ia+e, prev);
            x = ranges::next_permutation(ranges::make_subrange(Iter(ia), Sent(ia+e)));
            if(e > 1)
            {
                if(x)
                    CHECK(std::lexicographical_compare(prev, prev+e, ia, ia+e));
                else
                    CHECK(std::lexicographical_compare(ia, ia+e, prev, prev+e));
            }
            ++count;
        } while(x);
        CHECK(count == factorial(e));
    }
}

template<typename Iter, typename Sent = Iter>
void test_iter_comp()
{
    typedef std::greater<int> C;
    int ia[] = {6, 5, 4, 3, 2, 1};
    const int sa = sizeof(ia)/sizeof(ia[0]);
    int prev[sa];
    for(int e = 0; e <= sa; ++e)
    {
        int count = 0;
        bool x;
        do
        {
            std::copy(ia, ia+e, prev);
            x = ranges::next_permutation(Iter(ia), Sent(ia+e), C());
            if(e > 1)
            {
                if (x)
                    CHECK(std::lexicographical_compare(prev, prev+e, ia, ia+e, C()));
                else
                    CHECK(std::lexicographical_compare(ia, ia+e, prev, prev+e, C()));
            }
            ++count;
        } while (x);
        CHECK(count == factorial(e));
    }
}

template<typename Iter, typename Sent = Iter>
void test_range_comp()
{
    typedef std::greater<int> C;
    int ia[] = {6, 5, 4, 3, 2, 1};
    const int sa = sizeof(ia)/sizeof(ia[0]);
    int prev[sa];
    for(int e = 0; e <= sa; ++e)
    {
        int count = 0;
        bool x;
        do
        {
            std::copy(ia, ia+e, prev);
            x = ranges::next_permutation(ranges::make_subrange(Iter(ia), Sent(ia+e)), C());
            if(e > 1)
            {
                if (x)
                    CHECK(std::lexicographical_compare(prev, prev+e, ia, ia+e, C()));
                else
                    CHECK(std::lexicographical_compare(ia, ia+e, prev, prev+e, C()));
            }
            ++count;
        } while (x);
            CHECK(count == factorial(e));
    }
}

struct c_str
{
    char const * value;

    friend bool operator==(c_str a, c_str b)
    {
        return 0 == std::strcmp(a.value, b.value);
    }

    friend bool operator!=(c_str a, c_str b)
    {
        return !(a == b);
    }
};

// For debugging the projection test
std::ostream &operator<<(std::ostream& sout, std::pair<int, c_str> p)
{
    return sout << "{" << p.first << "," << p.second.value << "}";
}

constexpr bool test_constexpr()
{
    using namespace ranges;
    int ia[] = {6, 5, 4, 3, 2, 1};
    using IL = std::initializer_list<int>;
    next_permutation(ia, greater{});
    STATIC_CHECK_RETURN(equal(ia, IL{6, 5, 4, 3, 1, 2}));
    next_permutation(ia, greater{});
    STATIC_CHECK_RETURN(equal(ia, IL{6, 5, 4, 2, 3, 1}));
    next_permutation(ia, greater{});
    STATIC_CHECK_RETURN(equal(ia, IL{6, 5, 4, 2, 1, 3}));
    return true;
}

int main()
{
    test_iter<BidirectionalIterator<int*> >();
    test_iter<RandomAccessIterator<int*> >();
    test_iter<int*>();

    test_iter<BidirectionalIterator<int*>, Sentinel<int*> >();
    test_iter<RandomAccessIterator<int*>, Sentinel<int*> >();

    test_iter_comp<BidirectionalIterator<int*> >();
    test_iter_comp<RandomAccessIterator<int*> >();
    test_iter_comp<int*>();

    test_iter_comp<BidirectionalIterator<int*>, Sentinel<int*> >();
    test_iter_comp<RandomAccessIterator<int*>, Sentinel<int*> >();

    test_range<BidirectionalIterator<int*> >();
    test_range<RandomAccessIterator<int*> >();
    test_range<int*>();

    test_range<BidirectionalIterator<int*>, Sentinel<int*> >();
    test_range<RandomAccessIterator<int*>, Sentinel<int*> >();

    test_range_comp<BidirectionalIterator<int*> >();
    test_range_comp<RandomAccessIterator<int*> >();
    test_range_comp<int*>();

    test_range_comp<BidirectionalIterator<int*>, Sentinel<int*> >();
    test_range_comp<RandomAccessIterator<int*>, Sentinel<int*> >();

    // Test projection

    using C = std::greater<int>;
    using I = std::initializer_list<std::pair<int, c_str>>;
    std::pair<int, c_str> ia[] = {{6, {"six"}}, {5,{"five"}}, {4,{"four"}}, {3,{"three"}}, {2,{"two"}}, {1,{"one"}}};
    CHECK(ranges::next_permutation(ia, C(), &std::pair<int,c_str>::first));
    ::check_equal(ia, I{{6, {"six"}}, {5,{"five"}}, {4,{"four"}}, {3,{"three"}}, {1,{"one"}}, {2,{"two"}}});
    CHECK(ranges::next_permutation(ia, C(), &std::pair<int,c_str>::first));
    ::check_equal(ia, I{{6, {"six"}}, {5,{"five"}}, {4,{"four"}}, {2,{"two"}}, {3,{"three"}}, {1,{"one"}}});
    CHECK(ranges::next_permutation(ia, C(), &std::pair<int,c_str>::first));
    ::check_equal(ia, I{{6, {"six"}}, {5,{"five"}}, {4,{"four"}}, {2,{"two"}}, {1,{"one"}}, {3,{"three"}}});
    // etc..

    {
        STATIC_CHECK(test_constexpr());
    }

    return ::test_result();
}
