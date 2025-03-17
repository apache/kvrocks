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

#include <utility>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/replace_copy_if.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

template<class InIter, class OutIter, class Sent = InIter>
void test_iter()
{
    int ia[] = {0, 1, 2, 3, 4};
    const unsigned sa = sizeof(ia)/sizeof(ia[0]);
    int ib[sa] = {0};
    ranges::replace_copy_if_result<InIter, OutIter> r = ranges::replace_copy_if(InIter(ia), Sent(ia+sa), OutIter(ib),
        [](int i){return 2==i;}, 5);
    CHECK(base(r.in) == ia + sa);
    CHECK(base(r.out) == ib + sa);
    CHECK(ib[0] == 0);
    CHECK(ib[1] == 1);
    CHECK(ib[2] == 5);
    CHECK(ib[3] == 3);
    CHECK(ib[4] == 4);
}

template<class InIter, class OutIter, class Sent = InIter>
void test_rng()
{
    int ia[] = {0, 1, 2, 3, 4};
    const unsigned sa = sizeof(ia)/sizeof(ia[0]);
    int ib[sa] = {0};
    auto rng = ranges::make_subrange(InIter(ia), Sent(ia+sa));
    ranges::replace_copy_if_result<InIter, OutIter> r = ranges::replace_copy_if(rng, OutIter(ib),
        [](int i){return 2==i;}, 5);
    CHECK(base(r.in) == ia + sa);
    CHECK(base(r.out) == ib + sa);
    CHECK(ib[0] == 0);
    CHECK(ib[1] == 1);
    CHECK(ib[2] == 5);
    CHECK(ib[3] == 3);
    CHECK(ib[4] == 4);
}

template<class InIter, class OutIter>
void test()
{
    using Sent = typename sentinel_type<InIter>::type;
    test_iter<InIter, OutIter>();
    test_iter<InIter, OutIter>();
    test_rng<InIter, OutIter, Sent>();
    test_rng<InIter, OutIter, Sent>();
}

constexpr bool equals_two(int i)
{
    return i == 2;
}

constexpr bool test_constexpr()
{
    using namespace ranges;
    int ia[] = {0, 1, 2, 3, 4};
    int ib[5] = {0};
    constexpr auto sa = ranges::size(ia);
    const auto r = ranges::replace_copy_if(ia, ib, equals_two, 42);
    STATIC_CHECK_RETURN(r.in == ia + sa);
    STATIC_CHECK_RETURN(r.out == ib + sa);
    STATIC_CHECK_RETURN(ib[0] == 0);
    STATIC_CHECK_RETURN(ib[1] == 1);
    STATIC_CHECK_RETURN(ib[2] == 42);
    STATIC_CHECK_RETURN(ib[3] == 3);
    STATIC_CHECK_RETURN(ib[4] == 4);
    return true;
}

int main()
{
    test<InputIterator<const int*>, OutputIterator<int*> >();
    test<InputIterator<const int*>, ForwardIterator<int*> >();
    test<InputIterator<const int*>, BidirectionalIterator<int*> >();
    test<InputIterator<const int*>, RandomAccessIterator<int*> >();
    test<InputIterator<const int*>, int*>();

    test<ForwardIterator<const int*>, OutputIterator<int*> >();
    test<ForwardIterator<const int*>, ForwardIterator<int*> >();
    test<ForwardIterator<const int*>, BidirectionalIterator<int*> >();
    test<ForwardIterator<const int*>, RandomAccessIterator<int*> >();
    test<ForwardIterator<const int*>, int*>();

    test<BidirectionalIterator<const int*>, OutputIterator<int*> >();
    test<BidirectionalIterator<const int*>, ForwardIterator<int*> >();
    test<BidirectionalIterator<const int*>, BidirectionalIterator<int*> >();
    test<BidirectionalIterator<const int*>, RandomAccessIterator<int*> >();
    test<BidirectionalIterator<const int*>, int*>();

    test<RandomAccessIterator<const int*>, OutputIterator<int*> >();
    test<RandomAccessIterator<const int*>, ForwardIterator<int*> >();
    test<RandomAccessIterator<const int*>, BidirectionalIterator<int*> >();
    test<RandomAccessIterator<const int*>, RandomAccessIterator<int*> >();
    test<RandomAccessIterator<const int*>, int*>();

    test<const int*, OutputIterator<int*> >();
    test<const int*, ForwardIterator<int*> >();
    test<const int*, BidirectionalIterator<int*> >();
    test<const int*, RandomAccessIterator<int*> >();
    test<const int*, int*>();

    // Test projection
    {
        using P = std::pair<int, std::string>;
        P in[] = {{0, "0"}, {1, "1"}, {2, "2"}, {3, "3"}, {4, "4"}};
        P out[ranges::size(in)] = {};
        ranges::replace_copy_if_result<P *, P *> r = ranges::replace_copy_if(in, out,
            [](int i){return 2==i;}, P{5, "5"}, &std::pair<int, std::string>::first);
        CHECK(r.in == ranges::end(in));
        CHECK(r.out == ranges::end(out));
        CHECK(out[0] == P{0, "0"});
        CHECK(out[1] == P{1, "1"});
        CHECK(out[2] == P{5, "5"});
        CHECK(out[3] == P{3, "3"});
        CHECK(out[4] == P{4, "4"});
    }

    {
        STATIC_CHECK(test_constexpr());
    }

    return ::test_result();
}
