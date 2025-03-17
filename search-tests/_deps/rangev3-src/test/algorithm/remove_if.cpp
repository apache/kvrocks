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

#include <functional>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/remove_if.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

template<class Iter, class Sent = Iter>
void
test_iter()
{
    int ia[] = {0, 1, 2, 3, 4, 2, 3, 4, 2};
    constexpr auto sa = ranges::size(ia);
    using namespace std::placeholders;
    Iter r = ranges::remove_if(Iter(ia), Sent(ia+sa), std::bind(std::equal_to<int>(), _1, 2));
    CHECK(base(r) == ia + sa-3);
    CHECK(ia[0] == 0);
    CHECK(ia[1] == 1);
    CHECK(ia[2] == 3);
    CHECK(ia[3] == 4);
    CHECK(ia[4] == 3);
    CHECK(ia[5] == 4);
}

template<class Iter, class Sent = Iter>
void
test_range()
{
    int ia[] = {0, 1, 2, 3, 4, 2, 3, 4, 2};
    constexpr auto sa = ranges::size(ia);
    using namespace std::placeholders;
    Iter r = ranges::remove_if(::as_lvalue(ranges::make_subrange(Iter(ia), Sent(ia+sa))), std::bind(std::equal_to<int>(), _1, 2));
    CHECK(base(r) == ia + sa-3);
    CHECK(ia[0] == 0);
    CHECK(ia[1] == 1);
    CHECK(ia[2] == 3);
    CHECK(ia[3] == 4);
    CHECK(ia[4] == 3);
    CHECK(ia[5] == 4);
}

struct pred
{
    bool operator()(const std::unique_ptr<int>& i) {return *i == 2;}
};

template<class Iter, class Sent = Iter>
void
test_iter_rvalue()
{
    constexpr unsigned sa = 9;
    std::unique_ptr<int> ia[sa];
    ia[0].reset(new int(0));
    ia[1].reset(new int(1));
    ia[2].reset(new int(2));
    ia[3].reset(new int(3));
    ia[4].reset(new int(4));
    ia[5].reset(new int(2));
    ia[6].reset(new int(3));
    ia[7].reset(new int(4));
    ia[8].reset(new int(2));
    Iter r = ranges::remove_if(Iter(ia), Sent(ia+sa), pred());
    CHECK(base(r) == ia + sa-3);
    CHECK(*ia[0] == 0);
    CHECK(*ia[1] == 1);
    CHECK(*ia[2] == 3);
    CHECK(*ia[3] == 4);
    CHECK(*ia[4] == 3);
    CHECK(*ia[5] == 4);
}

template<class Iter, class Sent = Iter>
void
test_range_rvalue()
{
    constexpr unsigned sa = 9;
    std::unique_ptr<int> ia[sa];
    ia[0].reset(new int(0));
    ia[1].reset(new int(1));
    ia[2].reset(new int(2));
    ia[3].reset(new int(3));
    ia[4].reset(new int(4));
    ia[5].reset(new int(2));
    ia[6].reset(new int(3));
    ia[7].reset(new int(4));
    ia[8].reset(new int(2));
    Iter r = ranges::remove_if(::as_lvalue(ranges::make_subrange(Iter(ia), Sent(ia+sa))), pred());
    CHECK(base(r) == ia + sa-3);
    CHECK(*ia[0] == 0);
    CHECK(*ia[1] == 1);
    CHECK(*ia[2] == 3);
    CHECK(*ia[3] == 4);
    CHECK(*ia[4] == 3);
    CHECK(*ia[5] == 4);
}

struct S
{
    int i;
};

constexpr bool equals_two(int i)
{
    return i == 2;
}

constexpr bool test_constexpr()
{
    using namespace ranges;
    int ia[] = {0, 1, 2, 3, 4, 2, 3, 4, 2};
    constexpr auto sa = ranges::size(ia);
    auto r = ranges::remove_if(ia, equals_two);
    STATIC_CHECK_RETURN(r == ia + sa - 3);
    STATIC_CHECK_RETURN(ia[0] == 0);
    STATIC_CHECK_RETURN(ia[1] == 1);
    STATIC_CHECK_RETURN(ia[2] == 3);
    STATIC_CHECK_RETURN(ia[3] == 4);
    STATIC_CHECK_RETURN(ia[4] == 3);
    STATIC_CHECK_RETURN(ia[5] == 4);
    return true;
}

int main()
{
    test_iter<ForwardIterator<int*> >();
    test_iter<BidirectionalIterator<int*> >();
    test_iter<RandomAccessIterator<int*> >();
    test_iter<int*>();
    test_iter<ForwardIterator<int*>, Sentinel<int*>>();
    test_iter<BidirectionalIterator<int*>, Sentinel<int*>>();
    test_iter<RandomAccessIterator<int*>, Sentinel<int*>>();

    test_range<ForwardIterator<int*> >();
    test_range<BidirectionalIterator<int*> >();
    test_range<RandomAccessIterator<int*> >();
    test_range<int*>();
    test_range<ForwardIterator<int*>, Sentinel<int*>>();
    test_range<BidirectionalIterator<int*>, Sentinel<int*>>();
    test_range<RandomAccessIterator<int*>, Sentinel<int*>>();

    test_iter_rvalue<ForwardIterator<std::unique_ptr<int>*> >();
    test_iter_rvalue<BidirectionalIterator<std::unique_ptr<int>*> >();
    test_iter_rvalue<RandomAccessIterator<std::unique_ptr<int>*> >();
    test_iter_rvalue<std::unique_ptr<int>*>();
    test_iter_rvalue<ForwardIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*>>();
    test_iter_rvalue<BidirectionalIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*>>();
    test_iter_rvalue<RandomAccessIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*>>();

    test_range_rvalue<ForwardIterator<std::unique_ptr<int>*> >();
    test_range_rvalue<BidirectionalIterator<std::unique_ptr<int>*> >();
    test_range_rvalue<RandomAccessIterator<std::unique_ptr<int>*> >();
    test_range_rvalue<std::unique_ptr<int>*>();
    test_range_rvalue<ForwardIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*>>();
    test_range_rvalue<BidirectionalIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*>>();
    test_range_rvalue<RandomAccessIterator<std::unique_ptr<int>*>, Sentinel<std::unique_ptr<int>*>>();

    {
        // Check projection
        S ia[] = {S{0}, S{1}, S{2}, S{3}, S{4}, S{2}, S{3}, S{4}, S{2}};
        constexpr auto sa = ranges::size(ia);
        using namespace std::placeholders;
        S* r = ranges::remove_if(ia, std::bind(std::equal_to<int>(), _1, 2), &S::i);
        CHECK(r == ia + sa-3);
        CHECK(ia[0].i == 0);
        CHECK(ia[1].i == 1);
        CHECK(ia[2].i == 3);
        CHECK(ia[3].i == 4);
        CHECK(ia[4].i == 3);
        CHECK(ia[5].i == 4);
    }

    {
        // Check rvalue ranges
        S ia[] = {S{0}, S{1}, S{2}, S{3}, S{4}, S{2}, S{3}, S{4}, S{2}};
        using namespace std::placeholders;
        auto r0 = ranges::remove_if(std::move(ia), std::bind(std::equal_to<int>(), _1, 2), &S::i);
#ifndef RANGES_WORKAROUND_MSVC_573728
        static_assert(std::is_same<decltype(r0), ranges::dangling>::value, "");
#endif // RANGES_WORKAROUND_MSVC_573728
        CHECK(ia[0].i == 0);
        CHECK(ia[1].i == 1);
        CHECK(ia[2].i == 3);
        CHECK(ia[3].i == 4);
        CHECK(ia[4].i == 3);
        CHECK(ia[5].i == 4);

        std::vector<S> vec{S{0}, S{1}, S{2}, S{3}, S{4}, S{2}, S{3}, S{4}, S{2}};
        auto r1 = ranges::remove_if(std::move(vec), std::bind(std::equal_to<int>(), _1, 2), &S::i);
        static_assert(std::is_same<decltype(r1), ranges::dangling>::value, "");
        CHECK(vec[0].i == 0);
        CHECK(vec[1].i == 1);
        CHECK(vec[2].i == 3);
        CHECK(vec[3].i == 4);
        CHECK(vec[4].i == 3);
        CHECK(vec[5].i == 4);
    }

    {
        STATIC_CHECK(test_constexpr());
    }

    return ::test_result();
}
