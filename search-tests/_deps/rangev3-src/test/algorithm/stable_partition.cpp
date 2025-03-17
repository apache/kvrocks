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
#include <utility>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/stable_partition.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

struct is_odd
{
    bool operator()(const int& i) const
    {
        return i & 1;
    }
};

struct odd_first
{
    bool operator()(const std::pair<int,int>& p) const
    {
        return p.first & 1;
    }
};

template<class Iter, class Sent = Iter>
void
test_iter()
{
    using P = std::pair<int, int>;
    {  // check mixed
        P ap[] = { {0, 1}, {0, 2}, {1, 1}, {1, 2}, {2, 1}, {2, 2}, {3, 1}, {3, 2}, {4, 1}, {4, 2} };
        std::size_t size = ranges::size(ap);
        Iter r = ranges::stable_partition(Iter(ap), Sent(ap+size), odd_first());
        CHECK(base(r) == ap + 4);
        CHECK(ap[0] == P{1, 1});
        CHECK(ap[1] == P{1, 2});
        CHECK(ap[2] == P{3, 1});
        CHECK(ap[3] == P{3, 2});
        CHECK(ap[4] == P{0, 1});
        CHECK(ap[5] == P{0, 2});
        CHECK(ap[6] == P{2, 1});
        CHECK(ap[7] == P{2, 2});
        CHECK(ap[8] == P{4, 1});
        CHECK(ap[9] == P{4, 2});
    }
    {
        P ap[] = { {0, 1}, {0, 2}, {1, 1}, {1, 2}, {2, 1}, {2, 2}, {3, 1}, {3, 2}, {4, 1}, {4, 2} };
        std::size_t size = ranges::size(ap);
        Iter r = ranges::stable_partition(Iter(ap), Sent(ap+size), odd_first());
        CHECK(base(r) == ap + 4);
        CHECK(ap[0] == P{1, 1});
        CHECK(ap[1] == P{1, 2});
        CHECK(ap[2] == P{3, 1});
        CHECK(ap[3] == P{3, 2});
        CHECK(ap[4] == P{0, 1});
        CHECK(ap[5] == P{0, 2});
        CHECK(ap[6] == P{2, 1});
        CHECK(ap[7] == P{2, 2});
        CHECK(ap[8] == P{4, 1});
        CHECK(ap[9] == P{4, 2});
        // check empty
        r = ranges::stable_partition(Iter(ap), Sent(ap), odd_first());
        CHECK(base(r) == ap);
        // check one true
        r = ranges::stable_partition(Iter(ap), Sent(ap+1), odd_first());
        CHECK(base(r) == ap+1);
        CHECK(ap[0] == P{1, 1});
        // check one false
        r = ranges::stable_partition(Iter(ap+4), Sent(ap+5), odd_first());
        CHECK(base(r) == ap+4);
        CHECK(ap[4] == P{0, 1});
    }
    {  // check all false
        P ap[] = { {0, 1}, {0, 2}, {2, 1}, {2, 2}, {4, 1}, {4, 2}, {6, 1}, {6, 2}, {8, 1}, {8, 2} };
        std::size_t size = ranges::size(ap);
        Iter r = ranges::stable_partition(Iter(ap), Sent(ap+size), odd_first());
        CHECK(base(r) == ap);
        CHECK(ap[0] == P{0, 1});
        CHECK(ap[1] == P{0, 2});
        CHECK(ap[2] == P{2, 1});
        CHECK(ap[3] == P{2, 2});
        CHECK(ap[4] == P{4, 1});
        CHECK(ap[5] == P{4, 2});
        CHECK(ap[6] == P{6, 1});
        CHECK(ap[7] == P{6, 2});
        CHECK(ap[8] == P{8, 1});
        CHECK(ap[9] == P{8, 2});
    }
    {  // check all true
        P ap[] = { {1, 1}, {1, 2}, {3, 1}, {3, 2}, {5, 1}, {5, 2}, {7, 1}, {7, 2}, {9, 1}, {9, 2} };
        std::size_t size = ranges::size(ap);
        Iter r = ranges::stable_partition(Iter(ap), Sent(ap+size), odd_first());
        CHECK(base(r) == ap + size);
        CHECK(ap[0] == P{1, 1});
        CHECK(ap[1] == P{1, 2});
        CHECK(ap[2] == P{3, 1});
        CHECK(ap[3] == P{3, 2});
        CHECK(ap[4] == P{5, 1});
        CHECK(ap[5] == P{5, 2});
        CHECK(ap[6] == P{7, 1});
        CHECK(ap[7] == P{7, 2});
        CHECK(ap[8] == P{9, 1});
        CHECK(ap[9] == P{9, 2});
    }
    {  // check all false but first true
        P ap[] = { {1, 1}, {0, 2}, {2, 1}, {2, 2}, {4, 1}, {4, 2}, {6, 1}, {6, 2}, {8, 1}, {8, 2} };
        std::size_t size = ranges::size(ap);
        Iter r = ranges::stable_partition(Iter(ap), Sent(ap+size), odd_first());
        CHECK(base(r) == ap + 1);
        CHECK(ap[0] == P{1, 1});
        CHECK(ap[1] == P{0, 2});
        CHECK(ap[2] == P{2, 1});
        CHECK(ap[3] == P{2, 2});
        CHECK(ap[4] == P{4, 1});
        CHECK(ap[5] == P{4, 2});
        CHECK(ap[6] == P{6, 1});
        CHECK(ap[7] == P{6, 2});
        CHECK(ap[8] == P{8, 1});
        CHECK(ap[9] == P{8, 2});
    }
    {  // check all false but last true
        P ap[] = { {0, 1}, {0, 2}, {2, 1}, {2, 2}, {4, 1}, {4, 2}, {6, 1}, {6, 2}, {8, 1}, {1, 2} };
        std::size_t size = ranges::size(ap);
        Iter r = ranges::stable_partition(Iter(ap), Sent(ap+size), odd_first());
        CHECK(base(r) == ap + 1);
        CHECK(ap[0] == P{1, 2});
        CHECK(ap[1] == P{0, 1});
        CHECK(ap[2] == P{0, 2});
        CHECK(ap[3] == P{2, 1});
        CHECK(ap[4] == P{2, 2});
        CHECK(ap[5] == P{4, 1});
        CHECK(ap[6] == P{4, 2});
        CHECK(ap[7] == P{6, 1});
        CHECK(ap[8] == P{6, 2});
        CHECK(ap[9] == P{8, 1});
    }
    {  // check all true but first false
        P ap[] = { {0, 1}, {1, 2}, {3, 1}, {3, 2}, {5, 1}, {5, 2}, {7, 1}, {7, 2}, {9, 1}, {9, 2} };
        std::size_t size = ranges::size(ap);
        Iter r = ranges::stable_partition(Iter(ap), Sent(ap+size), odd_first());
        CHECK(base(r) == ap + size-1);
        CHECK(ap[0] == P{1, 2});
        CHECK(ap[1] == P{3, 1});
        CHECK(ap[2] == P{3, 2});
        CHECK(ap[3] == P{5, 1});
        CHECK(ap[4] == P{5, 2});
        CHECK(ap[5] == P{7, 1});
        CHECK(ap[6] == P{7, 2});
        CHECK(ap[7] == P{9, 1});
        CHECK(ap[8] == P{9, 2});
        CHECK(ap[9] == P{0, 1});
    }
    {  // check all true but last false
        P ap[] = { {1, 1}, {1, 2}, {3, 1}, {3, 2}, {5, 1}, {5, 2}, {7, 1}, {7, 2}, {9, 1}, {0, 2} };
        std::size_t size = ranges::size(ap);
        Iter r = ranges::stable_partition(Iter(ap), Sent(ap+size), odd_first());
        CHECK(base(r) == ap + size-1);
        CHECK(ap[0] == P{1, 1});
        CHECK(ap[1] == P{1, 2});
        CHECK(ap[2] == P{3, 1});
        CHECK(ap[3] == P{3, 2});
        CHECK(ap[4] == P{5, 1});
        CHECK(ap[5] == P{5, 2});
        CHECK(ap[6] == P{7, 1});
        CHECK(ap[7] == P{7, 2});
        CHECK(ap[8] == P{9, 1});
        CHECK(ap[9] == P{0, 2});
    }
}

template<class Iter, class Sent = Iter>
void
test_range()
{
    using P = std::pair<int, int>;
    {  // check mixed
        P ap[] = { {0, 1}, {0, 2}, {1, 1}, {1, 2}, {2, 1}, {2, 2}, {3, 1}, {3, 2}, {4, 1}, {4, 2} };
        std::size_t size = ranges::size(ap);
        Iter r = ranges::stable_partition(::as_lvalue(ranges::make_subrange(Iter(ap), Sent(ap+size))), odd_first());
        CHECK(base(r) == ap + 4);
        CHECK(ap[0] == P{1, 1});
        CHECK(ap[1] == P{1, 2});
        CHECK(ap[2] == P{3, 1});
        CHECK(ap[3] == P{3, 2});
        CHECK(ap[4] == P{0, 1});
        CHECK(ap[5] == P{0, 2});
        CHECK(ap[6] == P{2, 1});
        CHECK(ap[7] == P{2, 2});
        CHECK(ap[8] == P{4, 1});
        CHECK(ap[9] == P{4, 2});
    }
    {
        P ap[] = { {0, 1}, {0, 2}, {1, 1}, {1, 2}, {2, 1}, {2, 2}, {3, 1}, {3, 2}, {4, 1}, {4, 2} };
        std::size_t size = ranges::size(ap);
        Iter r = ranges::stable_partition(::as_lvalue(ranges::make_subrange(Iter(ap), Sent(ap+size))), odd_first());
        CHECK(base(r) == ap + 4);
        CHECK(ap[0] == P{1, 1});
        CHECK(ap[1] == P{1, 2});
        CHECK(ap[2] == P{3, 1});
        CHECK(ap[3] == P{3, 2});
        CHECK(ap[4] == P{0, 1});
        CHECK(ap[5] == P{0, 2});
        CHECK(ap[6] == P{2, 1});
        CHECK(ap[7] == P{2, 2});
        CHECK(ap[8] == P{4, 1});
        CHECK(ap[9] == P{4, 2});
        // check empty
        r = ranges::stable_partition(::as_lvalue(ranges::make_subrange(Iter(ap), Sent(ap))), odd_first());
        CHECK(base(r) == ap);
        // check one true
        r = ranges::stable_partition(::as_lvalue(ranges::make_subrange(Iter(ap), Sent(ap+1))), odd_first());
        CHECK(base(r) == ap+1);
        CHECK(ap[0] == P{1, 1});
        // check one false
        r = ranges::stable_partition(::as_lvalue(ranges::make_subrange(Iter(ap+4), Sent(ap+5))), odd_first());
        CHECK(base(r) == ap+4);
        CHECK(ap[4] == P{0, 1});
    }
    {  // check all false
        P ap[] = { {0, 1}, {0, 2}, {2, 1}, {2, 2}, {4, 1}, {4, 2}, {6, 1}, {6, 2}, {8, 1}, {8, 2} };
        std::size_t size = ranges::size(ap);
        Iter r = ranges::stable_partition(::as_lvalue(ranges::make_subrange(Iter(ap), Sent(ap+size))), odd_first());
        CHECK(base(r) == ap);
        CHECK(ap[0] == P{0, 1});
        CHECK(ap[1] == P{0, 2});
        CHECK(ap[2] == P{2, 1});
        CHECK(ap[3] == P{2, 2});
        CHECK(ap[4] == P{4, 1});
        CHECK(ap[5] == P{4, 2});
        CHECK(ap[6] == P{6, 1});
        CHECK(ap[7] == P{6, 2});
        CHECK(ap[8] == P{8, 1});
        CHECK(ap[9] == P{8, 2});
    }
    {  // check all true
        P ap[] = { {1, 1}, {1, 2}, {3, 1}, {3, 2}, {5, 1}, {5, 2}, {7, 1}, {7, 2}, {9, 1}, {9, 2} };
        std::size_t size = ranges::size(ap);
        Iter r = ranges::stable_partition(::as_lvalue(ranges::make_subrange(Iter(ap), Sent(ap+size))), odd_first());
        CHECK(base(r) == ap + size);
        CHECK(ap[0] == P{1, 1});
        CHECK(ap[1] == P{1, 2});
        CHECK(ap[2] == P{3, 1});
        CHECK(ap[3] == P{3, 2});
        CHECK(ap[4] == P{5, 1});
        CHECK(ap[5] == P{5, 2});
        CHECK(ap[6] == P{7, 1});
        CHECK(ap[7] == P{7, 2});
        CHECK(ap[8] == P{9, 1});
        CHECK(ap[9] == P{9, 2});
    }
    {  // check all false but first true
        P ap[] = { {1, 1}, {0, 2}, {2, 1}, {2, 2}, {4, 1}, {4, 2}, {6, 1}, {6, 2}, {8, 1}, {8, 2} };
        std::size_t size = ranges::size(ap);
        Iter r = ranges::stable_partition(::as_lvalue(ranges::make_subrange(Iter(ap), Sent(ap+size))), odd_first());
        CHECK(base(r) == ap + 1);
        CHECK(ap[0] == P{1, 1});
        CHECK(ap[1] == P{0, 2});
        CHECK(ap[2] == P{2, 1});
        CHECK(ap[3] == P{2, 2});
        CHECK(ap[4] == P{4, 1});
        CHECK(ap[5] == P{4, 2});
        CHECK(ap[6] == P{6, 1});
        CHECK(ap[7] == P{6, 2});
        CHECK(ap[8] == P{8, 1});
        CHECK(ap[9] == P{8, 2});
    }
    {  // check all false but last true
        P ap[] = { {0, 1}, {0, 2}, {2, 1}, {2, 2}, {4, 1}, {4, 2}, {6, 1}, {6, 2}, {8, 1}, {1, 2} };
        std::size_t size = ranges::size(ap);
        Iter r = ranges::stable_partition(::as_lvalue(ranges::make_subrange(Iter(ap), Sent(ap+size))), odd_first());
        CHECK(base(r) == ap + 1);
        CHECK(ap[0] == P{1, 2});
        CHECK(ap[1] == P{0, 1});
        CHECK(ap[2] == P{0, 2});
        CHECK(ap[3] == P{2, 1});
        CHECK(ap[4] == P{2, 2});
        CHECK(ap[5] == P{4, 1});
        CHECK(ap[6] == P{4, 2});
        CHECK(ap[7] == P{6, 1});
        CHECK(ap[8] == P{6, 2});
        CHECK(ap[9] == P{8, 1});
    }
    {  // check all true but first false
        P ap[] = { {0, 1}, {1, 2}, {3, 1}, {3, 2}, {5, 1}, {5, 2}, {7, 1}, {7, 2}, {9, 1}, {9, 2} };
        std::size_t size = ranges::size(ap);
        Iter r = ranges::stable_partition(::as_lvalue(ranges::make_subrange(Iter(ap), Sent(ap+size))), odd_first());
        CHECK(base(r) == ap + size-1);
        CHECK(ap[0] == P{1, 2});
        CHECK(ap[1] == P{3, 1});
        CHECK(ap[2] == P{3, 2});
        CHECK(ap[3] == P{5, 1});
        CHECK(ap[4] == P{5, 2});
        CHECK(ap[5] == P{7, 1});
        CHECK(ap[6] == P{7, 2});
        CHECK(ap[7] == P{9, 1});
        CHECK(ap[8] == P{9, 2});
        CHECK(ap[9] == P{0, 1});
    }
    {  // check all true but last false
        P ap[] = { {1, 1}, {1, 2}, {3, 1}, {3, 2}, {5, 1}, {5, 2}, {7, 1}, {7, 2}, {9, 1}, {0, 2} };
        std::size_t size = ranges::size(ap);
        Iter r = ranges::stable_partition(::as_lvalue(ranges::make_subrange(Iter(ap), Sent(ap+size))), odd_first());
        CHECK(base(r) == ap + size-1);
        CHECK(ap[0] == P{1, 1});
        CHECK(ap[1] == P{1, 2});
        CHECK(ap[2] == P{3, 1});
        CHECK(ap[3] == P{3, 2});
        CHECK(ap[4] == P{5, 1});
        CHECK(ap[5] == P{5, 2});
        CHECK(ap[6] == P{7, 1});
        CHECK(ap[7] == P{7, 2});
        CHECK(ap[8] == P{9, 1});
        CHECK(ap[9] == P{0, 2});
    }
}

struct move_only
{
    static int count;
    int i;
    move_only() = delete;
    move_only(int j) : i(j) { ++count; }
    move_only(move_only &&that) : i(that.i) {  ++count; }
    move_only(move_only const &) = delete;
    ~move_only() { --count; }
    move_only &operator=(move_only &&) = default;
    move_only &operator=(move_only const &) = delete;
};

int move_only::count = 0;

template<class Iter>
void
test_move_only()
{
    const unsigned size = 5;
    move_only array[size] = { 1, 2, 3, 4, 5 };
    Iter r = ranges::stable_partition(Iter(array), Iter(array+size), is_odd{}, &move_only::i);
    CHECK(base(r) == array + 3);
    CHECK(array[0].i == 1);
    CHECK(array[1].i == 3);
    CHECK(array[2].i == 5);
    CHECK(array[3].i == 2);
    CHECK(array[4].i == 4);
}

struct S
{
    std::pair<int,int> p;
};

int main()
{
    test_iter<BidirectionalIterator<std::pair<int,int>*> >();
    test_iter<RandomAccessIterator<std::pair<int,int>*> >();
    test_iter<std::pair<int,int>*>();
    test_iter<BidirectionalIterator<std::pair<int,int>*>, Sentinel<std::pair<int,int>*> >();
    test_iter<RandomAccessIterator<std::pair<int,int>*>, Sentinel<std::pair<int,int>*> >();

    test_range<BidirectionalIterator<std::pair<int,int>*> >();
    test_range<RandomAccessIterator<std::pair<int,int>*> >();
    test_range<std::pair<int,int>*>();
    test_range<BidirectionalIterator<std::pair<int,int>*>, Sentinel<std::pair<int,int>*> >();
    test_range<RandomAccessIterator<std::pair<int,int>*>, Sentinel<std::pair<int,int>*> >();

    CHECK(move_only::count == 0);
    test_move_only<BidirectionalIterator<move_only*> >();
    CHECK(move_only::count == 0);

    // Test projections
    using P = std::pair<int, int>;
    {  // check mixed
        S ap[] = { {{0, 1}}, {{0, 2}}, {{1, 1}}, {{1, 2}}, {{2, 1}}, {{2, 2}}, {{3, 1}}, {{3, 2}}, {{4, 1}}, {{4, 2}} };
        S* r = ranges::stable_partition(ap, odd_first(), &S::p);
        CHECK(r == ap + 4);
        CHECK(ap[0].p == P{1, 1});
        CHECK(ap[1].p == P{1, 2});
        CHECK(ap[2].p == P{3, 1});
        CHECK(ap[3].p == P{3, 2});
        CHECK(ap[4].p == P{0, 1});
        CHECK(ap[5].p == P{0, 2});
        CHECK(ap[6].p == P{2, 1});
        CHECK(ap[7].p == P{2, 2});
        CHECK(ap[8].p == P{4, 1});
        CHECK(ap[9].p == P{4, 2});
    }

    // Test rvalue ranges
    using P = std::pair<int, int>;
    {  // check mixed
        S ap[] = { {{0, 1}}, {{0, 2}}, {{1, 1}}, {{1, 2}}, {{2, 1}}, {{2, 2}}, {{3, 1}}, {{3, 2}}, {{4, 1}}, {{4, 2}} };
        auto r = ranges::stable_partition(ranges::views::all(ap), odd_first(), &S::p);
        CHECK(r == ap + 4);
        CHECK(ap[0].p == P{1, 1});
        CHECK(ap[1].p == P{1, 2});
        CHECK(ap[2].p == P{3, 1});
        CHECK(ap[3].p == P{3, 2});
        CHECK(ap[4].p == P{0, 1});
        CHECK(ap[5].p == P{0, 2});
        CHECK(ap[6].p == P{2, 1});
        CHECK(ap[7].p == P{2, 2});
        CHECK(ap[8].p == P{4, 1});
        CHECK(ap[9].p == P{4, 2});
    }
    {  // check mixed
        S ap[] = { {{0, 1}}, {{0, 2}}, {{1, 1}}, {{1, 2}}, {{2, 1}}, {{2, 2}}, {{3, 1}}, {{3, 2}}, {{4, 1}}, {{4, 2}} };
        auto r = ranges::stable_partition(std::move(ap), odd_first(), &S::p);
#ifndef RANGES_WORKAROUND_MSVC_573728
        CHECK(::is_dangling(r));
#endif // RANGES_WORKAROUND_MSVC_573728
        CHECK(ap[0].p == P{1, 1});
        CHECK(ap[1].p == P{1, 2});
        CHECK(ap[2].p == P{3, 1});
        CHECK(ap[3].p == P{3, 2});
        CHECK(ap[4].p == P{0, 1});
        CHECK(ap[5].p == P{0, 2});
        CHECK(ap[6].p == P{2, 1});
        CHECK(ap[7].p == P{2, 2});
        CHECK(ap[8].p == P{4, 1});
        CHECK(ap[9].p == P{4, 2});
    }
    {  // check mixed
        std::vector<S> ap{ {{0, 1}}, {{0, 2}}, {{1, 1}}, {{1, 2}}, {{2, 1}}, {{2, 2}}, {{3, 1}}, {{3, 2}}, {{4, 1}}, {{4, 2}} };
        auto r = ranges::stable_partition(std::move(ap), odd_first(), &S::p);
        CHECK(::is_dangling(r));
        CHECK(ap[0].p == P{1, 1});
        CHECK(ap[1].p == P{1, 2});
        CHECK(ap[2].p == P{3, 1});
        CHECK(ap[3].p == P{3, 2});
        CHECK(ap[4].p == P{0, 1});
        CHECK(ap[5].p == P{0, 2});
        CHECK(ap[6].p == P{2, 1});
        CHECK(ap[7].p == P{2, 2});
        CHECK(ap[8].p == P{4, 1});
        CHECK(ap[9].p == P{4, 2});
    }

    return ::test_result();
}
