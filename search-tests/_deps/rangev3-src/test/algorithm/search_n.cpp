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

#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/search_n.hpp>
#include <range/v3/view/counted.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

template<class Iter, typename Sent = Iter>
void
test_iter_impl()
{
    int ia[] = {0, 1, 2, 3, 4, 5};
    const unsigned sa = sizeof(ia)/sizeof(ia[0]);
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 0, 0).begin() == Iter(ia));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 0, 0).end() == Iter(ia));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 1, 0).begin() == Iter(ia+0));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 1, 0).end() == Iter(ia+1));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 2, 0).begin() == Iter(ia+sa));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 2, 0).end() == Iter(ia+sa));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), sa, 0).begin() == Iter(ia+sa));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), sa, 0).end() == Iter(ia+sa));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 0, 3).begin() == Iter(ia));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 0, 3).end() == Iter(ia));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 1, 3).begin() == Iter(ia+3));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 1, 3).end() == Iter(ia+4));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 2, 3).begin() == Iter(ia+sa));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 2, 3).end() == Iter(ia+sa));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), sa, 3).begin() == Iter(ia+sa));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), sa, 3).end() == Iter(ia+sa));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 0, 5).begin() == Iter(ia));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 0, 5).end() == Iter(ia));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 1, 5).begin() == Iter(ia+5));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 1, 5).end() == Iter(ia+6));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 2, 5).begin() == Iter(ia+sa));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), 2, 5).end() == Iter(ia+sa));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), sa, 5).begin() == Iter(ia+sa));
    CHECK(ranges::search_n(Iter(ia), Sent(ia+sa), sa, 5).end() == Iter(ia+sa));

    int ib[] = {0, 0, 1, 1, 2, 2};
    const unsigned sb = sizeof(ib)/sizeof(ib[0]);
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 0, 0).begin() == Iter(ib));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 0, 0).end() == Iter(ib));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 1, 0).begin() == Iter(ib+0));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 1, 0).end() == Iter(ib+1));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 2, 0).begin() == Iter(ib+0));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 2, 0).end() == Iter(ib+2));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 3, 0).begin() == Iter(ib+sb));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 3, 0).end() == Iter(ib+sb));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), sb, 0).begin() == Iter(ib+sb));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), sb, 0).end() == Iter(ib+sb));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 0, 1).begin() == Iter(ib));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 0, 1).end() == Iter(ib));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 1, 1).begin() == Iter(ib+2));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 1, 1).end() == Iter(ib+3));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 2, 1).begin() == Iter(ib+2));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 2, 1).end() == Iter(ib+4));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 3, 1).begin() == Iter(ib+sb));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 3, 1).end() == Iter(ib+sb));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), sb, 1).begin() == Iter(ib+sb));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), sb, 1).end() == Iter(ib+sb));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 0, 2).begin() == Iter(ib));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 0, 2).end() == Iter(ib));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 1, 2).begin() == Iter(ib+4));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 1, 2).end() == Iter(ib+5));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 2, 2).begin() == Iter(ib+4));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 2, 2).end() == Iter(ib+6));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 3, 2).begin() == Iter(ib+sb));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), 3, 2).end() == Iter(ib+sb));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), sb, 2).begin() == Iter(ib+sb));
    CHECK(ranges::search_n(Iter(ib), Sent(ib+sb), sb, 2).end() == Iter(ib+sb));

    int ic[] = {0, 0, 0};
    const unsigned sc = sizeof(ic)/sizeof(ic[0]);
    CHECK(ranges::search_n(Iter(ic), Sent(ic+sc), 0, 0).begin() == Iter(ic));
    CHECK(ranges::search_n(Iter(ic), Sent(ic+sc), 0, 0).end() == Iter(ic));
    CHECK(ranges::search_n(Iter(ic), Sent(ic+sc), 1, 0).begin() == Iter(ic));
    CHECK(ranges::search_n(Iter(ic), Sent(ic+sc), 1, 0).end() == Iter(ic+1));
    CHECK(ranges::search_n(Iter(ic), Sent(ic+sc), 2, 0).begin() == Iter(ic));
    CHECK(ranges::search_n(Iter(ic), Sent(ic+sc), 2, 0).end() == Iter(ic+2));
    CHECK(ranges::search_n(Iter(ic), Sent(ic+sc), 3, 0).begin() == Iter(ic));
    CHECK(ranges::search_n(Iter(ic), Sent(ic+sc), 3, 0).end() == Iter(ic+3));
    CHECK(ranges::search_n(Iter(ic), Sent(ic+sc), 4, 0).begin() == Iter(ic+sc));
    CHECK(ranges::search_n(Iter(ic), Sent(ic+sc), 4, 0).end() == Iter(ic+sc));
}

template<class Iter, class Iter2>
void
test_iter()
{
    using Sent = typename sentinel_type<Iter>::type;
    test_iter_impl<Iter>();
    test_iter_impl<Iter, Sent>();

    using SizedSent1 = typename sentinel_type<Iter, true>::type;
    test_iter_impl<Iter, SizedSent1>();
}

template<class Iter, typename Sent = Iter>
void
test_range_impl()
{
    int ia[] = {0, 1, 2, 3, 4, 5};
    const unsigned sa = sizeof(ia)/sizeof(ia[0]);
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ia), Sent(ia+sa)), 0, 0).begin() == Iter(ia));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ia), Sent(ia+sa)), 1, 0).begin() == Iter(ia+0));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ia), Sent(ia+sa)), 2, 0).begin() == Iter(ia+sa));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ia), Sent(ia+sa)), sa, 0).begin() == Iter(ia+sa));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ia), Sent(ia+sa)), 0, 3).begin() == Iter(ia));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ia), Sent(ia+sa)), 1, 3).begin() == Iter(ia+3));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ia), Sent(ia+sa)), 2, 3).begin() == Iter(ia+sa));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ia), Sent(ia+sa)), sa, 3).begin() == Iter(ia+sa));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ia), Sent(ia+sa)), 0, 5).begin() == Iter(ia));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ia), Sent(ia+sa)), 1, 5).begin() == Iter(ia+5));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ia), Sent(ia+sa)), 2, 5).begin() == Iter(ia+sa));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ia), Sent(ia+sa)), sa, 5).begin() == Iter(ia+sa));

    int ib[] = {0, 0, 1, 1, 2, 2};
    const unsigned sb = sizeof(ib)/sizeof(ib[0]);
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ib), Sent(ib+sb)), 0, 0).begin() == Iter(ib));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ib), Sent(ib+sb)), 1, 0).begin() == Iter(ib+0));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ib), Sent(ib+sb)), 2, 0).begin() == Iter(ib+0));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ib), Sent(ib+sb)), 3, 0).begin() == Iter(ib+sb));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ib), Sent(ib+sb)), sb, 0).begin() == Iter(ib+sb));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ib), Sent(ib+sb)), 0, 1).begin() == Iter(ib));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ib), Sent(ib+sb)), 1, 1).begin() == Iter(ib+2));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ib), Sent(ib+sb)), 2, 1).begin() == Iter(ib+2));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ib), Sent(ib+sb)), 3, 1).begin() == Iter(ib+sb));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ib), Sent(ib+sb)), sb, 1).begin() == Iter(ib+sb));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ib), Sent(ib+sb)), 0, 2).begin() == Iter(ib));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ib), Sent(ib+sb)), 1, 2).begin() == Iter(ib+4));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ib), Sent(ib+sb)), 2, 2).begin() == Iter(ib+4));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ib), Sent(ib+sb)), 3, 2).begin() == Iter(ib+sb));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ib), Sent(ib+sb)), sb, 2).begin() == Iter(ib+sb));

    int ic[] = {0, 0, 0};
    const unsigned sc = sizeof(ic)/sizeof(ic[0]);
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ic), Sent(ic+sc)), 0, 0).begin() == Iter(ic));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ic), Sent(ic+sc)), 1, 0).begin() == Iter(ic));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ic), Sent(ic+sc)), 2, 0).begin() == Iter(ic));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ic), Sent(ic+sc)), 3, 0).begin() == Iter(ic));
    CHECK(ranges::search_n(ranges::make_subrange(Iter(ic), Sent(ic+sc)), 4, 0).begin() == Iter(ic+sc));
}

template<class Iter, class Iter2>
void
test_range()
{
    using Sent = typename sentinel_type<Iter>::type;
    test_range_impl<Iter>();
    test_range_impl<Iter, Sent>();

    using SizedSent1 = typename sentinel_type<Iter, true>::type;
    test_range_impl<Iter, SizedSent1>();
}

template<class Iter, class Iter2>
void
test()
{
    test_iter<Iter, Iter2>();
    test_range<Iter, Iter2>();
}

struct S
{
    int i;
};

constexpr bool test_constexpr()
{
    using namespace ranges;
    int ia[] = {0, 1, 2, 2, 4, 5};
    auto r = search_n(ia, 2, 2, equal_to{});
    STATIC_CHECK_RETURN(r.begin() == ia + 2);

    return true;
}

int main()
{
    test<ForwardIterator<const int*>, ForwardIterator<const int*> >();
    test<ForwardIterator<const int*>, BidirectionalIterator<const int*> >();
    test<ForwardIterator<const int*>, RandomAccessIterator<const int*> >();
    test<BidirectionalIterator<const int*>, ForwardIterator<const int*> >();
    test<BidirectionalIterator<const int*>, BidirectionalIterator<const int*> >();
    test<BidirectionalIterator<const int*>, RandomAccessIterator<const int*> >();
    test<RandomAccessIterator<const int*>, ForwardIterator<const int*> >();
    test<RandomAccessIterator<const int*>, BidirectionalIterator<const int*> >();
    test<RandomAccessIterator<const int*>, RandomAccessIterator<const int*> >();

    // Test projections:
    {
        S const in[] = {{0}, {1}, {2}, {2}, {4}, {5}};

        auto sub = ranges::search_n(in, 2, 2, std::equal_to<int>{}, &S::i);
        CHECK(sub.begin() == in+2);
        CHECK(sub.end() == in+4);
    }

    // Test counted ranges
    {
        int in[] = {0,1,2,2,4,5};
        auto rng = ranges::views::counted(BidirectionalIterator<int*>(in), 6);
        auto sub = ranges::search_n(rng, 2, 2);
        CHECK(base(sub.begin().base()) == in+2);
        CHECK(base(sub.end().base()) == in+4);
        CHECK(sub.begin().count() == 4);
        CHECK(sub.end().count() == 2);

        auto sub2 = ranges::search_n(rng, 3, 2);
        CHECK(base(sub2.begin().base()) == in+6);
        CHECK(base(sub2.end().base()) == in+6);
        CHECK(sub2.begin().count() == 0);
        CHECK(sub2.end().count() == 0);
    }

    // Test rvalue ranges
    {
        int ib[] = {0, 0, 1, 1, 2, 2};
        CHECK(ranges::search_n(ranges::views::all(ib), 2, 1).begin() == ib+2);
    }
#ifndef RANGES_WORKAROUND_MSVC_573728
    {
        int ib[] = {0, 0, 1, 1, 2, 2};
        CHECK(::is_dangling(ranges::search_n(std::move(ib), 2, 1)));
    }
#endif // RANGES_WORKAROUND_MSVC_573728
    {
        std::vector<int> ib{0, 0, 1, 1, 2, 2};
        CHECK(::is_dangling(ranges::search_n(std::move(ib), 2, 1)));
    }

    {
        STATIC_CHECK(test_constexpr());
    }

    return ::test_result();
}
