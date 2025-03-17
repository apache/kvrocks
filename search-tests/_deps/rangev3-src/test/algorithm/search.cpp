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
#include <range/v3/algorithm/search.hpp>
#include <range/v3/view/counted.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

template<class Iter1, class Iter2, typename Sent1 = Iter1, typename Sent2 = Iter2>
void
test_iter_impl()
{
    int ia[] = {0, 1, 2, 3, 4, 5};
    const unsigned sa = sizeof(ia)/sizeof(ia[0]);
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia), Sent2(ia)).begin() == Iter1(ia));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia), Sent2(ia)).end() == Iter1(ia));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia), Sent2(ia+1)).begin() == Iter1(ia));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia), Sent2(ia+1)).end() == Iter1(ia+1));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia+1), Sent2(ia+2)).begin() == Iter1(ia+1));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia+1), Sent2(ia+2)).end() == Iter1(ia+2));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia+2), Sent2(ia+2)).begin() == Iter1(ia));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia+2), Sent2(ia+2)).end() == Iter1(ia));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia+2), Sent2(ia+3)).begin() == Iter1(ia+2));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia+2), Sent2(ia+3)).end() == Iter1(ia+3));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia+2), Sent2(ia+3)).begin() == Iter1(ia+2));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia+2), Sent2(ia+3)).end() == Iter1(ia+3));
    CHECK(ranges::search(Iter1(ia), Sent1(ia), Iter2(ia+2), Sent2(ia+3)).begin() == Iter1(ia));
    CHECK(ranges::search(Iter1(ia), Sent1(ia), Iter2(ia+2), Sent2(ia+3)).end() == Iter1(ia));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia+sa-1), Sent2(ia+sa)).begin() == Iter1(ia+sa-1));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia+sa-1), Sent2(ia+sa)).end() == Iter1(ia+sa));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia+sa-3), Sent2(ia+sa)).begin() == Iter1(ia+sa-3));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia+sa-3), Sent2(ia+sa)).end() == Iter1(ia+sa));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia), Sent2(ia+sa)).begin() == Iter1(ia));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa), Iter2(ia), Sent2(ia+sa)).end() == Iter1(ia+sa));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa-1), Iter2(ia), Sent2(ia+sa)).begin() == Iter1(ia+sa-1));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+sa-1), Iter2(ia), Sent2(ia+sa)).end() == Iter1(ia+sa-1));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+1), Iter2(ia), Sent2(ia+sa)).begin() == Iter1(ia+1));
    CHECK(ranges::search(Iter1(ia), Sent1(ia+1), Iter2(ia), Sent2(ia+sa)).end() == Iter1(ia+1));
    int ib[] = {0, 1, 2, 0, 1, 2, 3, 0, 1, 2, 3, 4};
    const unsigned sb = sizeof(ib)/sizeof(ib[0]);
    int ic[] = {1};
    CHECK(ranges::search(Iter1(ib), Sent1(ib+sb), Iter2(ic), Sent2(ic+1)).begin() == Iter1(ib+1));
    CHECK(ranges::search(Iter1(ib), Sent1(ib+sb), Iter2(ic), Sent2(ic+1)).end() == Iter1(ib+2));
    int id[] = {1, 2};
    CHECK(ranges::search(Iter1(ib), Sent1(ib+sb), Iter2(id), Sent2(id+2)).begin() == Iter1(ib+1));
    CHECK(ranges::search(Iter1(ib), Sent1(ib+sb), Iter2(id), Sent2(id+2)).end() == Iter1(ib+3));
    int ie[] = {1, 2, 3};
    CHECK(ranges::search(Iter1(ib), Sent1(ib+sb), Iter2(ie), Sent2(ie+3)).begin() == Iter1(ib+4));
    CHECK(ranges::search(Iter1(ib), Sent1(ib+sb), Iter2(ie), Sent2(ie+3)).end() == Iter1(ib+7));
    int ig[] = {1, 2, 3, 4};
    CHECK(ranges::search(Iter1(ib), Sent1(ib+sb), Iter2(ig), Sent2(ig+4)).begin() == Iter1(ib+8));
    CHECK(ranges::search(Iter1(ib), Sent1(ib+sb), Iter2(ig), Sent2(ig+4)).end() == Iter1(ib+12));
    int ih[] = {0, 1, 1, 1, 1, 2, 3, 0, 1, 2, 3, 4};
    const unsigned sh = sizeof(ih)/sizeof(ih[0]);
    int ii[] = {1, 1, 2};
    CHECK(ranges::search(Iter1(ih), Sent1(ih+sh), Iter2(ii), Sent2(ii+3)).begin() == Iter1(ih+3));
    CHECK(ranges::search(Iter1(ih), Sent1(ih+sh), Iter2(ii), Sent2(ii+3)).end() == Iter1(ih+6));
    int ij[] = {0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0};
    const unsigned sj = sizeof(ij)/sizeof(ij[0]);
    int ik[] = {0, 0, 0, 0, 1, 1, 1, 1, 0, 0};
    const unsigned sk = sizeof(ik)/sizeof(ik[0]);
    CHECK(ranges::search(Iter1(ij), Sent1(ij+sj), Iter2(ik), Sent2(ik+sk)).begin() == Iter1(ij+6));
    CHECK(ranges::search(Iter1(ij), Sent1(ij+sj), Iter2(ik), Sent2(ik+sk)).end() == Iter1(ij+6+sk));
}

template<class Iter1, class Iter2>
void
test_iter()
{
    using Sent1 = typename sentinel_type<Iter1>::type;
    using Sent2 = typename sentinel_type<Iter2>::type;
    test_iter_impl<Iter1, Iter2>();
    test_iter_impl<Iter1, Iter2, Sent1>();
    test_iter_impl<Iter1, Iter2, Iter1, Sent2>();
    test_iter_impl<Iter1, Iter2, Sent1, Sent2>();

    using SizedSent1 = typename sentinel_type<Iter1, true>::type;
    using SizedSent2 = typename sentinel_type<Iter2, true>::type;
    test_iter_impl<Iter1, Iter2, SizedSent1, SizedSent2>();
}

template<class Iter1, class Iter2, typename Sent1 = Iter1, typename Sent2 = Iter2>
void
test_range_impl()
{
    int ia[] = {0, 1, 2, 3, 4, 5};
    const unsigned sa = sizeof(ia)/sizeof(ia[0]);
    CHECK(ranges::search(ranges::make_subrange(Iter1(ia), Sent1(ia+sa)), ranges::make_subrange(Iter2(ia), Sent2(ia))).begin() == Iter1(ia));
    CHECK(ranges::search(ranges::make_subrange(Iter1(ia), Sent1(ia+sa)), ranges::make_subrange(Iter2(ia), Sent2(ia+1))).begin() == Iter1(ia));
    CHECK(ranges::search(ranges::make_subrange(Iter1(ia), Sent1(ia+sa)), ranges::make_subrange(Iter2(ia+1), Sent2(ia+2))).begin() == Iter1(ia+1));
    CHECK(ranges::search(ranges::make_subrange(Iter1(ia), Sent1(ia+sa)), ranges::make_subrange(Iter2(ia+2), Sent2(ia+2))).begin() == Iter1(ia));
    CHECK(ranges::search(ranges::make_subrange(Iter1(ia), Sent1(ia+sa)), ranges::make_subrange(Iter2(ia+2), Sent2(ia+3))).begin() == Iter1(ia+2));
    CHECK(ranges::search(ranges::make_subrange(Iter1(ia), Sent1(ia+sa)), ranges::make_subrange(Iter2(ia+2), Sent2(ia+3))).begin() == Iter1(ia+2));
    CHECK(ranges::search(ranges::make_subrange(Iter1(ia), Sent1(ia)), ranges::make_subrange(Iter2(ia+2), Sent2(ia+3))).begin() == Iter1(ia));
    CHECK(ranges::search(ranges::make_subrange(Iter1(ia), Sent1(ia+sa)), ranges::make_subrange(Iter2(ia+sa-1), Sent2(ia+sa))).begin() == Iter1(ia+sa-1));
    CHECK(ranges::search(ranges::make_subrange(Iter1(ia), Sent1(ia+sa)), ranges::make_subrange(Iter2(ia+sa-3), Sent2(ia+sa))).begin() == Iter1(ia+sa-3));
    CHECK(ranges::search(ranges::make_subrange(Iter1(ia), Sent1(ia+sa)), ranges::make_subrange(Iter2(ia), Sent2(ia+sa))).begin() == Iter1(ia));
    CHECK(ranges::search(ranges::make_subrange(Iter1(ia), Sent1(ia+sa-1)), ranges::make_subrange(Iter2(ia), Sent2(ia+sa))).begin() == Iter1(ia+sa-1));
    CHECK(ranges::search(ranges::make_subrange(Iter1(ia), Sent1(ia+1)), ranges::make_subrange(Iter2(ia), Sent2(ia+sa))).begin() == Iter1(ia+1));
    int ib[] = {0, 1, 2, 0, 1, 2, 3, 0, 1, 2, 3, 4};
    const unsigned sb = sizeof(ib)/sizeof(ib[0]);
    int ic[] = {1};
    CHECK(ranges::search(ranges::make_subrange(Iter1(ib), Sent1(ib+sb)), ranges::make_subrange(Iter2(ic), Sent2(ic+1))).begin() == Iter1(ib+1));
    int id[] = {1, 2};
    CHECK(ranges::search(ranges::make_subrange(Iter1(ib), Sent1(ib+sb)), ranges::make_subrange(Iter2(id), Sent2(id+2))).begin() == Iter1(ib+1));
    int ie[] = {1, 2, 3};
    CHECK(ranges::search(ranges::make_subrange(Iter1(ib), Sent1(ib+sb)), ranges::make_subrange(Iter2(ie), Sent2(ie+3))).begin() == Iter1(ib+4));
    int ig[] = {1, 2, 3, 4};
    CHECK(ranges::search(ranges::make_subrange(Iter1(ib), Sent1(ib+sb)), ranges::make_subrange(Iter2(ig), Sent2(ig+4))).begin() == Iter1(ib+8));
    int ih[] = {0, 1, 1, 1, 1, 2, 3, 0, 1, 2, 3, 4};
    const unsigned sh = sizeof(ih)/sizeof(ih[0]);
    int ii[] = {1, 1, 2};
    CHECK(ranges::search(ranges::make_subrange(Iter1(ih), Sent1(ih+sh)), ranges::make_subrange(Iter2(ii), Sent2(ii+3))).begin() == Iter1(ih+3));
    int ij[] = {0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0};
    const unsigned sj = sizeof(ij)/sizeof(ij[0]);
    int ik[] = {0, 0, 0, 0, 1, 1, 1, 1, 0, 0};
    const unsigned sk = sizeof(ik)/sizeof(ik[0]);
    CHECK(ranges::search(ranges::make_subrange(Iter1(ij), Sent1(ij+sj)), ranges::make_subrange(Iter2(ik), Sent2(ik+sk))).begin() == Iter1(ij+6));
}

template<class Iter1, class Iter2>
void
test_range()
{
    using Sent1 = typename sentinel_type<Iter1>::type;
    using Sent2 = typename sentinel_type<Iter2>::type;
    test_range_impl<Iter1, Iter2>();
    test_range_impl<Iter1, Iter2, Sent1>();
    test_range_impl<Iter1, Iter2, Iter1, Sent2>();
    test_range_impl<Iter1, Iter2, Sent1, Sent2>();

    using SizedSent1 = typename sentinel_type<Iter1, true>::type;
    using SizedSent2 = typename sentinel_type<Iter2, true>::type;
    test_range_impl<Iter1, Iter2, SizedSent1, SizedSent2>();
}

template<class Iter1, class Iter2>
void
test()
{
    test_iter<Iter1, Iter2>();
    test_range<Iter1, Iter2>();
}

struct S
{
    int i;
};

struct T
{
    int i;
};

constexpr bool test_constexpr()
{
    using namespace ranges;
    int ia[] = {0, 1, 2, 3, 4};
    int ib[] = {2, 3};
    int ic[] = {2, 4};
    constexpr auto sa = size(ia);
    auto r = search(ia, ib, equal_to{});
    STATIC_CHECK_RETURN(r.begin() == ia + 2);
    auto r2 = search(ia, ic, equal_to{});
    STATIC_CHECK_RETURN(r2.begin() == ia + sa);

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
        S const in[] = {{0}, {1}, {2}, {3}, {4}, {5}};
        T const pat[] = {{2}, {3}};

        S const *p = ranges::search(in, pat, std::equal_to<int>{}, &S::i, &T::i).begin();
        CHECK(p == in+2);
    }

    // Test counted ranges
    {
        int in[] = {0,1,2,3,4,5};
        auto rng = ranges::views::counted(BidirectionalIterator<int*>(in), 6);
        auto sub = ranges::search(rng, std::initializer_list<int>{2,3});
        CHECK(base(sub.begin().base()) == in+2);
        CHECK(base(sub.end().base()) == in+4);
        CHECK(sub.begin().count() == 4);
        CHECK(sub.end().count() == 2);

        sub = ranges::search(rng, std::initializer_list<int>{5,6});
        CHECK(base(sub.begin().base()) == in+6);
        CHECK(base(sub.end().base()) == in+6);
        CHECK(sub.begin().count() == 0);
        CHECK(sub.end().count() == 0);
    }

    // Test rvalue ranges
    {
        int ib[] = {0, 1, 2, 0, 1, 2, 3, 0, 1, 2, 3, 4};
        int ie[] = {1, 2, 3};
        CHECK(ranges::search(ranges::views::all(ib), ie).begin() == ib+4);
    }
#ifndef RANGES_WORKAROUND_MSVC_573728
    {
        int ib[] = {0, 1, 2, 0, 1, 2, 3, 0, 1, 2, 3, 4};
        int ie[] = {1, 2, 3};
        CHECK(::is_dangling(ranges::search(std::move(ib), ie)));
    }
#endif
    {
        std::vector<int> ib{0, 1, 2, 0, 1, 2, 3, 0, 1, 2, 3, 4};
        int ie[] = {1, 2, 3};
        CHECK(::is_dangling(ranges::search(std::move(ib), ie)));
    }

    {
        STATIC_CHECK(test_constexpr());
    }

    return ::test_result();
}
