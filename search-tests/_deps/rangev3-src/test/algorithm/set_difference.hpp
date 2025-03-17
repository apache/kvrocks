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
//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <functional>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/fill.hpp>
#include <range/v3/algorithm/set_algorithm.hpp>
#include <range/v3/algorithm/lexicographical_compare.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

template<class Iter1, class Iter2, class OutIter>
void
test_iter()
{
    int ia[] = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4};
    static const int sa = sizeof(ia)/sizeof(ia[0]);
    int ib[] = {2, 4, 4, 6};
    static const int sb = sizeof(ib)/sizeof(ib[0]);
    int ic[20];
    int ir[] = {1, 2, 3, 3, 3, 4, 4};
    static const int sr = sizeof(ir)/sizeof(ir[0]);

    auto set_difference = ::make_testable_2<false, true>(ranges::set_difference);

    set_difference(Iter1(ia), Iter1(ia+sa), Iter2(ib), Iter2(ib+sb), OutIter(ic)).
        check([&](ranges::set_difference_result<Iter1, OutIter> res)
        {
            CHECK((base(res.in1) - ia) == sa);
            CHECK((base(res.out) - ic) == sr);
            CHECK(std::lexicographical_compare(ic, base(res.out), ir, ir+sr) == false);
            ranges::fill(ic, 0);
        }
    );
    int irr[] = {6};
    static const int srr = sizeof(irr)/sizeof(irr[0]);
    set_difference(Iter1(ib), Iter1(ib+sb), Iter2(ia), Iter2(ia+sa), OutIter(ic)).
        check([&](ranges::set_difference_result<Iter1, OutIter> res)
        {
            CHECK((base(res.in1) - ib) == sb);
            CHECK((base(res.out) - ic) == srr);
            CHECK(std::lexicographical_compare(ic, base(res.out), irr, irr+srr) == false);
            ranges::fill(ic, 0);
        }
    );
}

template<class Iter1, class Iter2, class OutIter>
void
test_comp()
{
    int ia[] = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4};
    static const int sa = sizeof(ia)/sizeof(ia[0]);
    int ib[] = {2, 4, 4, 6};
    static const int sb = sizeof(ib)/sizeof(ib[0]);
    int ic[20];
    int ir[] = {1, 2, 3, 3, 3, 4, 4};
    static const int sr = sizeof(ir)/sizeof(ir[0]);

    auto set_difference = ::make_testable_2<false, true>(ranges::set_difference);

    set_difference(Iter1(ia), Iter1(ia+sa), Iter2(ib), Iter2(ib+sb), OutIter(ic), std::less<int>()).
        check([&](ranges::set_difference_result<Iter1, OutIter> res)
        {
            CHECK((base(res.in1) - ia) == sa);
            CHECK((base(res.out) - ic) == sr);
            CHECK(std::lexicographical_compare(ic, base(res.out), ir, ir+sr) == false);
            ranges::fill(ic, 0);
        }
    );
    int irr[] = {6};
    static const int srr = sizeof(irr)/sizeof(irr[0]);
    set_difference(Iter1(ib), Iter1(ib+sb), Iter2(ia), Iter2(ia+sa), OutIter(ic), std::less<int>()).
        check([&](ranges::set_difference_result<Iter1, OutIter> res)
        {
            CHECK((base(res.in1) - ib) == sb);
            CHECK((base(res.out) - ic) == srr);
            CHECK(std::lexicographical_compare(ic, base(res.out), irr, irr+srr) == false);
            ranges::fill(ic, 0);
        }
    );
}

template<class Iter1, class Iter2, class OutIter>
void test()
{
    test_iter<Iter1, Iter2, OutIter>();
    test_comp<Iter1, Iter2, OutIter>();
}

struct S
{
    int i;
};

struct T
{
    int j;
};

struct U
{
    int k;
    U& operator=(S s) { k = s.i; return *this;}
    U& operator=(T t) { k = t.j; return *this;}
};

constexpr bool test_constexpr()
{
    using namespace ranges;
    using IL = std::initializer_list<int>;
    int ia[] = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4};
    const int sa = sizeof(ia) / sizeof(ia[0]);
    int ib[] = {2, 4, 4, 6};
    const int sb = sizeof(ib) / sizeof(ib[0]);
    int ic[20] = {0};
    int ir[] = {1, 2, 3, 3, 3, 4, 4};
    const int sr = sizeof(ir) / sizeof(ir[0]);

    const auto res = set_difference(ia, IL{2, 4, 4, 6}, ic, less{});
    STATIC_CHECK_RETURN((res.in1 - ia) == sa);
    STATIC_CHECK_RETURN((res.out - ic) == sr);
    STATIC_CHECK_RETURN(lexicographical_compare(ic, res.out, ir, ir + sr, less{}) == 0);
    fill(ic, 0);

    int irr[] = {6};
    const int srr = sizeof(irr) / sizeof(irr[0]);
    const auto res2 = set_difference(ib, IL{1, 2, 2, 3, 3, 3, 4, 4, 4, 4}, ic, less{});
    STATIC_CHECK_RETURN((res2.in1 - ib) == sb);
    STATIC_CHECK_RETURN((res2.out - ic) == srr);
    STATIC_CHECK_RETURN(lexicographical_compare(ic, res2.out, irr, irr + srr, less{}) == 0);

    return true;
}

int main()
{
#ifdef SET_DIFFERENCE_1
    test<InputIterator<const int*>, InputIterator<const int*>, OutputIterator<int*> >();
    test<InputIterator<const int*>, InputIterator<const int*>, ForwardIterator<int*> >();
    test<InputIterator<const int*>, InputIterator<const int*>, BidirectionalIterator<int*> >();
    test<InputIterator<const int*>, InputIterator<const int*>, RandomAccessIterator<int*> >();
    test<InputIterator<const int*>, InputIterator<const int*>, int*>();

    test<InputIterator<const int*>, ForwardIterator<const int*>, OutputIterator<int*> >();
    test<InputIterator<const int*>, ForwardIterator<const int*>, ForwardIterator<int*> >();
    test<InputIterator<const int*>, ForwardIterator<const int*>, BidirectionalIterator<int*> >();
    test<InputIterator<const int*>, ForwardIterator<const int*>, RandomAccessIterator<int*> >();
    test<InputIterator<const int*>, ForwardIterator<const int*>, int*>();

    test<InputIterator<const int*>, BidirectionalIterator<const int*>, OutputIterator<int*> >();
    test<InputIterator<const int*>, BidirectionalIterator<const int*>, ForwardIterator<int*> >();
    test<InputIterator<const int*>, BidirectionalIterator<const int*>, BidirectionalIterator<int*> >();
    test<InputIterator<const int*>, BidirectionalIterator<const int*>, RandomAccessIterator<int*> >();
    test<InputIterator<const int*>, BidirectionalIterator<const int*>, int*>();

    test<InputIterator<const int*>, RandomAccessIterator<const int*>, OutputIterator<int*> >();
    test<InputIterator<const int*>, RandomAccessIterator<const int*>, ForwardIterator<int*> >();
    test<InputIterator<const int*>, RandomAccessIterator<const int*>, BidirectionalIterator<int*> >();
    test<InputIterator<const int*>, RandomAccessIterator<const int*>, RandomAccessIterator<int*> >();
    test<InputIterator<const int*>, RandomAccessIterator<const int*>, int*>();

    test<InputIterator<const int*>, const int*, OutputIterator<int*> >();
    test<InputIterator<const int*>, const int*, ForwardIterator<int*> >();
    test<InputIterator<const int*>, const int*, BidirectionalIterator<int*> >();
    test<InputIterator<const int*>, const int*, RandomAccessIterator<int*> >();
    test<InputIterator<const int*>, const int*, int*>();
#endif
#ifdef SET_DIFFERENCE_2
    test<ForwardIterator<const int*>, InputIterator<const int*>, OutputIterator<int*> >();
    test<ForwardIterator<const int*>, InputIterator<const int*>, ForwardIterator<int*> >();
    test<ForwardIterator<const int*>, InputIterator<const int*>, BidirectionalIterator<int*> >();
    test<ForwardIterator<const int*>, InputIterator<const int*>, RandomAccessIterator<int*> >();
    test<ForwardIterator<const int*>, InputIterator<const int*>, int*>();

    test<ForwardIterator<const int*>, ForwardIterator<const int*>, OutputIterator<int*> >();
    test<ForwardIterator<const int*>, ForwardIterator<const int*>, ForwardIterator<int*> >();
    test<ForwardIterator<const int*>, ForwardIterator<const int*>, BidirectionalIterator<int*> >();
    test<ForwardIterator<const int*>, ForwardIterator<const int*>, RandomAccessIterator<int*> >();
    test<ForwardIterator<const int*>, ForwardIterator<const int*>, int*>();

    test<ForwardIterator<const int*>, BidirectionalIterator<const int*>, OutputIterator<int*> >();
    test<ForwardIterator<const int*>, BidirectionalIterator<const int*>, ForwardIterator<int*> >();
    test<ForwardIterator<const int*>, BidirectionalIterator<const int*>, BidirectionalIterator<int*> >();
    test<ForwardIterator<const int*>, BidirectionalIterator<const int*>, RandomAccessIterator<int*> >();
    test<ForwardIterator<const int*>, BidirectionalIterator<const int*>, int*>();

    test<ForwardIterator<const int*>, RandomAccessIterator<const int*>, OutputIterator<int*> >();
    test<ForwardIterator<const int*>, RandomAccessIterator<const int*>, ForwardIterator<int*> >();
    test<ForwardIterator<const int*>, RandomAccessIterator<const int*>, BidirectionalIterator<int*> >();
    test<ForwardIterator<const int*>, RandomAccessIterator<const int*>, RandomAccessIterator<int*> >();
    test<ForwardIterator<const int*>, RandomAccessIterator<const int*>, int*>();

    test<ForwardIterator<const int*>, const int*, OutputIterator<int*> >();
    test<ForwardIterator<const int*>, const int*, ForwardIterator<int*> >();
    test<ForwardIterator<const int*>, const int*, BidirectionalIterator<int*> >();
    test<ForwardIterator<const int*>, const int*, RandomAccessIterator<int*> >();
    test<ForwardIterator<const int*>, const int*, int*>();
#endif
#ifdef SET_DIFFERENCE_3
    test<BidirectionalIterator<const int*>, InputIterator<const int*>, OutputIterator<int*> >();
    test<BidirectionalIterator<const int*>, InputIterator<const int*>, ForwardIterator<int*> >();
    test<BidirectionalIterator<const int*>, InputIterator<const int*>, BidirectionalIterator<int*> >();
    test<BidirectionalIterator<const int*>, InputIterator<const int*>, RandomAccessIterator<int*> >();
    test<BidirectionalIterator<const int*>, InputIterator<const int*>, int*>();

    test<BidirectionalIterator<const int*>, ForwardIterator<const int*>, OutputIterator<int*> >();
    test<BidirectionalIterator<const int*>, ForwardIterator<const int*>, ForwardIterator<int*> >();
    test<BidirectionalIterator<const int*>, ForwardIterator<const int*>, BidirectionalIterator<int*> >();
    test<BidirectionalIterator<const int*>, ForwardIterator<const int*>, RandomAccessIterator<int*> >();
    test<BidirectionalIterator<const int*>, ForwardIterator<const int*>, int*>();

    test<BidirectionalIterator<const int*>, BidirectionalIterator<const int*>, OutputIterator<int*> >();
    test<BidirectionalIterator<const int*>, BidirectionalIterator<const int*>, ForwardIterator<int*> >();
    test<BidirectionalIterator<const int*>, BidirectionalIterator<const int*>, BidirectionalIterator<int*> >();
    test<BidirectionalIterator<const int*>, BidirectionalIterator<const int*>, RandomAccessIterator<int*> >();
    test<BidirectionalIterator<const int*>, BidirectionalIterator<const int*>, int*>();

    test<BidirectionalIterator<const int*>, RandomAccessIterator<const int*>, OutputIterator<int*> >();
    test<BidirectionalIterator<const int*>, RandomAccessIterator<const int*>, ForwardIterator<int*> >();
    test<BidirectionalIterator<const int*>, RandomAccessIterator<const int*>, BidirectionalIterator<int*> >();
    test<BidirectionalIterator<const int*>, RandomAccessIterator<const int*>, RandomAccessIterator<int*> >();
    test<BidirectionalIterator<const int*>, RandomAccessIterator<const int*>, int*>();

    test<BidirectionalIterator<const int*>, const int*, OutputIterator<int*> >();
    test<BidirectionalIterator<const int*>, const int*, ForwardIterator<int*> >();
    test<BidirectionalIterator<const int*>, const int*, BidirectionalIterator<int*> >();
    test<BidirectionalIterator<const int*>, const int*, RandomAccessIterator<int*> >();
    test<BidirectionalIterator<const int*>, const int*, int*>();
#endif
#ifdef SET_DIFFERENCE_4
    test<RandomAccessIterator<const int*>, InputIterator<const int*>, OutputIterator<int*> >();
    test<RandomAccessIterator<const int*>, InputIterator<const int*>, ForwardIterator<int*> >();
    test<RandomAccessIterator<const int*>, InputIterator<const int*>, BidirectionalIterator<int*> >();
    test<RandomAccessIterator<const int*>, InputIterator<const int*>, RandomAccessIterator<int*> >();
    test<RandomAccessIterator<const int*>, InputIterator<const int*>, int*>();

    test<RandomAccessIterator<const int*>, ForwardIterator<const int*>, OutputIterator<int*> >();
    test<RandomAccessIterator<const int*>, ForwardIterator<const int*>, ForwardIterator<int*> >();
    test<RandomAccessIterator<const int*>, ForwardIterator<const int*>, BidirectionalIterator<int*> >();
    test<RandomAccessIterator<const int*>, ForwardIterator<const int*>, RandomAccessIterator<int*> >();
    test<RandomAccessIterator<const int*>, ForwardIterator<const int*>, int*>();

    test<RandomAccessIterator<const int*>, BidirectionalIterator<const int*>, OutputIterator<int*> >();
    test<RandomAccessIterator<const int*>, BidirectionalIterator<const int*>, ForwardIterator<int*> >();
    test<RandomAccessIterator<const int*>, BidirectionalIterator<const int*>, BidirectionalIterator<int*> >();
    test<RandomAccessIterator<const int*>, BidirectionalIterator<const int*>, RandomAccessIterator<int*> >();
    test<RandomAccessIterator<const int*>, BidirectionalIterator<const int*>, int*>();

    test<RandomAccessIterator<const int*>, RandomAccessIterator<const int*>, OutputIterator<int*> >();
    test<RandomAccessIterator<const int*>, RandomAccessIterator<const int*>, ForwardIterator<int*> >();
    test<RandomAccessIterator<const int*>, RandomAccessIterator<const int*>, BidirectionalIterator<int*> >();
    test<RandomAccessIterator<const int*>, RandomAccessIterator<const int*>, RandomAccessIterator<int*> >();
    test<RandomAccessIterator<const int*>, RandomAccessIterator<const int*>, int*>();

    test<RandomAccessIterator<const int*>, const int*, OutputIterator<int*> >();
    test<RandomAccessIterator<const int*>, const int*, ForwardIterator<int*> >();
    test<RandomAccessIterator<const int*>, const int*, BidirectionalIterator<int*> >();
    test<RandomAccessIterator<const int*>, const int*, RandomAccessIterator<int*> >();
    test<RandomAccessIterator<const int*>, const int*, int*>();
#endif
#ifdef SET_DIFFERENCE_5
    test<const int*, InputIterator<const int*>, OutputIterator<int*> >();
    test<const int*, InputIterator<const int*>, BidirectionalIterator<int*> >();    test<const int*, InputIterator<const int*>, BidirectionalIterator<int*> >();
    test<const int*, InputIterator<const int*>, RandomAccessIterator<int*> >();
    test<const int*, InputIterator<const int*>, int*>();

    test<const int*, ForwardIterator<const int*>, OutputIterator<int*> >();
    test<const int*, ForwardIterator<const int*>, ForwardIterator<int*> >();
    test<const int*, ForwardIterator<const int*>, BidirectionalIterator<int*> >();
    test<const int*, ForwardIterator<const int*>, RandomAccessIterator<int*> >();
    test<const int*, ForwardIterator<const int*>, int*>();

    test<const int*, BidirectionalIterator<const int*>, OutputIterator<int*> >();
    test<const int*, BidirectionalIterator<const int*>, ForwardIterator<int*> >();
    test<const int*, BidirectionalIterator<const int*>, BidirectionalIterator<int*> >();
    test<const int*, BidirectionalIterator<const int*>, RandomAccessIterator<int*> >();
    test<const int*, BidirectionalIterator<const int*>, int*>();

    test<const int*, RandomAccessIterator<const int*>, OutputIterator<int*> >();
    test<const int*, RandomAccessIterator<const int*>, ForwardIterator<int*> >();
    test<const int*, RandomAccessIterator<const int*>, BidirectionalIterator<int*> >();
    test<const int*, RandomAccessIterator<const int*>, RandomAccessIterator<int*> >();
    test<const int*, RandomAccessIterator<const int*>, int*>();

    test<const int*, const int*, OutputIterator<int*> >();
    test<const int*, const int*, ForwardIterator<int*> >();
    test<const int*, const int*, BidirectionalIterator<int*> >();
    test<const int*, const int*, RandomAccessIterator<int*> >();
    test<const int*, const int*, int*>();
#endif
#ifdef SET_DIFFERENCE_6
    // Test projections
    {
        S ia[] = {S{1}, S{2}, S{2}, S{3}, S{3}, S{3}, S{4}, S{4}, S{4}, S{4}};
        static const int sa = sizeof(ia)/sizeof(ia[0]);
        T ib[] = {T{2}, T{4}, T{4}, T{6}};
        static const int sb = sizeof(ib)/sizeof(ib[0]);
        U ic[20];
        int ir[] = {1, 2, 3, 3, 3, 4, 4};
        static const int sr = sizeof(ir)/sizeof(ir[0]);

        ranges::set_difference_result<S *, U *> res = ranges::set_difference(ia, ib, ic, std::less<int>(), &S::i, &T::j);
        CHECK((res.in1 - ia) == sa);
        CHECK((res.out - ic) == sr);
        CHECK(ranges::lexicographical_compare(ic, res.out, ir, ir+sr, std::less<int>(), &U::k) == false);
        ranges::fill(ic, U{0});

        int irr[] = {6};
        static const int srr = sizeof(irr)/sizeof(irr[0]);
        ranges::set_difference_result<T *, U *> res2 = ranges::set_difference(ib, ia, ic, std::less<int>(), &T::j, &S::i);
        CHECK((res2.in1 - ib) == sb);
        CHECK((res2.out - ic) == srr);
        CHECK(ranges::lexicographical_compare(ic, res2.out, ir, irr+srr, std::less<int>(), &U::k) == false);
    }

    // Test rvalue ranges
    {
        S ia[] = {S{1}, S{2}, S{2}, S{3}, S{3}, S{3}, S{4}, S{4}, S{4}, S{4}};
        T ib[] = {T{2}, T{4}, T{4}, T{6}};
        static const int sb = sizeof(ib)/sizeof(ib[0]);
        U ic[20];
        int ir[] = {1, 2, 3, 3, 3, 4, 4};
        static const int sr = sizeof(ir)/sizeof(ir[0]);

        auto res = ranges::set_difference(std::move(ia), ranges::views::all(ib), ic, std::less<int>(), &S::i, &T::j);
#ifndef RANGES_WORKAROUND_MSVC_573728
        CHECK(::is_dangling(res.in1));
#endif // RANGES_WORKAROUND_MSVC_573728
        CHECK((res.out - ic) == sr);
        CHECK(ranges::lexicographical_compare(ic, res.out, ir, ir+sr, std::less<int>(), &U::k) == false);

        ranges::fill(ic, U{0});
        int irr[] = {6};
        static const int srr = sizeof(irr)/sizeof(irr[0]);
        auto res2 = ranges::set_difference(ranges::views::all(ib), ranges::views::all(ia), ic, std::less<int>(), &T::j, &S::i);
        CHECK((res2.in1 - ib) == sb);
        CHECK((res2.out - ic) == srr);
        CHECK(ranges::lexicographical_compare(ic, res2.out, ir, irr+srr, std::less<int>(), &U::k) == false);

        ranges::fill(ic, U{0});
        std::vector<S> vec{S{1}, S{2}, S{2}, S{3}, S{3}, S{3}, S{4}, S{4}, S{4}, S{4}};
        auto res3 = ranges::set_difference(std::move(vec), ranges::views::all(ib), ic, std::less<int>(), &S::i, &T::j);
        CHECK(::is_dangling(res3.in1));
        CHECK((res3.out - ic) == sr);
        CHECK(ranges::lexicographical_compare(ic, res3.out, ir, ir+sr, std::less<int>(), &U::k) == false);
    }

    {
        STATIC_CHECK(test_constexpr());
    }
#endif

    return ::test_result();
}
