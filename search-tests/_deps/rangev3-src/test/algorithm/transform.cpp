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

//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#include <functional>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/transform.hpp>
#include <range/v3/view/unbounded.hpp>
#include "../simple_test.hpp"
#include "../test_iterators.hpp"

using namespace std::placeholders;

RANGES_DIAGNOSTIC_IGNORE_DEPRECATED_DECLARATIONS

template<class InIter, class OutIter>
void
test1()
{
    {
        int ia[] = {0, 1, 2, 3, 4};
        constexpr auto sa = ranges::size(ia);
        int ib[sa] = {0};
        ranges::unary_transform_result<InIter, OutIter> r =
            ranges::transform(InIter(ia), Sentinel<int const *>(ia+sa), OutIter(ib),
                              std::bind(std::plus<int>(), _1, 1));
        CHECK(base(r.in) == ia + sa);
        CHECK(base(r.out) == ib + sa);
        CHECK(ib[0] == 1);
        CHECK(ib[1] == 2);
        CHECK(ib[2] == 3);
        CHECK(ib[3] == 4);
        CHECK(ib[4] == 5);
    }

    {
        int ia[] = {0, 1, 2, 3, 4};
        constexpr auto sa = ranges::size(ia);
        int ib[sa] = {0};
        auto rng = ranges::make_subrange(InIter(ia), Sentinel<int const *>(ia + sa));
        ranges::unary_transform_result<InIter, OutIter> r =
            ranges::transform(rng, OutIter(ib),
                              std::bind(std::plus<int>(), _1, 1));
        CHECK(base(r.in) == ia + sa);
        CHECK(base(r.out) == ib + sa);
        CHECK(ib[0] == 1);
        CHECK(ib[1] == 2);
        CHECK(ib[2] == 3);
        CHECK(ib[3] == 4);
        CHECK(ib[4] == 5);
    }

    {
        int ia[] = {0, 1, 2, 3, 4};
        constexpr auto sa = ranges::size(ia);
        int ib[sa] = {0};
        auto rng = ranges::make_subrange(InIter(ia), Sentinel<int const *>(ia + sa));
        auto r =
            ranges::transform(std::move(rng), OutIter(ib),
                              std::bind(std::plus<int>(), _1, 1));
        CHECK(base(r.in) == ia + sa);
        CHECK(base(r.out) == ib + sa);
        CHECK(ib[0] == 1);
        CHECK(ib[1] == 2);
        CHECK(ib[2] == 3);
        CHECK(ib[3] == 4);
        CHECK(ib[4] == 5);
    }
}

template<class InIter1, class InIter2, class OutIter>
void
test2()
{
    {
        int ia[] = {0, 1, 2, 3, 4};
        constexpr auto sa = ranges::size(ia);
        int ib[sa] = {1, 2, 3, 4, 5};
        ranges::binary_transform_result<InIter1, InIter2, OutIter> r =
            ranges::transform(InIter1(ib), Sentinel<int const *>(ib + sa), InIter2(ia),
                              OutIter(ib), std::minus<int>());
        CHECK(base(r.in1) == ib + sa);
        CHECK(base(r.in2) == ia + sa);
        CHECK(base(r.out) == ib + sa);
        CHECK(ib[0] == 1);
        CHECK(ib[1] == 1);
        CHECK(ib[2] == 1);
        CHECK(ib[3] == 1);
        CHECK(ib[4] == 1);
    }

    {
        int ia[] = {0, 1, 2, 3, 4};
        constexpr auto sa = ranges::size(ia);
        int ib[sa] = {1, 2, 3, 4, 5};
        ranges::binary_transform_result<InIter1, InIter2, OutIter> r =
            ranges::transform(InIter1(ib), Sentinel<int const *>(ib + sa),
                              InIter2(ia), Sentinel<int const *>(ia + sa),
                              OutIter(ib), std::minus<int>());
        CHECK(base(r.in1) == ib + sa);
        CHECK(base(r.in2) == ia + sa);
        CHECK(base(r.out) == ib + sa);
        CHECK(ib[0] == 1);
        CHECK(ib[1] == 1);
        CHECK(ib[2] == 1);
        CHECK(ib[3] == 1);
        CHECK(ib[4] == 1);
    }

    {
        int ia[] = {0, 1, 2, 3, 4};
        constexpr auto sa = ranges::size(ia);
        int ib[sa] = {1, 2, 3, 4, 5};
        auto rng0 = ranges::make_subrange(InIter1(ib), Sentinel<int const *>(ib + sa));
        ranges::binary_transform_result<InIter1, InIter2, OutIter> r =
            ranges::transform(rng0, InIter2(ia),
                              OutIter(ib), std::minus<int>());
        CHECK(base(r.in1) == ib + sa);
        CHECK(base(r.in2) == ia + sa);
        CHECK(base(r.out) == ib + sa);
        CHECK(ib[0] == 1);
        CHECK(ib[1] == 1);
        CHECK(ib[2] == 1);
        CHECK(ib[3] == 1);
        CHECK(ib[4] == 1);
    }

    {
        int ia[] = {0, 1, 2, 3, 4};
        constexpr auto sa = ranges::size(ia);
        int ib[sa] = {1, 2, 3, 4, 5};
        auto rng0 = ranges::make_subrange(InIter1(ib), Sentinel<int const *>(ib + sa));
        auto r =
            ranges::transform(std::move(rng0), InIter2(ia),
                              OutIter(ib), std::minus<int>());
        CHECK(base(r.in1) == ib + sa);
        CHECK(base(r.in2) == ia + sa);
        CHECK(base(r.out) == ib + sa);
        CHECK(ib[0] == 1);
        CHECK(ib[1] == 1);
        CHECK(ib[2] == 1);
        CHECK(ib[3] == 1);
        CHECK(ib[4] == 1);
    }

    {
        int ia[] = {0, 1, 2, 3, 4};
        constexpr auto sa = ranges::size(ia);
        int ib[sa] = {1, 2, 3, 4, 5};
        auto rng0 = ranges::make_subrange(InIter1(ib), Sentinel<int const *>(ib + sa));
        auto rng1 = ranges::make_subrange(InIter2(ia), Sentinel<int const *>(ia + sa));
        ranges::binary_transform_result<InIter1, InIter2, OutIter> r =
            ranges::transform(rng0, rng1, OutIter(ib), std::minus<int>());
        CHECK(base(r.in1) == ib + sa);
        CHECK(base(r.in2) == ia + sa);
        CHECK(base(r.out) == ib + sa);
        CHECK(ib[0] == 1);
        CHECK(ib[1] == 1);
        CHECK(ib[2] == 1);
        CHECK(ib[3] == 1);
        CHECK(ib[4] == 1);
    }

    {
        int ia[] = {0, 1, 2, 3, 4};
        constexpr auto sa = ranges::size(ia);
        int ib[sa] = {1, 2, 3, 4, 5};
        auto rng0 = ranges::make_subrange(InIter1(ib), Sentinel<int const *>(ib + sa));
        auto rng1 = ranges::make_subrange(InIter2(ia), Sentinel<int const *>(ia + sa));
        auto r =
            ranges::transform(std::move(rng0), std::move(rng1), OutIter(ib), std::minus<int>());
        CHECK(base(r.in1) == ib + sa);
        CHECK(base(r.in2) == ia + sa);
        CHECK(base(r.out) == ib + sa);
        CHECK(ib[0] == 1);
        CHECK(ib[1] == 1);
        CHECK(ib[2] == 1);
        CHECK(ib[3] == 1);
        CHECK(ib[4] == 1);
    }
}

struct S
{
    int i;
};

constexpr int plus_one(int i)
{
    return i + 1;
}
constexpr bool test_constexpr()
{
    using namespace ranges;
    int ia[] = {0, 1, 2, 3, 4};
    constexpr auto sa = ranges::size(ia);
    int ib[sa] = {0};
    auto r = transform(ia, ib, plus_one);
    STATIC_CHECK_RETURN(r.in == ia + sa);
    STATIC_CHECK_RETURN(r.out == ib + sa);
    STATIC_CHECK_RETURN(ib[0] == 1);
    STATIC_CHECK_RETURN(ib[1] == 2);
    STATIC_CHECK_RETURN(ib[2] == 3);
    STATIC_CHECK_RETURN(ib[3] == 4);
    STATIC_CHECK_RETURN(ib[4] == 5);
    return true;
}

int main()
{
    test1<InputIterator<const int*>, OutputIterator<int*> >();
    test1<InputIterator<const int*>, InputIterator<int*> >();
    test1<InputIterator<const int*>, ForwardIterator<int*> >();
    test1<InputIterator<const int*>, BidirectionalIterator<int*> >();
    test1<InputIterator<const int*>, RandomAccessIterator<int*> >();
    test1<InputIterator<const int*>, int*>();

    test1<ForwardIterator<const int*>, OutputIterator<int*> >();
    test1<ForwardIterator<const int*>, InputIterator<int*> >();
    test1<ForwardIterator<const int*>, ForwardIterator<int*> >();
    test1<ForwardIterator<const int*>, BidirectionalIterator<int*> >();
    test1<ForwardIterator<const int*>, RandomAccessIterator<int*> >();
    test1<ForwardIterator<const int*>, int*>();

    test1<BidirectionalIterator<const int*>, OutputIterator<int*> >();
    test1<BidirectionalIterator<const int*>, InputIterator<int*> >();
    test1<BidirectionalIterator<const int*>, ForwardIterator<int*> >();
    test1<BidirectionalIterator<const int*>, BidirectionalIterator<int*> >();
    test1<BidirectionalIterator<const int*>, RandomAccessIterator<int*> >();
    test1<BidirectionalIterator<const int*>, int*>();

    test1<RandomAccessIterator<const int*>, OutputIterator<int*> >();
    test1<RandomAccessIterator<const int*>, InputIterator<int*> >();
    test1<RandomAccessIterator<const int*>, ForwardIterator<int*> >();
    test1<RandomAccessIterator<const int*>, BidirectionalIterator<int*> >();
    test1<RandomAccessIterator<const int*>, RandomAccessIterator<int*> >();
    test1<RandomAccessIterator<const int*>, int*>();

    test1<const int*, OutputIterator<int*> >();
    test1<const int*, InputIterator<int*> >();
    test1<const int*, ForwardIterator<int*> >();
    test1<const int*, BidirectionalIterator<int*> >();
    test1<const int*, RandomAccessIterator<int*> >();
    test1<const int*, int*>();


    test2<InputIterator<const int*>, InputIterator<const int*>, OutputIterator<int*> >();
    test2<InputIterator<const int*>, InputIterator<const int*>, InputIterator<int*> >();
    test2<InputIterator<const int*>, InputIterator<const int*>, ForwardIterator<int*> >();
    test2<InputIterator<const int*>, InputIterator<const int*>, BidirectionalIterator<int*> >();
    test2<InputIterator<const int*>, InputIterator<const int*>, RandomAccessIterator<int*> >();
    test2<InputIterator<const int*>, InputIterator<const int*>, int*>();

    test2<InputIterator<const int*>, ForwardIterator<const int*>, OutputIterator<int*> >();
    test2<InputIterator<const int*>, ForwardIterator<const int*>, InputIterator<int*> >();
    test2<InputIterator<const int*>, ForwardIterator<const int*>, ForwardIterator<int*> >();
    test2<InputIterator<const int*>, ForwardIterator<const int*>, BidirectionalIterator<int*> >();
    test2<InputIterator<const int*>, ForwardIterator<const int*>, RandomAccessIterator<int*> >();
    test2<InputIterator<const int*>, ForwardIterator<const int*>, int*>();

    test2<InputIterator<const int*>, BidirectionalIterator<const int*>, OutputIterator<int*> >();
    test2<InputIterator<const int*>, BidirectionalIterator<const int*>, InputIterator<int*> >();
    test2<InputIterator<const int*>, BidirectionalIterator<const int*>, ForwardIterator<int*> >();
    test2<InputIterator<const int*>, BidirectionalIterator<const int*>, BidirectionalIterator<int*> >();
    test2<InputIterator<const int*>, BidirectionalIterator<const int*>, RandomAccessIterator<int*> >();
    test2<InputIterator<const int*>, BidirectionalIterator<const int*>, int*>();

    test2<InputIterator<const int*>, RandomAccessIterator<const int*>, OutputIterator<int*> >();
    test2<InputIterator<const int*>, RandomAccessIterator<const int*>, InputIterator<int*> >();
    test2<InputIterator<const int*>, RandomAccessIterator<const int*>, ForwardIterator<int*> >();
    test2<InputIterator<const int*>, RandomAccessIterator<const int*>, BidirectionalIterator<int*> >();
    test2<InputIterator<const int*>, RandomAccessIterator<const int*>, RandomAccessIterator<int*> >();
    test2<InputIterator<const int*>, RandomAccessIterator<const int*>, int*>();

    test2<InputIterator<const int*>, const int*, OutputIterator<int*> >();
    test2<InputIterator<const int*>, const int*, InputIterator<int*> >();
    test2<InputIterator<const int*>, const int*, ForwardIterator<int*> >();
    test2<InputIterator<const int*>, const int*, BidirectionalIterator<int*> >();
    test2<InputIterator<const int*>, const int*, RandomAccessIterator<int*> >();
    test2<InputIterator<const int*>, const int*, int*>();

    test2<ForwardIterator<const int*>, InputIterator<const int*>, OutputIterator<int*> >();
    test2<ForwardIterator<const int*>, InputIterator<const int*>, InputIterator<int*> >();
    test2<ForwardIterator<const int*>, InputIterator<const int*>, ForwardIterator<int*> >();
    test2<ForwardIterator<const int*>, InputIterator<const int*>, BidirectionalIterator<int*> >();
    test2<ForwardIterator<const int*>, InputIterator<const int*>, RandomAccessIterator<int*> >();
    test2<ForwardIterator<const int*>, InputIterator<const int*>, int*>();

    test2<ForwardIterator<const int*>, ForwardIterator<const int*>, OutputIterator<int*> >();
    test2<ForwardIterator<const int*>, ForwardIterator<const int*>, InputIterator<int*> >();
    test2<ForwardIterator<const int*>, ForwardIterator<const int*>, ForwardIterator<int*> >();
    test2<ForwardIterator<const int*>, ForwardIterator<const int*>, BidirectionalIterator<int*> >();
    test2<ForwardIterator<const int*>, ForwardIterator<const int*>, RandomAccessIterator<int*> >();
    test2<ForwardIterator<const int*>, ForwardIterator<const int*>, int*>();

    test2<ForwardIterator<const int*>, BidirectionalIterator<const int*>, OutputIterator<int*> >();
    test2<ForwardIterator<const int*>, BidirectionalIterator<const int*>, InputIterator<int*> >();
    test2<ForwardIterator<const int*>, BidirectionalIterator<const int*>, ForwardIterator<int*> >();
    test2<ForwardIterator<const int*>, BidirectionalIterator<const int*>, BidirectionalIterator<int*> >();
    test2<ForwardIterator<const int*>, BidirectionalIterator<const int*>, RandomAccessIterator<int*> >();
    test2<ForwardIterator<const int*>, BidirectionalIterator<const int*>, int*>();

    test2<ForwardIterator<const int*>, RandomAccessIterator<const int*>, OutputIterator<int*> >();
    test2<ForwardIterator<const int*>, RandomAccessIterator<const int*>, InputIterator<int*> >();
    test2<ForwardIterator<const int*>, RandomAccessIterator<const int*>, ForwardIterator<int*> >();
    test2<ForwardIterator<const int*>, RandomAccessIterator<const int*>, BidirectionalIterator<int*> >();
    test2<ForwardIterator<const int*>, RandomAccessIterator<const int*>, RandomAccessIterator<int*> >();
    test2<ForwardIterator<const int*>, RandomAccessIterator<const int*>, int*>();

    test2<ForwardIterator<const int*>, const int*, OutputIterator<int*> >();
    test2<ForwardIterator<const int*>, const int*, InputIterator<int*> >();
    test2<ForwardIterator<const int*>, const int*, ForwardIterator<int*> >();
    test2<ForwardIterator<const int*>, const int*, BidirectionalIterator<int*> >();
    test2<ForwardIterator<const int*>, const int*, RandomAccessIterator<int*> >();
    test2<ForwardIterator<const int*>, const int*, int*>();

    test2<BidirectionalIterator<const int*>, InputIterator<const int*>, OutputIterator<int*> >();
    test2<BidirectionalIterator<const int*>, InputIterator<const int*>, InputIterator<int*> >();
    test2<BidirectionalIterator<const int*>, InputIterator<const int*>, ForwardIterator<int*> >();
    test2<BidirectionalIterator<const int*>, InputIterator<const int*>, BidirectionalIterator<int*> >();
    test2<BidirectionalIterator<const int*>, InputIterator<const int*>, RandomAccessIterator<int*> >();
    test2<BidirectionalIterator<const int*>, InputIterator<const int*>, int*>();

    test2<BidirectionalIterator<const int*>, ForwardIterator<const int*>, OutputIterator<int*> >();
    test2<BidirectionalIterator<const int*>, ForwardIterator<const int*>, InputIterator<int*> >();
    test2<BidirectionalIterator<const int*>, ForwardIterator<const int*>, ForwardIterator<int*> >();
    test2<BidirectionalIterator<const int*>, ForwardIterator<const int*>, BidirectionalIterator<int*> >();
    test2<BidirectionalIterator<const int*>, ForwardIterator<const int*>, RandomAccessIterator<int*> >();
    test2<BidirectionalIterator<const int*>, ForwardIterator<const int*>, int*>();

    test2<BidirectionalIterator<const int*>, BidirectionalIterator<const int*>, OutputIterator<int*> >();
    test2<BidirectionalIterator<const int*>, BidirectionalIterator<const int*>, InputIterator<int*> >();
    test2<BidirectionalIterator<const int*>, BidirectionalIterator<const int*>, ForwardIterator<int*> >();
    test2<BidirectionalIterator<const int*>, BidirectionalIterator<const int*>, BidirectionalIterator<int*> >();
    test2<BidirectionalIterator<const int*>, BidirectionalIterator<const int*>, RandomAccessIterator<int*> >();
    test2<BidirectionalIterator<const int*>, BidirectionalIterator<const int*>, int*>();

    test2<BidirectionalIterator<const int*>, RandomAccessIterator<const int*>, OutputIterator<int*> >();
    test2<BidirectionalIterator<const int*>, RandomAccessIterator<const int*>, InputIterator<int*> >();
    test2<BidirectionalIterator<const int*>, RandomAccessIterator<const int*>, ForwardIterator<int*> >();
    test2<BidirectionalIterator<const int*>, RandomAccessIterator<const int*>, BidirectionalIterator<int*> >();
    test2<BidirectionalIterator<const int*>, RandomAccessIterator<const int*>, RandomAccessIterator<int*> >();
    test2<BidirectionalIterator<const int*>, RandomAccessIterator<const int*>, int*>();

    test2<BidirectionalIterator<const int*>, const int*, OutputIterator<int*> >();
    test2<BidirectionalIterator<const int*>, const int*, InputIterator<int*> >();
    test2<BidirectionalIterator<const int*>, const int*, ForwardIterator<int*> >();
    test2<BidirectionalIterator<const int*>, const int*, BidirectionalIterator<int*> >();
    test2<BidirectionalIterator<const int*>, const int*, RandomAccessIterator<int*> >();
    test2<BidirectionalIterator<const int*>, const int*, int*>();

    test2<RandomAccessIterator<const int*>, InputIterator<const int*>, OutputIterator<int*> >();
    test2<RandomAccessIterator<const int*>, InputIterator<const int*>, InputIterator<int*> >();
    test2<RandomAccessIterator<const int*>, InputIterator<const int*>, ForwardIterator<int*> >();
    test2<RandomAccessIterator<const int*>, InputIterator<const int*>, BidirectionalIterator<int*> >();
    test2<RandomAccessIterator<const int*>, InputIterator<const int*>, RandomAccessIterator<int*> >();
    test2<RandomAccessIterator<const int*>, InputIterator<const int*>, int*>();

    test2<RandomAccessIterator<const int*>, ForwardIterator<const int*>, OutputIterator<int*> >();
    test2<RandomAccessIterator<const int*>, ForwardIterator<const int*>, InputIterator<int*> >();
    test2<RandomAccessIterator<const int*>, ForwardIterator<const int*>, ForwardIterator<int*> >();
    test2<RandomAccessIterator<const int*>, ForwardIterator<const int*>, BidirectionalIterator<int*> >();
    test2<RandomAccessIterator<const int*>, ForwardIterator<const int*>, RandomAccessIterator<int*> >();
    test2<RandomAccessIterator<const int*>, ForwardIterator<const int*>, int*>();

    test2<RandomAccessIterator<const int*>, BidirectionalIterator<const int*>, OutputIterator<int*> >();
    test2<RandomAccessIterator<const int*>, BidirectionalIterator<const int*>, InputIterator<int*> >();
    test2<RandomAccessIterator<const int*>, BidirectionalIterator<const int*>, ForwardIterator<int*> >();
    test2<RandomAccessIterator<const int*>, BidirectionalIterator<const int*>, BidirectionalIterator<int*> >();
    test2<RandomAccessIterator<const int*>, BidirectionalIterator<const int*>, RandomAccessIterator<int*> >();
    test2<RandomAccessIterator<const int*>, BidirectionalIterator<const int*>, int*>();

    test2<RandomAccessIterator<const int*>, RandomAccessIterator<const int*>, OutputIterator<int*> >();
    test2<RandomAccessIterator<const int*>, RandomAccessIterator<const int*>, InputIterator<int*> >();
    test2<RandomAccessIterator<const int*>, RandomAccessIterator<const int*>, ForwardIterator<int*> >();
    test2<RandomAccessIterator<const int*>, RandomAccessIterator<const int*>, BidirectionalIterator<int*> >();
    test2<RandomAccessIterator<const int*>, RandomAccessIterator<const int*>, RandomAccessIterator<int*> >();
    test2<RandomAccessIterator<const int*>, RandomAccessIterator<const int*>, int*>();

    test2<RandomAccessIterator<const int*>, const int*, OutputIterator<int*> >();
    test2<RandomAccessIterator<const int*>, const int*, InputIterator<int*> >();
    test2<RandomAccessIterator<const int*>, const int*, ForwardIterator<int*> >();
    test2<RandomAccessIterator<const int*>, const int*, BidirectionalIterator<int*> >();
    test2<RandomAccessIterator<const int*>, const int*, RandomAccessIterator<int*> >();
    test2<RandomAccessIterator<const int*>, const int*, int*>();

    test2<const int*, InputIterator<const int*>, OutputIterator<int*> >();
    test2<const int*, InputIterator<const int*>, InputIterator<int*> >();
    test2<const int*, InputIterator<const int*>, ForwardIterator<int*> >();
    test2<const int*, InputIterator<const int*>, BidirectionalIterator<int*> >();
    test2<const int*, InputIterator<const int*>, RandomAccessIterator<int*> >();
    test2<const int*, InputIterator<const int*>, int*>();

    test2<const int*, ForwardIterator<const int*>, OutputIterator<int*> >();
    test2<const int*, ForwardIterator<const int*>, InputIterator<int*> >();
    test2<const int*, ForwardIterator<const int*>, ForwardIterator<int*> >();
    test2<const int*, ForwardIterator<const int*>, BidirectionalIterator<int*> >();
    test2<const int*, ForwardIterator<const int*>, RandomAccessIterator<int*> >();
    test2<const int*, ForwardIterator<const int*>, int*>();

    test2<const int*, BidirectionalIterator<const int*>, OutputIterator<int*> >();
    test2<const int*, BidirectionalIterator<const int*>, InputIterator<int*> >();
    test2<const int*, BidirectionalIterator<const int*>, ForwardIterator<int*> >();
    test2<const int*, BidirectionalIterator<const int*>, BidirectionalIterator<int*> >();
    test2<const int*, BidirectionalIterator<const int*>, RandomAccessIterator<int*> >();
    test2<const int*, BidirectionalIterator<const int*>, int*>();

    test2<const int*, RandomAccessIterator<const int*>, OutputIterator<int*> >();
    test2<const int*, RandomAccessIterator<const int*>, InputIterator<int*> >();
    test2<const int*, RandomAccessIterator<const int*>, ForwardIterator<int*> >();
    test2<const int*, RandomAccessIterator<const int*>, BidirectionalIterator<int*> >();
    test2<const int*, RandomAccessIterator<const int*>, RandomAccessIterator<int*> >();
    test2<const int*, RandomAccessIterator<const int*>, int*>();

    test2<const int*, const int*, OutputIterator<int*> >();
    test2<const int*, const int*, InputIterator<int*> >();
    test2<const int*, const int*, ForwardIterator<int*> >();
    test2<const int*, const int*, BidirectionalIterator<int*> >();
    test2<const int*, const int*, RandomAccessIterator<int*> >();
    test2<const int*, const int*, int*>();

    int *p = nullptr;
    auto unary = [](int i){return i + 1; };
    auto binary = [](int i, int j){return i + j; };
    S const s[] = {S{1}, S{2}, S{3}, S{4}};
    int const i[] = {1, 2, 3, 4};
    static_assert(std::is_same<ranges::unary_transform_result<S const*, int*>,
        decltype(ranges::transform(s, p, unary, &S::i))>::value, "");
    static_assert(std::is_same<ranges::binary_transform_result<S const*, int const *, int*>,
        decltype(ranges::transform(s, i, p, binary, &S::i))>::value, "");
    static_assert(std::is_same<ranges::binary_transform_result<S const*, S const *, int*>,
        decltype(ranges::transform(s, s, p, binary, &S::i, &S::i))>::value, "");

    {
        STATIC_CHECK(test_constexpr());
    }
    
    return ::test_result();
}
