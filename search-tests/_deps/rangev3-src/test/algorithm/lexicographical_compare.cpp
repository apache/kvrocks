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

#include <range/v3/core.hpp>
#include <range/v3/algorithm/lexicographical_compare.hpp>
#include "../array.hpp"
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

template<class Iter1, class Iter2, class Sent1 = Iter1, class Sent2 = Iter2>
void
test_iter1()
{
    int ia[] = {1, 2, 3, 4};
    constexpr auto sa = ranges::size(ia);
    int ib[] = {1, 2, 3};
    CHECK(!ranges::lexicographical_compare(Iter1(ia), Sent1(ia+sa), Iter2(ib), Sent2(ib+2)));
    CHECK(ranges::lexicographical_compare(Iter1(ib), Sent1(ib+2), Iter2(ia), Sent2(ia+sa)));
    CHECK(!ranges::lexicographical_compare(Iter1(ia), Sent1(ia+sa), Iter2(ib), Sent2(ib+3)));
    CHECK(ranges::lexicographical_compare(Iter1(ib), Sent1(ib+3), Iter2(ia), Sent2(ia+sa)));
    CHECK(ranges::lexicographical_compare(Iter1(ia), Sent1(ia+sa), Iter2(ib+1), Sent2(ib+3)));
    CHECK(!ranges::lexicographical_compare(Iter1(ib+1), Sent1(ib+3), Iter2(ia), Sent2(ia+sa)));
}

void test_iter()
{
    typedef Sentinel<const int*> S;

    test_iter1<InputIterator<const int*>, InputIterator<const int*> >();
    test_iter1<InputIterator<const int*>, ForwardIterator<const int*> >();
    test_iter1<InputIterator<const int*>, BidirectionalIterator<const int*> >();
    test_iter1<InputIterator<const int*>, RandomAccessIterator<const int*> >();
    test_iter1<InputIterator<const int*>, InputIterator<const int*>, S, S>();
    test_iter1<InputIterator<const int*>, ForwardIterator<const int*>, S, S>();
    test_iter1<InputIterator<const int*>, BidirectionalIterator<const int*>, S, S>();
    test_iter1<InputIterator<const int*>, RandomAccessIterator<const int*>, S, S>();
    test_iter1<InputIterator<const int*>, const int*>();

    test_iter1<ForwardIterator<const int*>, InputIterator<const int*> >();
    test_iter1<ForwardIterator<const int*>, ForwardIterator<const int*> >();
    test_iter1<ForwardIterator<const int*>, BidirectionalIterator<const int*> >();
    test_iter1<ForwardIterator<const int*>, RandomAccessIterator<const int*> >();
    test_iter1<ForwardIterator<const int*>, InputIterator<const int*>, S, S>();
    test_iter1<ForwardIterator<const int*>, ForwardIterator<const int*>, S, S>();
    test_iter1<ForwardIterator<const int*>, BidirectionalIterator<const int*>, S, S>();
    test_iter1<ForwardIterator<const int*>, RandomAccessIterator<const int*>, S, S>();
    test_iter1<ForwardIterator<const int*>, const int*>();

    test_iter1<BidirectionalIterator<const int*>, InputIterator<const int*> >();
    test_iter1<BidirectionalIterator<const int*>, ForwardIterator<const int*> >();
    test_iter1<BidirectionalIterator<const int*>, BidirectionalIterator<const int*> >();
    test_iter1<BidirectionalIterator<const int*>, RandomAccessIterator<const int*> >();
    test_iter1<BidirectionalIterator<const int*>, InputIterator<const int*>, S, S>();
    test_iter1<BidirectionalIterator<const int*>, ForwardIterator<const int*>, S, S>();
    test_iter1<BidirectionalIterator<const int*>, BidirectionalIterator<const int*>, S, S>();
    test_iter1<BidirectionalIterator<const int*>, RandomAccessIterator<const int*>, S, S>();
    test_iter1<BidirectionalIterator<const int*>, const int*>();

    test_iter1<RandomAccessIterator<const int*>, InputIterator<const int*> >();
    test_iter1<RandomAccessIterator<const int*>, ForwardIterator<const int*> >();
    test_iter1<RandomAccessIterator<const int*>, BidirectionalIterator<const int*> >();
    test_iter1<RandomAccessIterator<const int*>, RandomAccessIterator<const int*> >();
    test_iter1<RandomAccessIterator<const int*>, InputIterator<const int*>, S, S>();
    test_iter1<RandomAccessIterator<const int*>, ForwardIterator<const int*>, S, S>();
    test_iter1<RandomAccessIterator<const int*>, BidirectionalIterator<const int*>, S, S>();
    test_iter1<RandomAccessIterator<const int*>, RandomAccessIterator<const int*>, S, S>();
    test_iter1<RandomAccessIterator<const int*>, const int*>();

    test_iter1<const int*, InputIterator<const int*> >();
    test_iter1<const int*, ForwardIterator<const int*> >();
    test_iter1<const int*, BidirectionalIterator<const int*> >();
    test_iter1<const int*, RandomAccessIterator<const int*> >();
    test_iter1<const int*, InputIterator<const int*>, const int*, S>();
    test_iter1<const int*, ForwardIterator<const int*>, const int*, S>();
    test_iter1<const int*, BidirectionalIterator<const int*>, const int*, S>();
    test_iter1<const int*, RandomAccessIterator<const int*>, const int*, S>();
    test_iter1<const int*, const int*>();
}

template<class Iter1, class Iter2, class Sent1 = Iter1, class Sent2 = Iter2>
void
test_iter_comp1()
{
    int ia[] = {1, 2, 3, 4};
    const unsigned sa = sizeof(ia)/sizeof(ia[0]);
    int ib[] = {1, 2, 3};
    typedef std::greater<int> C;
    C c;
    CHECK(!ranges::lexicographical_compare(Iter1(ia), Sent1(ia+sa), Iter2(ib), Sent2(ib+2), c));
    CHECK(ranges::lexicographical_compare(Iter1(ib), Sent1(ib+2), Iter2(ia), Sent2(ia+sa), c));
    CHECK(!ranges::lexicographical_compare(Iter1(ia), Sent1(ia+sa), Iter2(ib), Sent2(ib+3), c));
    CHECK(ranges::lexicographical_compare(Iter1(ib), Sent1(ib+3), Iter2(ia), Sent2(ia+sa), c));
    CHECK(!ranges::lexicographical_compare(Iter1(ia), Sent1(ia+sa), Iter2(ib+1), Sent2(ib+3), c));
    CHECK(ranges::lexicographical_compare(Iter1(ib+1), Sent1(ib+3), Iter2(ia), Sent2(ia+sa), c));
}

void test_iter_comp()
{
    typedef Sentinel<const int*> S;

    test_iter_comp1<InputIterator<const int*>, InputIterator<const int*> >();
    test_iter_comp1<InputIterator<const int*>, ForwardIterator<const int*> >();
    test_iter_comp1<InputIterator<const int*>, BidirectionalIterator<const int*> >();
    test_iter_comp1<InputIterator<const int*>, RandomAccessIterator<const int*> >();
    test_iter_comp1<InputIterator<const int*>, InputIterator<const int*>, S, S>();
    test_iter_comp1<InputIterator<const int*>, ForwardIterator<const int*>, S, S>();
    test_iter_comp1<InputIterator<const int*>, BidirectionalIterator<const int*>, S, S>();
    test_iter_comp1<InputIterator<const int*>, RandomAccessIterator<const int*>, S, S>();
    test_iter_comp1<InputIterator<const int*>, const int*>();

    test_iter_comp1<ForwardIterator<const int*>, InputIterator<const int*> >();
    test_iter_comp1<ForwardIterator<const int*>, ForwardIterator<const int*> >();
    test_iter_comp1<ForwardIterator<const int*>, BidirectionalIterator<const int*> >();
    test_iter_comp1<ForwardIterator<const int*>, RandomAccessIterator<const int*> >();
    test_iter_comp1<ForwardIterator<const int*>, InputIterator<const int*>, S, S>();
    test_iter_comp1<ForwardIterator<const int*>, ForwardIterator<const int*>, S, S>();
    test_iter_comp1<ForwardIterator<const int*>, BidirectionalIterator<const int*>, S, S>();
    test_iter_comp1<ForwardIterator<const int*>, RandomAccessIterator<const int*>, S, S>();
    test_iter_comp1<ForwardIterator<const int*>, const int*>();

    test_iter_comp1<BidirectionalIterator<const int*>, InputIterator<const int*> >();
    test_iter_comp1<BidirectionalIterator<const int*>, ForwardIterator<const int*> >();
    test_iter_comp1<BidirectionalIterator<const int*>, BidirectionalIterator<const int*> >();
    test_iter_comp1<BidirectionalIterator<const int*>, RandomAccessIterator<const int*> >();
    test_iter_comp1<BidirectionalIterator<const int*>, InputIterator<const int*>, S, S>();
    test_iter_comp1<BidirectionalIterator<const int*>, ForwardIterator<const int*>, S, S>();
    test_iter_comp1<BidirectionalIterator<const int*>, BidirectionalIterator<const int*>, S, S>();
    test_iter_comp1<BidirectionalIterator<const int*>, RandomAccessIterator<const int*>, S, S>();
    test_iter_comp1<BidirectionalIterator<const int*>, const int*>();

    test_iter_comp1<RandomAccessIterator<const int*>, InputIterator<const int*> >();
    test_iter_comp1<RandomAccessIterator<const int*>, ForwardIterator<const int*> >();
    test_iter_comp1<RandomAccessIterator<const int*>, BidirectionalIterator<const int*> >();
    test_iter_comp1<RandomAccessIterator<const int*>, RandomAccessIterator<const int*> >();
    test_iter_comp1<RandomAccessIterator<const int*>, InputIterator<const int*>, S, S>();
    test_iter_comp1<RandomAccessIterator<const int*>, ForwardIterator<const int*>, S, S>();
    test_iter_comp1<RandomAccessIterator<const int*>, BidirectionalIterator<const int*>, S, S>();
    test_iter_comp1<RandomAccessIterator<const int*>, RandomAccessIterator<const int*>, S, S>();
    test_iter_comp1<RandomAccessIterator<const int*>, const int*>();

    test_iter_comp1<const int*, InputIterator<const int*> >();
    test_iter_comp1<const int*, ForwardIterator<const int*> >();
    test_iter_comp1<const int*, BidirectionalIterator<const int*> >();
    test_iter_comp1<const int*, RandomAccessIterator<const int*> >();
    test_iter_comp1<const int*, InputIterator<const int*>, const int*, S>();
    test_iter_comp1<const int*, ForwardIterator<const int*>, const int*, S>();
    test_iter_comp1<const int*, BidirectionalIterator<const int*>, const int*, S>();
    test_iter_comp1<const int*, RandomAccessIterator<const int*>, const int*, S>();
    test_iter_comp1<const int*, const int*>();
}

constexpr bool test_constexpr()
{
    test::array<int, 4> ia{{1, 2, 3, 4}};
    test::array<int, 3> ib{{1, 2, 3}};
    auto ia_b = ranges::begin(ia);
    auto ia_e = ranges::end(ia);
    auto ib_b = ranges::begin(ib);
    auto ib_1 = ib_b + 1;
    auto ib_2 = ib_b + 2;
    auto ib_3 = ib_b + 3;

    STATIC_CHECK_RETURN(!ranges::lexicographical_compare(ia_b, ia_e, ib_b, ib_2));
    STATIC_CHECK_RETURN(ranges::lexicographical_compare(ib_b, ib_2, ia_b, ia_e));
    STATIC_CHECK_RETURN(!ranges::lexicographical_compare(ia_b, ia_e, ib_b, ib_3));
    STATIC_CHECK_RETURN(ranges::lexicographical_compare(ib_b, ib_3, ia_b, ia_e));
    STATIC_CHECK_RETURN(ranges::lexicographical_compare(ia_b, ia_e, ib_1, ib_3));
    STATIC_CHECK_RETURN(!ranges::lexicographical_compare(ib_1, ib_3, ia_b, ia_e));

    typedef std::greater<int> C;
    C c{};
    STATIC_CHECK_RETURN(!ranges::lexicographical_compare(ia_b, ia_e, ib_b, ib_2, c));
    STATIC_CHECK_RETURN(ranges::lexicographical_compare(ib_b, ib_2, ia_b, ia_e, c));
    STATIC_CHECK_RETURN(!ranges::lexicographical_compare(ia_b, ia_e, ib_b, ib_3, c));
    STATIC_CHECK_RETURN(ranges::lexicographical_compare(ib_b, ib_3, ia_b, ia_e, c));
    STATIC_CHECK_RETURN(!ranges::lexicographical_compare(ia_b, ia_e, ib_1, ib_3, c));
    STATIC_CHECK_RETURN(ranges::lexicographical_compare(ib_1, ib_3, ia_b, ia_e, c));
    return true;
}

int main()
{
    test_iter();
    test_iter_comp();

    {
        STATIC_CHECK(test_constexpr());
    }
    return test_result();
}
