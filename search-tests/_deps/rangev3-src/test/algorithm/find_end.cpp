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

#include <utility>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/find_end.hpp>
#include "../simple_test.hpp"
#include "../test_iterators.hpp"

constexpr bool test_constexpr()
{
    using namespace ranges;
    int ia[] = {0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 0, 1, 2, 3, 0, 1, 2, 0, 1, 0};
    auto ia_b = begin(ia);
    auto ia_e = end(ia);

    constexpr auto sa = size(ia);
    int b[] = {0};
    int c[] = {0, 1};
    int d[] = {0, 1, 2};
    int e[] = {0, 1, 2, 3};
    int f[] = {0, 1, 2, 3, 4};
    int g[] = {0, 1, 2, 3, 4, 5};
    int h[] = {0, 1, 2, 3, 4, 5, 6};
    STATIC_CHECK_RETURN(find_end(ia_b, ia_e, begin(b), b + 1).begin() == ia + sa - 1);
    STATIC_CHECK_RETURN(find_end(ia_b, ia_e, begin(c), c + 2).begin() == ia + 18);
    STATIC_CHECK_RETURN(find_end(ia_b, ia_e, begin(d), d + 3).begin() == ia + 15);
    STATIC_CHECK_RETURN(find_end(ia_b, ia_e, begin(e), e + 4).begin() == ia + 11);
    STATIC_CHECK_RETURN(find_end(ia_b, ia_e, begin(f), f + 5).begin() == ia + 6);
    STATIC_CHECK_RETURN(find_end(ia_b, ia_e, begin(g), g + 6).begin() == ia);
    STATIC_CHECK_RETURN(find_end(ia_b, ia_e, begin(h), h + 7).begin() == ia + sa);
    STATIC_CHECK_RETURN(find_end(ia_b, ia_e, begin(b), b).begin() == ia + sa);
    STATIC_CHECK_RETURN(find_end(ia_b, ia_b, begin(b), b + 1).begin() == ia);

    auto ir = make_subrange(ia_b, ia_e);
    STATIC_CHECK_RETURN(find_end(ir, make_subrange(begin(b), b + 1)).begin() == ia + sa - 1);
    STATIC_CHECK_RETURN(find_end(ir, make_subrange(begin(c), c + 2)).begin() == ia + 18);
    STATIC_CHECK_RETURN(find_end(ir, make_subrange(begin(d), d + 3)).begin() == ia + 15);
    STATIC_CHECK_RETURN(find_end(ir, make_subrange(begin(e), e + 4)).begin() == ia + 11);
    STATIC_CHECK_RETURN(find_end(ir, make_subrange(begin(f), f + 5)).begin() == ia + 6);
    STATIC_CHECK_RETURN(find_end(ir, make_subrange(begin(g), g + 6)).begin() == ia);
    STATIC_CHECK_RETURN(find_end(ir, make_subrange(begin(h), h + 7)).begin() == ia + sa);
    STATIC_CHECK_RETURN(find_end(ir, make_subrange(begin(b), b)).begin() == ia + sa);
    STATIC_CHECK_RETURN(find_end(std::move(ir), make_subrange(begin(b), b + 1)).begin() == ia + sa - 1);
    STATIC_CHECK_RETURN(find_end(std::move(ir), make_subrange(begin(c), c + 2)).begin() == ia + 18);
    STATIC_CHECK_RETURN(find_end(std::move(ir), make_subrange(begin(d), d + 3)).begin() == ia + 15);
    STATIC_CHECK_RETURN(find_end(std::move(ir), make_subrange(begin(e), e + 4)).begin() == ia + 11);
    STATIC_CHECK_RETURN(find_end(std::move(ir), make_subrange(begin(f), f + 5)).begin() == ia + 6);
    STATIC_CHECK_RETURN(find_end(std::move(ir), make_subrange(begin(g), g + 6)).begin() == ia);
    STATIC_CHECK_RETURN(find_end(std::move(ir), make_subrange(begin(h), h + 7)).begin() == ia + sa);
    STATIC_CHECK_RETURN(find_end(std::move(ir), make_subrange(begin(b), b)).begin() == ia + sa);

    auto er = make_subrange(ia_b, ia);
    STATIC_CHECK_RETURN(find_end(er, make_subrange(b, b + 1)).begin() == ia);
    STATIC_CHECK_RETURN(find_end(std::move(er), make_subrange(b, b + 1)).begin() == ia);
    return true;
}

template<class Iter1, class Iter2, typename Sent1 = Iter1, typename Sent2 = Iter2>
void
test()
{
    using namespace ranges;

    int ia[] = {0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 0, 1, 2, 3, 0, 1, 2, 0, 1, 0};
    constexpr auto sa = size(ia);
    int b[] = {0};
    int c[] = {0, 1};
    int d[] = {0, 1, 2};
    int e[] = {0, 1, 2, 3};
    int f[] = {0, 1, 2, 3, 4};
    int g[] = {0, 1, 2, 3, 4, 5};
    int h[] = {0, 1, 2, 3, 4, 5, 6};
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(b), Sent2(b + 1)).begin() == Iter1(ia + sa - 1));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(b), Sent2(b + 1)).end() == Iter1(ia + sa - 1 + 1));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(c), Sent2(c + 2)).begin() == Iter1(ia + 18));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(c), Sent2(c + 2)).end() == Iter1(ia + 18 + 2));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(d), Sent2(d + 3)).begin() == Iter1(ia + 15));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(d), Sent2(d + 3)).end() == Iter1(ia + 15 + 3));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(e), Sent2(e + 4)).begin() == Iter1(ia + 11));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(e), Sent2(e + 4)).end() == Iter1(ia + 11 + 4));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(f), Sent2(f + 5)).begin() == Iter1(ia + 6));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(f), Sent2(f + 5)).end() == Iter1(ia + 6 + 5));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(g), Sent2(g + 6)).begin() == Iter1(ia));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(g), Sent2(g + 6)).end() == Iter1(ia + 6));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(h), Sent2(h + 7)).begin() == Iter1(ia + sa));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(h), Sent2(h + 7)).end() == Iter1(ia + sa));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(b), Sent2(b)).begin() == Iter1(ia + sa));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(b), Sent2(b)).end() == Iter1(ia + sa));
    CHECK(find_end(Iter1(ia), Sent1(ia), Iter2(b), Sent2(b + 1)).begin() == Iter1(ia));
    CHECK(find_end(Iter1(ia), Sent1(ia), Iter2(b), Sent2(b + 1)).end() == Iter1(ia));

    auto ir = make_subrange(Iter1(ia), Sent1(ia + sa));
    CHECK(find_end(ir, make_subrange(Iter2(b), Sent2(b + 1))).begin() == Iter1(ia + sa - 1));
    CHECK(find_end(ir, make_subrange(Iter2(b), Sent2(b + 1))).end() == Iter1(ia + sa - 1 + 1));
    CHECK(find_end(ir, make_subrange(Iter2(c), Sent2(c + 2))).begin() == Iter1(ia + 18));
    CHECK(find_end(ir, make_subrange(Iter2(c), Sent2(c + 2))).end() == Iter1(ia + 18 + 2));
    CHECK(find_end(ir, make_subrange(Iter2(d), Sent2(d + 3))).begin() == Iter1(ia + 15));
    CHECK(find_end(ir, make_subrange(Iter2(d), Sent2(d + 3))).end() == Iter1(ia + 15 + 3));
    CHECK(find_end(ir, make_subrange(Iter2(e), Sent2(e + 4))).begin() == Iter1(ia + 11));
    CHECK(find_end(ir, make_subrange(Iter2(e), Sent2(e + 4))).end() == Iter1(ia + 11 + 4));
    CHECK(find_end(ir, make_subrange(Iter2(f), Sent2(f + 5))).begin() == Iter1(ia + 6));
    CHECK(find_end(ir, make_subrange(Iter2(f), Sent2(f + 5))).end() == Iter1(ia + 6 + 5));
    CHECK(find_end(ir, make_subrange(Iter2(g), Sent2(g + 6))).begin() == Iter1(ia));
    CHECK(find_end(ir, make_subrange(Iter2(g), Sent2(g + 6))).end() == Iter1(ia + 6));
    CHECK(find_end(ir, make_subrange(Iter2(h), Sent2(h + 7))).begin() == Iter1(ia + sa));
    CHECK(find_end(ir, make_subrange(Iter2(h), Sent2(h + 7))).end() == Iter1(ia + sa));
    CHECK(find_end(ir, make_subrange(Iter2(b), Sent2(b))).begin() == Iter1(ia + sa));
    CHECK(find_end(ir, make_subrange(Iter2(b), Sent2(b))).end() == Iter1(ia + sa));

    CHECK(find_end(std::move(ir), make_subrange(Iter2(b), Sent2(b + 1))).begin() == Iter1(ia + sa - 1));
    CHECK(find_end(std::move(ir), make_subrange(Iter2(c), Sent2(c + 2))).begin() == Iter1(ia + 18));
    CHECK(find_end(std::move(ir), make_subrange(Iter2(c), Sent2(c + 2))).end() == Iter1(ia + 18 + 2));
    CHECK(find_end(std::move(ir), make_subrange(Iter2(d), Sent2(d + 3))).begin() == Iter1(ia + 15));
    CHECK(find_end(std::move(ir), make_subrange(Iter2(d), Sent2(d + 3))).end() == Iter1(ia + 15 + 3));
    CHECK(find_end(std::move(ir), make_subrange(Iter2(e), Sent2(e + 4))).begin() == Iter1(ia + 11));
    CHECK(find_end(std::move(ir), make_subrange(Iter2(e), Sent2(e + 4))).end() == Iter1(ia + 11 + 4));
    CHECK(find_end(std::move(ir), make_subrange(Iter2(f), Sent2(f + 5))).begin() == Iter1(ia + 6));
    CHECK(find_end(std::move(ir), make_subrange(Iter2(f), Sent2(f + 5))).end() == Iter1(ia + 6 + 5));
    CHECK(find_end(std::move(ir), make_subrange(Iter2(g), Sent2(g + 6))).begin() == Iter1(ia));
    CHECK(find_end(std::move(ir), make_subrange(Iter2(g), Sent2(g + 6))).end() == Iter1(ia + 6));
    CHECK(find_end(std::move(ir), make_subrange(Iter2(h), Sent2(h + 7))).begin() == Iter1(ia + sa));
    CHECK(find_end(std::move(ir), make_subrange(Iter2(h), Sent2(h + 7))).end() == Iter1(ia + sa));
    CHECK(find_end(std::move(ir), make_subrange(Iter2(b), Sent2(b))).begin() == Iter1(ia + sa));

    auto er = make_subrange(Iter1(ia), Sent1(ia));
    CHECK(find_end(er, make_subrange(Iter2(b), Sent2(b + 1))).begin() == Iter1(ia));
    CHECK(find_end(er, make_subrange(Iter2(b), Sent2(b + 1))).end() == Iter1(ia));
    CHECK(find_end(std::move(er), make_subrange(Iter2(b), Sent2(b + 1))).begin() == Iter1(ia));
    CHECK(find_end(std::move(er), make_subrange(Iter2(b), Sent2(b + 1))).end() == Iter1(ia));
}

struct count_equal
{
    static unsigned count;
    template<class T>
    bool operator()(const T& x, const T& y)
    {
        ++count; return x == y;
    }
};

unsigned count_equal::count = 0;

template<class Iter1, class Iter2, typename Sent1 = Iter1, typename Sent2 = Iter2>
void
test_pred()
{
    using namespace ranges;

    int ia[] = {0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 0, 1, 2, 3, 0, 1, 2, 0, 1, 0};
    constexpr auto sa = size(ia);
    int b[] = {0};
    int c[] = {0, 1};
    int d[] = {0, 1, 2};
    int e[] = {0, 1, 2, 3};
    int f[] = {0, 1, 2, 3, 4};
    int g[] = {0, 1, 2, 3, 4, 5};
    int h[] = {0, 1, 2, 3, 4, 5, 6};

    count_equal::count = 0;
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(b), Sent2(b + 1), count_equal()).begin() == Iter1(ia + sa - 1));
    CHECK(count_equal::count <= 1 * (sa - 1 + 1));
    count_equal::count = 0;
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(c), Sent2(c + 2), count_equal()).begin() == Iter1(ia + 18));
    CHECK(count_equal::count <= 2 * (sa - 2 + 1));
    count_equal::count = 0;
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(d), Sent2(d + 3), count_equal()).begin() == Iter1(ia + 15));
    CHECK(count_equal::count <= 3 * (sa - 3 + 1));
    count_equal::count = 0;
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(e), Sent2(e + 4), count_equal()).begin() == Iter1(ia + 11));
    CHECK(count_equal::count <= 4 * (sa - 4 + 1));
    count_equal::count = 0;
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(f), Sent2(f + 5), count_equal()).begin() == Iter1(ia + 6));
    CHECK(count_equal::count <= 5 * (sa - 5 + 1));
    count_equal::count = 0;
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(g), Sent2(g + 6), count_equal()).begin() == Iter1(ia));
    CHECK(count_equal::count <= 6 * (sa - 6 + 1));
    count_equal::count = 0;
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(h), Sent2(h + 7), count_equal()).begin() == Iter1(ia + sa));
    CHECK(count_equal::count <= 7 * (sa - 7 + 1));
    count_equal::count = 0;
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(b), Sent2(b), count_equal()).begin() == Iter1(ia + sa));
    CHECK(count_equal::count == 0u);
    count_equal::count = 0;
    CHECK(find_end(Iter1(ia), Sent1(ia), Iter2(b), Sent2(b + 1), count_equal()).begin() == Iter1(ia));
    CHECK(count_equal::count == 0u);

    auto ir = make_subrange(Iter1(ia), Sent1(ia + sa));
    count_equal::count = 0;
    CHECK(find_end(ir, make_subrange(Iter2(b), Sent2(b + 1)), count_equal()).begin() == Iter1(ia + sa - 1));
    CHECK(count_equal::count <= 1 * (sa - 1 + 1));
    count_equal::count = 0;
    CHECK(find_end(ir, make_subrange(Iter2(c), Sent2(c + 2)), count_equal()).begin() == Iter1(ia + 18));
    CHECK(count_equal::count <= 2 * (sa - 2 + 1));
    count_equal::count = 0;
    CHECK(find_end(ir, make_subrange(Iter2(d), Sent2(d + 3)), count_equal()).begin() == Iter1(ia + 15));
    CHECK(count_equal::count <= 3 * (sa - 3 + 1));
    count_equal::count = 0;
    CHECK(find_end(ir, make_subrange(Iter2(e), Sent2(e + 4)), count_equal()).begin() == Iter1(ia + 11));
    CHECK(count_equal::count <= 4 * (sa - 4 + 1));
    count_equal::count = 0;
    CHECK(find_end(ir, make_subrange(Iter2(f), Sent2(f + 5)), count_equal()).begin() == Iter1(ia + 6));
    CHECK(count_equal::count <= 5 * (sa - 5 + 1));
    count_equal::count = 0;
    CHECK(find_end(ir, make_subrange(Iter2(g), Sent2(g + 6)), count_equal()).begin() == Iter1(ia));
    CHECK(count_equal::count <= 6 * (sa - 6 + 1));
    count_equal::count = 0;
    CHECK(find_end(ir, make_subrange(Iter2(h), Sent2(h + 7)), count_equal()).begin() == Iter1(ia + sa));
    CHECK(count_equal::count <= 7 * (sa - 7 + 1));
    count_equal::count = 0;
    CHECK(find_end(ir, make_subrange(Iter2(b), Sent2(b)), count_equal()).begin() == Iter1(ia + sa));
    CHECK(count_equal::count == 0u);
    count_equal::count = 0;
    auto er = make_subrange(Iter1(ia), Sent1(ia));
    CHECK(find_end(er, make_subrange(Iter2(b), Sent2(b + 1)), count_equal()).begin() == Iter1(ia));
    CHECK(count_equal::count == 0u);

    static_assert(std::is_same<subrange<Iter1>, decltype(find_end(er, {1, 2, 3}))>::value, "");
}

struct S
{
    int i_;
};

template<class Iter1, class Iter2, typename Sent1 = Iter1, typename Sent2 = Iter2>
void
test_proj()
{
    using namespace ranges;

    S ia[] = {{0}, {1}, {2}, {3}, {4}, {5}, {0}, {1}, {2}, {3}, {4}, {0}, {1}, {2}, {3}, {0}, {1}, {2}, {0}, {1}, {0}};
    constexpr auto sa = size(ia);
    int b[] = {0};
    int c[] = {0, 1};
    int d[] = {0, 1, 2};
    int e[] = {0, 1, 2, 3};
    int f[] = {0, 1, 2, 3, 4};
    int g[] = {0, 1, 2, 3, 4, 5};
    int h[] = {0, 1, 2, 3, 4, 5, 6};
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(b), Sent2(b + 1), equal_to(), &S::i_).begin() == Iter1(ia + sa - 1));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(c), Sent2(c + 2), equal_to(), &S::i_).begin() == Iter1(ia + 18));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(d), Sent2(d + 3), equal_to(), &S::i_).begin() == Iter1(ia + 15));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(e), Sent2(e + 4), equal_to(), &S::i_).begin() == Iter1(ia + 11));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(f), Sent2(f + 5), equal_to(), &S::i_).begin() == Iter1(ia + 6));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(g), Sent2(g + 6), equal_to(), &S::i_).begin() == Iter1(ia));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(h), Sent2(h + 7), equal_to(), &S::i_).begin() == Iter1(ia + sa));
    CHECK(find_end(Iter1(ia), Sent1(ia + sa), Iter2(b), Sent2(b), equal_to(), &S::i_).begin() == Iter1(ia + sa));
    CHECK(find_end(Iter1(ia), Sent1(ia), Iter2(b), Sent2(b + 1), equal_to(), &S::i_).begin() == Iter1(ia));

    auto ir = make_subrange(Iter1(ia), Sent1(ia + sa));
    CHECK(find_end(ir, make_subrange(Iter2(b), Sent2(b + 1)), equal_to(), &S::i_).begin() == Iter1(ia + sa - 1));
    CHECK(find_end(ir, make_subrange(Iter2(c), Sent2(c + 2)), equal_to(), &S::i_).begin() == Iter1(ia + 18));
    CHECK(find_end(ir, make_subrange(Iter2(d), Sent2(d + 3)), equal_to(), &S::i_).begin() == Iter1(ia + 15));
    CHECK(find_end(ir, make_subrange(Iter2(e), Sent2(e + 4)), equal_to(), &S::i_).begin() == Iter1(ia + 11));
    CHECK(find_end(ir, make_subrange(Iter2(f), Sent2(f + 5)), equal_to(), &S::i_).begin() == Iter1(ia + 6));
    CHECK(find_end(ir, make_subrange(Iter2(g), Sent2(g + 6)), equal_to(), &S::i_).begin() == Iter1(ia));
    CHECK(find_end(ir, make_subrange(Iter2(h), Sent2(h + 7)), equal_to(), &S::i_).begin() == Iter1(ia + sa));
    CHECK(find_end(ir, make_subrange(Iter2(b), Sent2(b)), equal_to(), &S::i_).begin() == Iter1(ia + sa));
    auto er = make_subrange(Iter1(ia), Sent1(ia));
    CHECK(find_end(er, make_subrange(Iter2(b), Sent2(b + 1)), equal_to(), &S::i_).begin() == Iter1(ia));
}

int main()
{
    test<ForwardIterator<const int*>, ForwardIterator<const int*> >();
    // test<ForwardIterator<const int*>, BidirectionalIterator<const int*> >();
    // test<ForwardIterator<const int*>, RandomAccessIterator<const int*> >();
    // test<BidirectionalIterator<const int*>, ForwardIterator<const int*> >();
    // test<BidirectionalIterator<const int*>, BidirectionalIterator<const int*> >();
    // test<BidirectionalIterator<const int*>, RandomAccessIterator<const int*> >();
    // test<RandomAccessIterator<const int*>, ForwardIterator<const int*> >();
    // test<RandomAccessIterator<const int*>, BidirectionalIterator<const int*> >();
    // test<RandomAccessIterator<const int*>, RandomAccessIterator<const int*> >();

    // test<ForwardIterator<const int*>, ForwardIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();
    // test<ForwardIterator<const int*>, BidirectionalIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();
    // test<ForwardIterator<const int*>, RandomAccessIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();
    // test<BidirectionalIterator<const int*>, ForwardIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();
    // test<BidirectionalIterator<const int*>, BidirectionalIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();
    // test<BidirectionalIterator<const int*>, RandomAccessIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();
    // test<RandomAccessIterator<const int*>, ForwardIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();
    // test<RandomAccessIterator<const int*>, BidirectionalIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();
    // test<RandomAccessIterator<const int*>, RandomAccessIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();

    // test_pred<ForwardIterator<const int*>, ForwardIterator<const int*> >();
    // test_pred<ForwardIterator<const int*>, BidirectionalIterator<const int*> >();
    // test_pred<ForwardIterator<const int*>, RandomAccessIterator<const int*> >();
    // test_pred<BidirectionalIterator<const int*>, ForwardIterator<const int*> >();
    // test_pred<BidirectionalIterator<const int*>, BidirectionalIterator<const int*> >();
    // test_pred<BidirectionalIterator<const int*>, RandomAccessIterator<const int*> >();
    // test_pred<RandomAccessIterator<const int*>, ForwardIterator<const int*> >();
    // test_pred<RandomAccessIterator<const int*>, BidirectionalIterator<const int*> >();
    // test_pred<RandomAccessIterator<const int*>, RandomAccessIterator<const int*> >();

    // test_pred<ForwardIterator<const int*>, ForwardIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();
    // test_pred<ForwardIterator<const int*>, BidirectionalIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();
    // test_pred<ForwardIterator<const int*>, RandomAccessIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();
    // test_pred<BidirectionalIterator<const int*>, ForwardIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();
    // test_pred<BidirectionalIterator<const int*>, BidirectionalIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();
    // test_pred<BidirectionalIterator<const int*>, RandomAccessIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();
    // test_pred<RandomAccessIterator<const int*>, ForwardIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();
    // test_pred<RandomAccessIterator<const int*>, BidirectionalIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();
    // test_pred<RandomAccessIterator<const int*>, RandomAccessIterator<const int*>, Sentinel<const int*>, Sentinel<const int *> >();

    // test_proj<ForwardIterator<const S*>, ForwardIterator<const int*> >();
    // test_proj<ForwardIterator<const S*>, BidirectionalIterator<const int*> >();
    // test_proj<ForwardIterator<const S*>, RandomAccessIterator<const int*> >();
    // test_proj<BidirectionalIterator<const S*>, ForwardIterator<const int*> >();
    // test_proj<BidirectionalIterator<const S*>, BidirectionalIterator<const int*> >();
    // test_proj<BidirectionalIterator<const S*>, RandomAccessIterator<const int*> >();
    // test_proj<RandomAccessIterator<const S*>, ForwardIterator<const int*> >();
    // test_proj<RandomAccessIterator<const S*>, BidirectionalIterator<const int*> >();
    // test_proj<RandomAccessIterator<const S*>, RandomAccessIterator<const int*> >();

    // test_proj<ForwardIterator<const S*>, ForwardIterator<const int*>, Sentinel<const S*>, Sentinel<const int *> >();
    // test_proj<ForwardIterator<const S*>, BidirectionalIterator<const int*>, Sentinel<const S*>, Sentinel<const int *> >();
    // test_proj<ForwardIterator<const S*>, RandomAccessIterator<const int*>, Sentinel<const S*>, Sentinel<const int *> >();
    // test_proj<BidirectionalIterator<const S*>, ForwardIterator<const int*>, Sentinel<const S*>, Sentinel<const int *> >();
    // test_proj<BidirectionalIterator<const S*>, BidirectionalIterator<const int*>, Sentinel<const S*>, Sentinel<const int *> >();
    // test_proj<BidirectionalIterator<const S*>, RandomAccessIterator<const int*>, Sentinel<const S*>, Sentinel<const int *> >();
    // test_proj<RandomAccessIterator<const S*>, ForwardIterator<const int*>, Sentinel<const S*>, Sentinel<const int *> >();
    // test_proj<RandomAccessIterator<const S*>, BidirectionalIterator<const int*>, Sentinel<const S*>, Sentinel<const int *> >();
    // test_proj<RandomAccessIterator<const S*>, RandomAccessIterator<const int*>, Sentinel<const S*>, Sentinel<const int *> >();

    {
        STATIC_CHECK(test_constexpr());
    }

    return ::test_result();
}
