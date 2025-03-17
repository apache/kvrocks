// Range v3 library
//
//  Copyright Gonzalo Brito Gadeschi 2015
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <range/v3/detail/config.hpp>

#if RANGES_CXX_CONSTEXPR >= RANGES_CXX_CONSTEXPR_14

#include <range/v3/range/access.hpp>
#include <range/v3/range/operations.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/utility/addressof.hpp>

#include "array.hpp"
#include "test_iterators.hpp"

// Test sequence 1,2,3,4
template<typename It>
constexpr /*c++14*/ auto test_it_back(It, It last,
    std::bidirectional_iterator_tag) -> bool
{
    auto end_m1_2 = It{ranges::prev(last, 1)};
    if (*end_m1_2 != 4) { return false; }
    return true;
}

template<typename It, typename Concept>
constexpr /*c++14*/ auto test_it_back(It, It, Concept) -> bool
{
    return true;
}

template<typename It>
constexpr /*c++14*/ auto test_it_(It beg, It last) -> bool
{
    if (*beg != 1) { return false; }
    if (ranges::distance(beg, last) != 4) { return false; }
    if (ranges::next(beg, 4) != last) { return false; }
    auto end_m1 = It{ranges::next(beg, 3)};
    if (*end_m1 != 4) { return false; }

    if (!test_it_back(beg, last, ranges::iterator_tag_of<It>{})) { return false; }
    auto end2 = beg;
    ranges::advance(end2, last);
    if (end2 != last) { return false; }
    auto end3 = beg;
    ranges::advance(end3, 4);
    if (end3 != last) { return false; }
    if (ranges::iter_enumerate(beg, last) != std::pair<std::ptrdiff_t, It>{4, last})
    {
        return false;
    }
    if (ranges::iter_distance(beg, last) != 4) { return false; }
    if (ranges::iter_distance_compare(beg, last, 4) != 0) { return false; }
    if (ranges::iter_distance_compare(beg, last, 3) != 1) { return false; }
    if (ranges::iter_distance_compare(beg, last, 5) != -1) { return false; }
    return true;
}

// Test sequence 4,3,2,1 (for reverse iterators)
template<typename It>
constexpr /*c++14*/ auto test_rit_(It beg, It last) -> bool
{
    if (ranges::distance(beg, last) != 4) { return false; }
    if (ranges::next(beg, 4) != last) { return false; }
    auto end_m1 = It{ranges::next(beg, 3)};
    if (*end_m1 != 1) { return false; }
    if (ranges::detail::is_convertible<ranges::iterator_tag_of<It>,
                                       std::bidirectional_iterator_tag>{})
    {
        auto end_m1_2 = It{ranges::prev(last, 1)};
        if (*end_m1_2 != 1) { return false; }
    }
    auto end2 = beg;
    ranges::advance(end2, last);
    if (end2 != last) { return false; }
    auto end3 = beg;
    ranges::advance(end3, 4);
    if (end3 != last) { return false; }
    using D = ranges::iter_difference_t<It>;
    if (ranges::iter_enumerate(beg, last) != std::pair<D, It>{4, last})
    {
        return false;
    }
    if (ranges::iter_distance(beg, last) != 4) { return false; }
    if (ranges::iter_distance_compare(beg, last, 4) != 0) { return false; }
    if (ranges::iter_distance_compare(beg, last, 3) != 1) { return false; }
    if (ranges::iter_distance_compare(beg, last, 5) != -1) { return false; }
    return true;
}

template<typename It, typename Sequence1234>
constexpr /*c++14*/ auto test_it(Sequence1234&& a) -> bool
{
    auto beg = It{ranges::begin(a)};
    auto last = It{ranges::end(a)};
    return test_it_(beg, last);
}

template<typename Sequence1234>
constexpr /*c++14*/ auto test_its_c(Sequence1234&& a) -> bool
{
    return     test_it<InputIterator<int const *>>(a)
            && test_it<ForwardIterator<int const *>>(a)
            && test_it<BidirectionalIterator<int const *>>(a)
            && test_it<RandomAccessIterator<int const *>>(a);

}

template<typename Sequence1234>
constexpr /*c++14*/ auto test_its(Sequence1234&& a) -> bool
{
    return     test_it<InputIterator<int *>>(a)
            && test_it<ForwardIterator<int *>>(a)
            && test_it<BidirectionalIterator<int *>>(a)
            && test_it<RandomAccessIterator<int *>>(a)
            && test_its_c(a);

}

template<typename It, typename Sequence1234>
constexpr /*c++14*/ auto test_rit(Sequence1234&& a) -> bool
{
    auto beg = It{ranges::rbegin(a)};
    auto last = It{ranges::rend(a)};
    return test_rit_(beg, last);
}

template<typename Sequence1234>
constexpr /*c++14*/ auto test_rits(Sequence1234&& a) -> bool
{
    using rit = decltype(ranges::rbegin(a));
    return     test_rit<BidirectionalIterator<rit>>(a)
            && test_rit<BidirectionalIterator<rit>>(a);
}

template<typename It, typename Sequence1234>
constexpr /*c++14*/ auto test_cit(Sequence1234&& a) -> bool
{
    auto beg = It{ranges::cbegin(a)};
    auto last = It{ranges::cend(a)};
    return test_it_(beg, last);
}

template<typename Sequence1234>
constexpr /*c++14*/ auto test_cits(Sequence1234&& a) -> bool
{
    return     test_cit<InputIterator<int const *>>(a)
            && test_cit<ForwardIterator<int const *>>(a)
            && test_cit<BidirectionalIterator<int const *>>(a)
            && test_cit<RandomAccessIterator<int const *>>(a);
}


template<typename It, typename Sequence1234>
constexpr /*c++14*/ auto test_crit(Sequence1234&& a) -> bool
{
    auto beg = It{ranges::crbegin(a)};
    auto last = It{ranges::crend(a)};
    return test_rit_(beg, last);
}

template<typename Sequence1234>
constexpr /*c++14*/ auto test_crits(Sequence1234&& a) -> bool
{
    using rit = decltype(ranges::crbegin(a));
    return     test_crit<BidirectionalIterator<rit>>(a)
            && test_crit<RandomAccessIterator<rit>>(a);
}

template<typename Sequence1234>
constexpr /*c++14*/ auto test_non_member_f(Sequence1234&& a) -> bool
{
    if (ranges::empty(a)) { return false; }
    if (ranges::front(a) != 1) { return false; }
    if (ranges::back(a) != 4) { return false; }
    if (ranges::index(a, 2) != 3) { return false; }
    if (ranges::at(a, 2) != 3) { return false; }
    if (ranges::size(a) != 4) { return false; }
    return true;
}

constexpr /*c++14*/ auto test_array() -> bool
{
    test::array<int, 4> a{{1, 2, 3, 4}};

    auto beg = ranges::begin(a);
    auto three = ranges::next(beg, 2);

    if ((false)) {
      ranges::iter_swap(beg, three);
      if (*beg != 3) { return false; }
      if (*three != 1) { return false; }
      ranges::iter_swap(beg, three);
    }

    if (!test_its(a)) { return false; }
    if (!test_cits(a)) { return false; }
    if (!test_rits(a)) { return false; }
    if (!test_crits(a)) { return false; }
    if (!test_non_member_f(a)) { return false; }

    // This can be worked around but is just bad:
    test::array<int, 4> b{{5, 6, 7, 8}};
    ranges::swap(a, b);
    if (a[0] != 5 || b[0] != 1 || a[3] != 8 || b[3] != 4) { return false; }

    return true;
}

constexpr /*c++14*/ auto test_c_array() -> bool
{
    int a[4]{1, 2, 3, 4};
    if (!test_its(a)) { return false; }
    if (!test_cits(a)) { return false; }
    if (!test_rits(a)) { return false; }
    if (!test_crits(a)) { return false; }
    if (!test_non_member_f(a)) { return false; }

    // C-arrays have no associated namespace, so this can't work:
    // int b[4]{5, 6, 7, 8};
    // ranges::swap(a, b);
    // if (a[0] != 5 || b[0] != 1 || a[3] != 8 || b[3] != 4) { return false; }

    return true;
}

constexpr /*c++14*/ auto test_init_list() -> bool
{
    std::initializer_list<int> a{1, 2, 3, 4};
    if (!test_its_c(a)) { return false; }
    if (!test_cits(a)) { return false; }
    if (!test_rits(a)) { return false; }
    if (!test_crits(a)) { return false; }
    if (!test_non_member_f(a)) { return false; }

    std::initializer_list<int> b{5, 6, 7, 8};
    ranges::swap(a, b);
    if (ranges::at(a, 0) != 5 || ranges::at(b, 0) != 1
        || ranges::at(a, 3) != 8 || ranges::at(b, 3) != 4)
    {
        return false;
    }

    return true;
}

#ifdef __cpp_lib_addressof_constexpr
#define ADDR_CONSTEXPR constexpr
#else
#define ADDR_CONSTEXPR
#endif

namespace addr {
    struct Good { };
    struct Bad { void operator&() const; };
    struct Bad2 { friend void operator&(Bad2); };
}

void test_constexpr_addressof() {
    static constexpr int i = 0;
    static constexpr int const* pi = ranges::detail::addressof(i);
    static_assert(&i == pi, "");

    static constexpr addr::Good g = {};
    static constexpr addr::Good const* pg = ranges::detail::addressof(g);
    static_assert(&g == pg, "");

    static constexpr addr::Bad b = {};
    static ADDR_CONSTEXPR addr::Bad const* pb = ranges::detail::addressof(b);

    static constexpr addr::Bad2 b2 = {};
    static ADDR_CONSTEXPR addr::Bad2 const* pb2 = ranges::detail::addressof(b2);

#ifdef __cpp_lib_addressof_constexpr
    static_assert(std::addressof(b) == pb, "");
    static_assert(std::addressof(b2) == pb2, "");
#else
    (void)pb;
    (void)pb2;
#endif
}

int main()
{
    static_assert(test_array(), "");
    static_assert(test_c_array(), "");
    static_assert(test_init_list(), "");
}

#else
int main() {}
#endif
