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

#include <vector>
#include <iterator>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/copy.hpp>
#include <range/v3/algorithm/equal_range.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/insert_iterators.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/join.hpp>
#include <range/v3/view/repeat_n.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/view/transform.hpp>
#include "../array.hpp"
#include "../simple_test.hpp"
#include "../test_iterators.hpp"

struct my_int
{
    int value;
};

bool compare(my_int lhs, my_int rhs)
{
    return lhs.value < rhs.value;
}

void not_totally_ordered()
{
    // This better compile!
    std::vector<my_int> vec;
    ranges::equal_range(vec, my_int{10}, compare);
}

template<class Iter, class Sent, class T, class Proj = ranges::identity>
void
test_it(Iter first, Sent last, const T& value, Proj proj = Proj{})
{
    ranges::subrange<Iter, Iter> i =
        ranges::equal_range(first, last, value, ranges::less{}, proj);
    for (Iter j = first; j != i.begin(); ++j)
        CHECK(ranges::invoke(proj, *j) < value);
    for (Iter j = i.begin(); j != last; ++j)
        CHECK(!(ranges::invoke(proj, *j) < value));
    for (Iter j = first; j != i.end(); ++j)
        CHECK(!(value < ranges::invoke(proj, *j)));
    for (Iter j = i.end(); j != last; ++j)
        CHECK(value < ranges::invoke(proj, *j));

    auto res = ranges::equal_range(
        ranges::make_subrange(first, last), value, ranges::less{}, proj);
    for (Iter j = first; j != res.begin(); ++j)
        CHECK(ranges::invoke(proj, *j) < value);
    for (Iter j = res.begin(); j != last; ++j)
        CHECK(!(ranges::invoke(proj, *j) < value));
    for (Iter j = first; j != res.end(); ++j)
        CHECK(!(value < ranges::invoke(proj, *j)));
    for (Iter j = res.end(); j != last; ++j)
        CHECK(value < ranges::invoke(proj, *j));
}

template<class Iter, class Sent = Iter>
void
test_it()
{
    using namespace ranges::views;
    static constexpr unsigned M = 10;
    std::vector<int> v;
    auto input = ints | take(100) | transform([](int i){return repeat_n(i,M);}) | join;
    ranges::copy(input, ranges::back_inserter(v));
    for (int x = 0; x <= (int)M; ++x)
        test_it(Iter(v.data()), Sent(v.data()+v.size()), x);
}

template<class Iter, class Sent, class T>
constexpr bool test_constexpr(Iter first, Sent last, const T & value)
{
    bool result = true;
    const auto i = ranges::equal_range(first, last, value);
    for(Iter j = first; j != i.begin(); ++j)
    {
        STATIC_CHECK_RETURN(*j < value);
    }
    for(Iter j = i.begin(); j != last; ++j)
    {
        STATIC_CHECK_RETURN(!(*j < value));
    }
    for(Iter j = first; j != i.end(); ++j)
    {
        STATIC_CHECK_RETURN(!(value < *j));
    }
    for(Iter j = i.end(); j != last; ++j)
    {
        STATIC_CHECK_RETURN(value < *j);
    }

    const auto res = ranges::equal_range(ranges::make_subrange(first, last), value);
    for(Iter j = first; j != res.begin(); ++j)
    {
        STATIC_CHECK_RETURN(*j < value);
    }
    for(Iter j = res.begin(); j != last; ++j)
    {
        STATIC_CHECK_RETURN(!(*j < value));
    }
    for(Iter j = first; j != res.end(); ++j)
    {
        STATIC_CHECK_RETURN(!(value < *j));
    }
    for(Iter j = res.end(); j != last; ++j)
    {
        STATIC_CHECK_RETURN(value < *j);
    }

    return result;
}

template<class Iter, class Sent = Iter>
constexpr bool test_constexpr()
{
    constexpr unsigned M = 10;
    constexpr unsigned N = 10;
    test::array<int, N * M> v{{0}};
    for(unsigned i = 0; i < N; ++i)
    {
        for(unsigned j = 0; j < M; ++j)
        {
            v[i * M + j] = (int)i;
        }
    }
    for(int x = 0; x <= (int)M; ++x)
    {
        STATIC_CHECK_RETURN(test_constexpr(Iter(v.data()), Sent(v.data() + v.size()), x));
    }
    return true;
}

constexpr bool test_constexpr_some()
{
    constexpr int d[] = {0, 1, 2, 3};
    for(auto e = ranges::begin(d); e < ranges::end(d); ++e)
    {
        for(int x = -1; x <= 4; ++x)
        {
            STATIC_CHECK_RETURN(test_constexpr(d, e, x));
        }
    }
    return true;
}

int main()
{
    int d[] = {0, 1, 2, 3};
    for (int* e = d; e <= d+4; ++e)
        for (int x = -1; x <= 4; ++x)
            test_it(d, e, x);


    test_it<ForwardIterator<const int*> >();
    test_it<BidirectionalIterator<const int*> >();
    test_it<RandomAccessIterator<const int*> >();
    test_it<const int*>();

    test_it<ForwardIterator<const int*>, Sentinel<const int*> >();
    test_it<BidirectionalIterator<const int*>, Sentinel<const int*> >();
    test_it<RandomAccessIterator<const int*>, Sentinel<const int*> >();

    {
        struct foo { int i; };

        foo some_foos[] = {{1}, {2}, {4}};
        test_it(some_foos, some_foos + 3, 2, &foo::i);
    }

    {
        STATIC_CHECK(test_constexpr_some());
        STATIC_CHECK(test_constexpr<ForwardIterator<const int *>>());
        STATIC_CHECK(test_constexpr<BidirectionalIterator<const int *>>());
        STATIC_CHECK(test_constexpr<RandomAccessIterator<const int *>>());
        STATIC_CHECK(test_constexpr<const int *>());
        STATIC_CHECK(test_constexpr<ForwardIterator<const int *>, Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<BidirectionalIterator<const int *>, Sentinel<const int *>>());
        STATIC_CHECK(test_constexpr<RandomAccessIterator<const int *>, Sentinel<const int *>>());
    }

    return ::test_result();
}
