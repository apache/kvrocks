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

#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/generate_n.hpp>
#include <range/v3/iterator/insert_iterators.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

struct gen_test
{
    int i_;
    constexpr gen_test()
      : i_{}
    {}
    constexpr gen_test(int i)
      : i_(i)
    {}
    constexpr int operator()()
    {
        return i_++;
    }
};

template<class Iter, class Sent = Iter>
void
test()
{
    const unsigned n = 4;
    int ia[n] = {0};
    ranges::generate_n_result<Iter, gen_test> res = ranges::generate_n(Iter(ia), n, gen_test(1));
    CHECK(ia[0] == 1);
    CHECK(ia[1] == 2);
    CHECK(ia[2] == 3);
    CHECK(ia[3] == 4);
    CHECK(res.out == Iter(ia + n));
    CHECK(res.fun.i_ == 5);
}

void test2()
{
    // Test ranges::generate with a genuine output range
    std::vector<int> v;
    ranges::generate_n(ranges::back_inserter(v), 5, gen_test(1));
    CHECK(v.size() == 5u);
    CHECK(v[0] == 1);
    CHECK(v[1] == 2);
    CHECK(v[2] == 3);
    CHECK(v[3] == 4);
    CHECK(v[4] == 5);
}

template<class Iter, class Sent = Iter>
constexpr bool test_constexpr()
{
    bool r = true;
    const unsigned n = 4;
    int ia[n] = {0};
    const auto res = ranges::generate_n(Iter(ia), n, gen_test(1));
    if(ia[0] != 1)
    {
        r = false;
    }
    if(ia[1] != 2)
    {
        r = false;
    }
    if(ia[2] != 3)
    {
        r = false;
    }
    if(ia[3] != 4)
    {
        r = false;
    }
    if(res.out != Iter(ia + n))
    {
        r = false;
    }
    if(res.fun.i_ != 5)
    {
        r = false;
    }
    return r;
}

int main()
{
    test<ForwardIterator<int*> >();
    test<BidirectionalIterator<int*> >();
    test<RandomAccessIterator<int*> >();
    test<int*>();

    test<ForwardIterator<int*>, Sentinel<int*> >();
    test<BidirectionalIterator<int*>, Sentinel<int*> >();
    test<RandomAccessIterator<int*>, Sentinel<int*> >();

    test2();

    {
        STATIC_CHECK(test_constexpr<ForwardIterator<int *>>());
        STATIC_CHECK(test_constexpr<BidirectionalIterator<int *>>());
        STATIC_CHECK(test_constexpr<RandomAccessIterator<int *>>());
        STATIC_CHECK(test_constexpr<int *>());

        STATIC_CHECK(test_constexpr<ForwardIterator<int *>, Sentinel<int *>>());
        STATIC_CHECK(test_constexpr<BidirectionalIterator<int *>, Sentinel<int *>>());
        STATIC_CHECK(test_constexpr<RandomAccessIterator<int *>, Sentinel<int *>>());
    }

    return ::test_result();
}
