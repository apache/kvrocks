// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//  Copyright Gonzalo Brito Gadeschi 2014
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
// Implementation based on the code in libc++
//   http://http://libcxx.llvm.org/

//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#include <range/v3/core.hpp>
#include <range/v3/numeric/iota.hpp>
#include <range/v3/algorithm/equal.hpp>
#include "../simple_test.hpp"
#include "../test_iterators.hpp"

template<class Iter, class Sent = Iter>
void test()
{
    int ir[] = {5, 6, 7, 8, 9};
    constexpr auto s = ranges::size(ir);
    {
        int ia[] = {1, 2, 3, 4, 5};
        ranges::iota(Iter(ia), Sent(ia + s), 5);
        CHECK(ranges::equal(ia, ir));
    }

    {
        int ia[] = {1, 2, 3, 4, 5};
        auto rng = ranges::make_subrange(Iter(ia), Sent(ia + s));
        ranges::iota(rng, 5);
        CHECK(ranges::equal(ia, ir));
    }
}

int main()
{
    test<InputIterator<int*> >();
    test<ForwardIterator<int*> >();
    test<BidirectionalIterator<int*> >();
    test<RandomAccessIterator<int*> >();
    test<int*>();

    test<InputIterator<int*>, Sentinel<int*> >();
    test<ForwardIterator<int*>, Sentinel<int*> >();
    test<BidirectionalIterator<int*>, Sentinel<int*> >();
    test<RandomAccessIterator<int*>, Sentinel<int*> >();

    return ::test_result();
}
