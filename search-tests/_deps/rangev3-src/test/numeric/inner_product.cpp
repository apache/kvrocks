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
#include <range/v3/numeric/inner_product.hpp>
#include <range/v3/algorithm/equal.hpp>
#include "../simple_test.hpp"
#include "../test_iterators.hpp"

RANGES_DIAGNOSTIC_IGNORE_SIGN_CONVERSION

namespace
{
  struct S
  {
      int i;
  };

  template<class Iter1, class Iter2, class Sent1 = Iter1>
  void test()
  {
      int a[] = {1, 2, 3, 4, 5, 6};
      int b[] = {6, 5, 4, 3, 2, 1};
      unsigned sa = sizeof(a) / sizeof(a[0]);

      // iterator test:
      auto it3 = [](int* b1, int l1, int* b2, int i)
      {
        return ranges::inner_product(Iter1(b1), Sent1(b1+l1), Iter2(b2), i);
      };
      CHECK(it3(a, 0, b, 0) == 0);
      CHECK(it3(a, 0, b, 10) == 10);
      CHECK(it3(a, 1, b, 0) == 6);
      CHECK(it3(a, 1, b, 10) == 16);
      CHECK(it3(a, 2, b, 0) == 16);
      CHECK(it3(a, 2, b, 10) == 26);
      CHECK(it3(a, sa, b, 0) == 56);
      CHECK(it3(a, sa, b, 10) == 66);

      auto it4 = [](int* b1, int l1, int* b2, int i)
      {
        return ranges::inner_product(Iter1(b1), Sent1(b1+l1), Iter2(b2), Iter2(b2+l1), i);
      };
      CHECK(it4(a, 0, b, 0) == 0);
      CHECK(it4(a, 0, b, 10) == 10);
      CHECK(it4(a, 1, b, 0) == 6);
      CHECK(it4(a, 1, b, 10) == 16);
      CHECK(it4(a, 2, b, 0) == 16);
      CHECK(it4(a, 2, b, 10) == 26);
      CHECK(it4(a, sa, b, 0) == 56);
      CHECK(it4(a, sa, b, 10) == 66);

      // rng test:
      auto rng3 = [](int* b1, int l1, int* b2, int i)
      {
        return ranges::inner_product(ranges::make_subrange(Iter1(b1), Sent1(b1+l1)), Iter2(b2), i);
      };
      CHECK(rng3(a, 0, b, 0) == 0);
      CHECK(rng3(a, 0, b, 10)  == 10);
      CHECK(rng3(a, 1, b, 0) == 6);
      CHECK(rng3(a, 1, b, 10) == 16);
      CHECK(rng3(a, 2, b, 0) == 16);
      CHECK(rng3(a, 2, b, 10) == 26);
      CHECK(rng3(a, sa, b, 0) == 56);
      CHECK(rng3(a, sa, b, 10) == 66);

      auto rng4 = [](int* b1, int l1, int* b2, int i)
      {
        return ranges::inner_product(ranges::make_subrange(Iter1(b1), Sent1(b1+l1)),
                                    ranges::make_subrange(Iter2(b2), Iter2(b2+l1)), i);
      };
      CHECK(rng4(a, 0, b, 0) == 0);
      CHECK(rng4(a, 0, b, 10)  == 10);
      CHECK(rng4(a, 1, b, 0) == 6);
      CHECK(rng4(a, 1, b, 10) == 16);
      CHECK(rng4(a, 2, b, 0) == 16);
      CHECK(rng4(a, 2, b, 10) == 26);
      CHECK(rng4(a, sa, b, 0) == 56);
      CHECK(rng4(a, sa, b, 10) == 66);

      // rng + bops:
      auto bops = [](int* b1, int l1, int* b2, int i)
      {
        return ranges::inner_product(ranges::make_subrange(Iter1(b1), Sent1(b1+l1)),
                                    ranges::make_subrange(Iter2(b2), Iter2(b2+l1)), i,
                                    std::multiplies<int>(), std::plus<int>());
      };
      CHECK(bops(a, 0, b, 1) == 1);
      CHECK(bops(a, 0, b, 10) == 10);
      CHECK(bops(a, 1, b, 1) == 7);
      CHECK(bops(a, 1, b, 10) == 70);
      CHECK(bops(a, 2, b, 1) == 49);
      CHECK(bops(a, 2, b, 10) == 490);
      CHECK(bops(a, sa, b, 1) == 117649);
      CHECK(bops(a, sa, b, 10) == 1176490);
  }
}

int main()
{
    test<InputIterator<const int*>, InputIterator<const int*> >();
    test<InputIterator<const int*>, ForwardIterator<const int*> >();
    test<InputIterator<const int*>, BidirectionalIterator<const int*> >();
    test<InputIterator<const int*>, RandomAccessIterator<const int*> >();
    test<InputIterator<const int*>, const int*>();

    test<ForwardIterator<const int*>, InputIterator<const int*> >();
    test<ForwardIterator<const int*>, ForwardIterator<const int*> >();
    test<ForwardIterator<const int*>, BidirectionalIterator<const int*> >();
    test<ForwardIterator<const int*>, RandomAccessIterator<const int*> >();
    test<ForwardIterator<const int*>, const int*>();

    test<BidirectionalIterator<const int*>, InputIterator<const int*> >();
    test<BidirectionalIterator<const int*>, ForwardIterator<const int*> >();
    test<BidirectionalIterator<const int*>, BidirectionalIterator<const int*> >();
    test<BidirectionalIterator<const int*>, RandomAccessIterator<const int*> >();
    test<BidirectionalIterator<const int*>, const int*>();

    test<RandomAccessIterator<const int*>, InputIterator<const int*> >();
    test<RandomAccessIterator<const int*>, ForwardIterator<const int*> >();
    test<RandomAccessIterator<const int*>, BidirectionalIterator<const int*> >();
    test<RandomAccessIterator<const int*>, RandomAccessIterator<const int*> >();
    test<RandomAccessIterator<const int*>, const int*>();

    test<const int*, InputIterator<const int*> >();
    test<const int*, ForwardIterator<const int*> >();
    test<const int*, BidirectionalIterator<const int*> >();
    test<const int*, RandomAccessIterator<const int*> >();
    test<const int*, const int*>();

    // test projections:
    {
      S a[] = {{1}, {2}, {3}, {4}, {5}, {6}};
      S b[] = {{6}, {5}, {4}, {3}, {2}, {1}};
      unsigned sa = sizeof(a) / sizeof(a[0]);

      using Iter1 = InputIterator<const S*>;
      using Sent1 = InputIterator<const S*>;
      using Iter2 = Iter1;

      // rng + bops:
      auto bops = [&](S* b1, int l1, S* b2, int i)
      {
        return ranges::inner_product(ranges::make_subrange(Iter1(b1), Sent1(b1+l1)),
                                     ranges::make_subrange(Iter2(b2), Iter2(b2+l1)), i,
                                     std::multiplies<int>(), std::plus<int>(),
                                     &S::i, &S::i);
      };

      CHECK(bops(a, 0, b, 1) == 1);
      CHECK(bops(a, 0, b, 10) == 10);
      CHECK(bops(a, 1, b, 1) == 7);
      CHECK(bops(a, 1, b, 10) == 70);
      CHECK(bops(a, 2, b, 1) == 49);
      CHECK(bops(a, 2, b, 10) == 490);
      CHECK(bops(a, sa, b, 1) == 117649);
      CHECK(bops(a, sa, b, 10) == 1176490);
    }

    {
        int a[] = {1, 2, 3, 4, 5, 6};
        int b[] = {6, 5, 4, 3, 2, 1};

        // raw array test:
        CHECK(ranges::inner_product(a, b, 0) == 56);
        CHECK(ranges::inner_product(a, b, 10) == 66);
    }

    return ::test_result();
}
