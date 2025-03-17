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

#include <cmath>
#include <functional>

#include <range/v3/algorithm/fold.hpp>
#include <range/v3/algorithm/min.hpp>
#include <range/v3/core.hpp>

#include "../simple_test.hpp"
#include "../test_iterators.hpp"

struct Approx
{
    double value;
    Approx(double v)
      : value(v)
    {}

    friend bool operator==(Approx a, double d)
    {
        return std::fabs(a.value - d) < 0.001;
    }
    friend bool operator==(double d, Approx a)
    {
        return a == d;
    }
};

template<class Iter, class Sent = Iter>
void test_left()
{
    double da[] = {0.25, 0.75};
    CHECK(ranges::fold_left(Iter(da), Sent(da), 1, std::plus<>()) == Approx{1.0});
    CHECK(ranges::fold_left(Iter(da), Sent(da + 2), 1, std::plus<>()) == Approx{2.0});

    CHECK(ranges::fold_left_first(Iter(da), Sent(da), ranges::min) == ranges::nullopt);
    CHECK(ranges::fold_left_first(Iter(da), Sent(da + 2), ranges::min) ==
          ranges::optional<Approx>(0.25));

    using ranges::make_subrange;
    CHECK(ranges::fold_left(make_subrange(Iter(da), Sent(da)), 1, std::plus<>()) ==
          Approx{1.0});
    CHECK(ranges::fold_left(make_subrange(Iter(da), Sent(da + 2)), 1, std::plus<>()) ==
          Approx{2.0});
    CHECK(ranges::fold_left_first(make_subrange(Iter(da), Sent(da)), ranges::min) ==
          ranges::nullopt);
    CHECK(ranges::fold_left_first(make_subrange(Iter(da), Sent(da + 2)), ranges::min) ==
          ranges::optional<Approx>(0.25));
}

void test_right()
{
    double da[] = {0.25, 0.75};
    CHECK(ranges::fold_right(da, da + 2, 1, std::plus<>()) == Approx{2.0});
    CHECK(ranges::fold_right(da, 1, std::plus<>()) == Approx{2.0});

    // f(0.25, f(0.75, 1))
    CHECK(ranges::fold_right(da, da + 2, 1, std::minus<>()) == Approx{0.5});
    CHECK(ranges::fold_right(da, 1, std::minus<>()) == Approx{0.5});

    int xs[] = {1, 2, 3};
    auto concat = [](int i, std::string s) { return s + std::to_string(i); };
    CHECK(ranges::fold_right(xs, xs + 2, std::string(), concat) == "21");
    CHECK(ranges::fold_right(xs, std::string(), concat) == "321");
}

int main()
{
    test_left<InputIterator<const double *>>();
    test_left<ForwardIterator<const double *>>();
    test_left<BidirectionalIterator<const double *>>();
    test_left<RandomAccessIterator<const double *>>();
    test_left<const double *>();

    test_left<InputIterator<const double *>, Sentinel<const double *>>();
    test_left<ForwardIterator<const double *>, Sentinel<const double *>>();
    test_left<BidirectionalIterator<const double *>, Sentinel<const double *>>();
    test_left<RandomAccessIterator<const double *>, Sentinel<const double *>>();

    test_right();

    return ::test_result();
}
