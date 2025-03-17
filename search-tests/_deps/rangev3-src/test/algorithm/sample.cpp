// Range v3 lbrary
//
//  Copyright Eric Niebler 2014-present
//  Copyright Casey Carter 2016
//
//  Use, modification and distrbution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
//  Copyright 2005 - 2007 Adobe Systems Incorporated
//  Distrbuted under the MIT License(see accompanying file LICENSE_1_0_0.txt
//  or a copy at http://stlab.adobe.com/licenses.html)

//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#include <array>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/equal.hpp>
#include <range/v3/algorithm/sample.hpp>
#include <range/v3/numeric/iota.hpp>
#include <range/v3/iterator/move_iterators.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

namespace
{
    template<typename I, typename S>
    auto in_sequence(I first, I mid, S last) ->
        CPP_ret(bool)(
            requires ranges::sentinel_for<S, I>)
    {
        for (; first != mid; ++first)
            RANGES_ENSURE(first != last);
        for (; first != last; ++first)
            ;
        return true;
    }
}

int main()
{
    constexpr unsigned N = 100;
    constexpr unsigned K = 10;
    {
        std::array<int, N> i;
        ranges::iota(i, 0);
        std::array<int, K> a{}, b{}, c{};
        std::minstd_rand g1, g2 = g1;

        {
            auto result = ranges::sample(RandomAccessIterator<int*>(i.data()),
                Sentinel<int*>(i.data()+N), a.begin(), K, g1);
            CHECK(in_sequence(i.data(), result.in.base(), i.data() + N));
            CHECK(result.out == a.end());
            CHECK(!ranges::equal(a, c));
        }

        {
            auto result = ranges::sample(i.begin(), i.end(), b.begin(), K, g1);
            CHECK(in_sequence(i.begin(), result.in, i.end()));
            CHECK(result.out == b.end());
            CHECK(!ranges::equal(a, b));
            CHECK(!ranges::equal(b, c));
        }

        {
            auto result = ranges::sample(i.begin(), i.end(), c.begin(), K, g2);
            CHECK(in_sequence(i.begin(), result.in, i.end()));
            CHECK(result.out == c.end());
            CHECK(ranges::equal(a, c));
        }
    }

    {
        std::array<int, N> i;
        ranges::iota(i, 0);
        std::array<int, K> a{}, b{}, c{};
        std::minstd_rand g1, g2 = g1;
        auto rng = ranges::make_subrange(RandomAccessIterator<int*>(i.data()), Sentinel<int*>(i.data() + N));

        {
            auto result = ranges::sample(rng, a.begin(), K, g1);
            CHECK(in_sequence(ranges::begin(rng), result.in, ranges::end(rng)));
            CHECK(result.out == a.end());
            CHECK(!ranges::equal(a, b));
        }

        {
            auto result = ranges::sample(i, b.begin(), K, g2);
            CHECK(in_sequence(i.begin(), result.in, i.end()));
            CHECK(result.out == b.end());
            CHECK(ranges::equal(a, b));
        }

        {
            auto result = ranges::sample(i, b.begin(), K, g1);
            CHECK(in_sequence(i.begin(), result.in, i.end()));
            CHECK(result.out == b.end());
            CHECK(!ranges::equal(a, b));
            CHECK(!ranges::equal(b, c));
        }

        {
            a.fill(0);
            auto result = ranges::sample(std::move(rng), a.begin(), K, g1);
            CHECK(in_sequence(ranges::begin(rng), result.in, ranges::end(rng)));
            CHECK(result.out == a.end());
            CHECK(!ranges::equal(a, c));
        }
    }

    {
        std::array<int, N> i;
        ranges::iota(i, 0);
        std::array<int, K> a{}, b{}, c{};

        {
            auto result = ranges::sample(RandomAccessIterator<int*>(i.data()),
                Sentinel<int*>(i.data() + N), a.begin(), K);
            CHECK(in_sequence(i.data(), result.in.base(), i.data() + N));
            CHECK(result.out == a.end());
            CHECK(!ranges::equal(a, b));
        }

        {
            auto result = ranges::sample(i, b.begin(), K);
            CHECK(in_sequence(i.begin(), result.in, i.end()));
            CHECK(result.out == b.end());
            CHECK(!ranges::equal(b, c));
            CHECK(!ranges::equal(a, b));
        }
    }

    {
        std::array<MoveOnlyString, 10> source;
        std::array<MoveOnlyString, 4> dest;
        auto result = ranges::sample(ranges::make_move_iterator(source.begin()),
            ranges::make_move_sentinel(source.end()),
            ForwardIterator<MoveOnlyString*>(dest.data()), dest.size());
        CHECK(in_sequence(ranges::make_move_iterator(source.begin()),
            result.in,
            ranges::make_move_sentinel(source.end())));
        CHECK(result.out == ForwardIterator<MoveOnlyString*>(dest.data() + dest.size()));
    }

    {
        std::array<int, N> i;
        ranges::iota(i, 0);
        std::array<int, K> a{}, b{}, c{};
        std::minstd_rand g1, g2 = g1;

        {
            auto result = ranges::sample(RandomAccessIterator<int*>(i.data()),
                Sentinel<int*>(i.data()+N), a, g1);
            CHECK(in_sequence(i.data(), result.in.base(), i.data() + N));
            CHECK(result.out == a.end());
            CHECK(!ranges::equal(a, c));
        }

        {
            auto result = ranges::sample(i.begin(), i.end(), b, g1);
            CHECK(in_sequence(i.begin(), result.in, i.end()));
            CHECK(result.out == b.end());
            CHECK(!ranges::equal(a, b));
            CHECK(!ranges::equal(b, c));
        }

        {
            auto result = ranges::sample(i.begin(), i.end(), c, g2);
            CHECK(in_sequence(i.begin(), result.in, i.end()));
            CHECK(result.out == c.end());
            CHECK(ranges::equal(a, c));
        }
    }

    {
        std::array<int, N> i;
        ranges::iota(i, 0);
        std::array<int, K> a{}, b{}, c{};
        std::minstd_rand g1, g2 = g1;
        auto rng = ranges::make_subrange(RandomAccessIterator<int*>(i.data()), Sentinel<int*>(i.data() + N));

        {
            auto result = ranges::sample(rng, a, g1);
            CHECK(in_sequence(i.data(), result.in.base(), i.data() + N));
            CHECK(result.out == a.end());
            CHECK(!ranges::equal(a, b));
        }

        {
            auto result = ranges::sample(i, b, g2);
            CHECK(in_sequence(i.begin(), result.in, i.end()));
            CHECK(result.out == b.end());
            CHECK(ranges::equal(a, b));
        }

        {
            auto result = ranges::sample(i, b, g1);
            CHECK(in_sequence(i.begin(), result.in, i.end()));
            CHECK(result.out == b.end());
            CHECK(!ranges::equal(a, b));
            CHECK(!ranges::equal(b, c));
        }

        {
            a.fill(0);
            auto result = ranges::sample(std::move(rng), a, g1);
            CHECK(in_sequence(i.data(), result.in.base(), i.data() + N));
            CHECK(result.out == a.end());
            CHECK(!ranges::equal(a, c));
        }
    }

    {
        std::array<int, N> i;
        ranges::iota(i, 0);
        std::array<int, K> a{}, b{}, c{};

        {
            auto result = ranges::sample(RandomAccessIterator<int*>(i.data()),
                Sentinel<int*>(i.data() + N), a);
            CHECK(in_sequence(i.data(), result.in.base(), i.data() + N));
            CHECK(result.out == a.end());
            CHECK(!ranges::equal(a, b));
        }

        {
            auto result = ranges::sample(i, b);
            CHECK(in_sequence(i.begin(), result.in, i.end()));
            CHECK(result.out == b.end());
            CHECK(!ranges::equal(b, c));
            CHECK(!ranges::equal(a, b));
        }
    }

    {
        std::array<MoveOnlyString, 10> source;
        std::array<MoveOnlyString, 4> dest;
        auto out = ranges::make_subrange(
            ForwardIterator<MoveOnlyString*>(dest.data()),
            Sentinel<MoveOnlyString*, true>(dest.data() + dest.size()));
        auto result = ranges::sample(ranges::make_move_iterator(source.begin()),
            ranges::make_move_sentinel(source.end()), out);
        CHECK(in_sequence(source.begin(), result.in.base(), source.end()));
        CHECK(result.out == ranges::end(out));
    }

    {
        int data[] = {0,1,2,3};
        int sample[2];
        std::minstd_rand g;
        {
            auto result = ranges::sample(data, sample, g);
            CHECK(in_sequence(ranges::begin(data), result.in, ranges::end(data)));
            CHECK(result.out == ranges::end(sample));
        }
        {
            auto result = ranges::sample(data, sample);
            CHECK(in_sequence(ranges::begin(data), result.in, ranges::end(data)));
            CHECK(result.out == ranges::end(sample));
        }
        {
            auto result = ranges::sample(data + 0, data + 2, sample + 0, 9999);
            CHECK(result.in == data + 2);
            CHECK(result.out == sample + 2);
        }
    }

    return ::test_result();
}
