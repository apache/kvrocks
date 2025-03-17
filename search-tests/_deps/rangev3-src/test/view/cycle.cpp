// Range v3 library
//
//  Copyright Eric Niebler 2013-present
//  Copyright Gonzalo Brito Gadeschi 2015
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <list>
#include <array>
#include <vector>
#include <memory>
#include <forward_list>
#include <range/v3/range_for.hpp>
#include <range/v3/algorithm/count_if.hpp>
#include <range/v3/view/cycle.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/view/take_exactly.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/view/slice.hpp>
#include <range/v3/view/c_str.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

using namespace ranges;

template<typename Rng>
void test_const_forward_range(Rng const &rng)
{
    auto r = rng | views::cycle;
    static_assert(is_infinite<decltype(r)>{}, "");
    static_assert(!common_range<decltype(r)>, "");
    CHECK(distance(r | views::take_exactly(0)) == 0);
    CHECK(distance(r | views::take_exactly(1)) == 1);
    CHECK(distance(r | views::take_exactly(2)) == 2);
    CHECK(distance(r | views::take_exactly(3)) == 3);
    CHECK(distance(r | views::take_exactly(4)) == 4);
    CHECK(distance(r | views::take_exactly(6)) == 6);
    CHECK(distance(r | views::take_exactly(7)) == 7);
    CHECK(count_if(r | views::take_exactly(7), [](int) { return true; }) == 7);

    ::check_equal(r | views::take_exactly(0), std::array<int, 0>{});
    ::check_equal(r | views::take_exactly(1), {0});
    ::check_equal(r | views::take_exactly(2), {0, 1});
    ::check_equal(r | views::take_exactly(3), {0, 1, 2});
    ::check_equal(r | views::take_exactly(4), {0, 1, 2, 0});
    ::check_equal(r | views::take_exactly(6), {0, 1, 2, 0, 1, 2});
    ::check_equal(r | views::take_exactly(7), {0, 1, 2, 0, 1, 2, 0});

    CHECK(distance(r | views::take(0)) == 0);
    CHECK(distance(r | views::take(1)) == 1);
    CHECK(distance(r | views::take(2)) == 2);
    CHECK(distance(r | views::take(3)) == 3);
    CHECK(distance(r | views::take(4)) == 4);
    CHECK(distance(r | views::take(6)) == 6);
    CHECK(distance(r | views::take(7)) == 7);
    CHECK(count_if(r | views::take(7), [](int) { return true; }) == 7);

    ::check_equal(r | views::take(0), std::array<int, 0>{});
    ::check_equal(r | views::take(1), {0});
    ::check_equal(r | views::take(2), {0, 1});
    ::check_equal(r | views::take(3), {0, 1, 2});
    ::check_equal(r | views::take(4), {0, 1, 2, 0});
    ::check_equal(r | views::take(6), {0, 1, 2, 0, 1, 2});
    ::check_equal(r | views::take(7), {0, 1, 2, 0, 1, 2, 0});
}

template<typename Rng>
void test_const_forward_reversed_range(Rng const &rng)
{
    test_const_forward_range(rng);

    auto r = rng | views::reverse | views::cycle;
    static_assert(is_infinite<decltype(r)>{}, "");
    static_assert(!common_range<decltype(r)>, "");

    CHECK(distance(r | views::take_exactly(0)) == 0);
    CHECK(distance(r | views::take_exactly(1)) == 1);
    CHECK(distance(r | views::take_exactly(2)) == 2);
    CHECK(distance(r | views::take_exactly(3)) == 3);
    CHECK(distance(r | views::take_exactly(4)) == 4);
    CHECK(distance(r | views::take_exactly(6)) == 6);
    CHECK(distance(r | views::take_exactly(7)) == 7);
    CHECK(count_if(r | views::take_exactly(7), [](int) { return true; }) == 7);

    ::check_equal(r | views::take_exactly(0), std::array<int, 0>{});
    ::check_equal(r | views::take_exactly(1), {2});
    ::check_equal(r | views::take_exactly(2), {2, 1});
    ::check_equal(r | views::take_exactly(3), {2, 1, 0});
    ::check_equal(r | views::take_exactly(4), {2, 1, 0, 2});
    ::check_equal(r | views::take_exactly(6), {2, 1, 0, 2, 1, 0});
    ::check_equal(r | views::take_exactly(7), {2, 1, 0, 2, 1, 0, 2});

    CHECK(distance(r | views::take(0)) == 0);
    CHECK(distance(r | views::take(1)) == 1);
    CHECK(distance(r | views::take(2)) == 2);
    CHECK(distance(r | views::take(3)) == 3);
    CHECK(distance(r | views::take(4)) == 4);
    CHECK(distance(r | views::take(6)) == 6);
    CHECK(distance(r | views::take(7)) == 7);
    CHECK(count_if(r | views::take(7), [](int) { return true; }) == 7);

    ::check_equal(r | views::take(0), std::array<int, 0>{});
    ::check_equal(r | views::take(1), {2});
    ::check_equal(r | views::take(2), {2, 1});
    ::check_equal(r | views::take(3), {2, 1, 0});
    ::check_equal(r | views::take(4), {2, 1, 0, 2});
    ::check_equal(r | views::take(6), {2, 1, 0, 2, 1, 0});
    ::check_equal(r | views::take(7), {2, 1, 0, 2, 1, 0, 2});
}

template<typename Rng>
void test_mutable_forward_range_reversed(Rng &rng)
{
    test_const_forward_reversed_range(rng);
    int count = 2;
    RANGES_FOR(auto &&i, rng | views::cycle | views::take_exactly(6)) { i = ++count; }
    ::check_equal(rng | views::take_exactly(3), {6, 7, 8});
}

template<typename Rng>
void test_forward_it(Rng const &rng)
{
    auto r = rng | views::cycle;
    static_assert(forward_range<decltype(r)>, "");
    auto f = begin(r);
    static_assert(forward_iterator<decltype(f)>, "");

    CHECK((*f) == 0);
    auto n = next(f, 1);
    CHECK((*n) == 1);
}

template<typename Rng>
void test_bidirectional_it(Rng const &rng)
{
    test_forward_it(rng);
    auto r = rng | views::cycle;
    static_assert(bidirectional_range<decltype(r)>, "");
    auto f = begin(r);
    static_assert(bidirectional_iterator<decltype(f)>, "");

    CHECK((*f) == 0);
    auto n = next(f, 1);
    CHECK((*n) == 1);
    CHECK((--n) == f);
}

template<typename Rng>
void test_random_access_it(Rng const &rng)
{
    test_bidirectional_it(rng);
    auto r = rng | views::cycle;
    static_assert(random_access_range<decltype(r)>, "");
    auto f = begin(r);
    static_assert(random_access_iterator<decltype(f)>, "");
    auto m = begin(r) + 1;
    auto l = begin(r) + 2;
    auto f1 = begin(r) + 3;
    auto f2 = begin(r) + 6;

    CHECK(r[0] == 0);
    CHECK(r[1] == 1);
    CHECK(r[2] == 2);
    CHECK(r[3] == 0);
    CHECK(r[4] == 1);
    CHECK(r[5] == 2);

    CHECK((f + 3) == f1);
    CHECK((f + 6) == f2);
    CHECK((f1 + 3) == f2);
    CHECK((f2 - 3) == f1);
    CHECK((f2 - 6) == f);

    auto e = end(r);

    CHECK(*f == 0);
    CHECK(f[0] == 0);
    CHECK(f[1] == 1);
    CHECK(f[2] == 2);
    CHECK(f[3] == 0);
    CHECK(f[4] == 1);
    CHECK(f[5] == 2);

    CHECK(*m == 1);
    CHECK(m[0] == 1);
    CHECK(m[1] == 2);
    CHECK(m[2] == 0);
    CHECK(m[3] == 1);
    CHECK(m[4] == 2);
    CHECK(m[5] == 0);

    CHECK(m[-1] == 0);

    CHECK(*l == 2);
    CHECK(l[0] == 2);
    CHECK(l[1] == 0);
    CHECK(l[2] == 1);
    CHECK(l[3] == 2);
    CHECK(l[4] == 0);
    CHECK(l[5] == 1);

    CHECK(l[-1] == 1);
    CHECK(l[-2] == 0);

    CHECK(f != e);

    auto cur = f;
    for (int i = 0; i < 100; ++i, ++cur)
    {
        CHECK((next(begin(r), i) - f) == i);
        CHECK((cur - f) == i);
        if(i > 0)
            CHECK((cur - m) == i - 1);
        if(i > 1)
            CHECK((cur - l) == i - 2);
    }
}

int main()
{
    // initializer list
    {
        auto il = {0, 1, 2};
        test_random_access_it(il);
        test_const_forward_reversed_range(il);

        const auto cil = {0, 1, 2};
        test_random_access_it(cil);
        test_const_forward_reversed_range(cil);
    }

    // array
    {
        std::array<int, 3> a = {{0, 1, 2}};
        test_random_access_it(a);
        test_mutable_forward_range_reversed(a);

        const std::array<int, 3> ca = {{0, 1, 2}};
        test_random_access_it(ca);
        test_const_forward_reversed_range(ca);
    }

    // list
    {
        std::list<int> l = {0, 1, 2};
        test_bidirectional_it(l);
        test_mutable_forward_range_reversed(l);

        const std::list<int> cl = {0, 1, 2};
        test_bidirectional_it(cl);
        test_const_forward_reversed_range(cl);
    }

    // forward list
    {
        std::forward_list<int> l = {0, 1, 2};
        test_forward_it(l);
        test_const_forward_range(l);

        const std::forward_list<int> cl = {0, 1, 2};
        test_forward_it(cl);
        test_const_forward_range(cl);
    }

    // move-only types
    {
        std::array<std::unique_ptr<int>, 3> a = {{
            std::unique_ptr<int>(new int(0)),
            std::unique_ptr<int>(new int(1)),
            std::unique_ptr<int>(new int(2))
        }};
        auto r = a | views::cycle;
        auto b = iter_move(r.begin() + 4);
        CHECK((*(b)) == 1);
    }

    // infinite
    {
        int count = 0;
        auto il = {0, 1, 2};
        auto v = 10;
        RANGES_FOR(auto&& i, il | views::cycle)
        {
            if (count == 42) { break; }
            v = i;
            ++count;
        }
        CHECK(count == 42);
        CHECK(v == 2);
    }

    // non-bounded
    {
        auto sz = views::c_str((char const *)"hi! ");
        ::check_equal(
            sz | views::cycle | views::take(10),
            {'h','i','!',' ','h','i','!',' ','h','i'} );

        auto rng = sz | views::cycle;
        auto it = ranges::begin(rng);
        CHECK(*it == 'h');
        CHECK(*++it == 'i');
        CHECK(*++it == '!');
        CHECK(*++it == ' ');
        CHECK(*++it == 'h');
        CHECK(*--it == ' ');
        CHECK(*--it == '!');
        CHECK(*--it == 'i');
        CHECK(*--it == 'h');

        rng = sz | views::cycle;
        it = ranges::begin(rng);
        it += 4;
        CHECK(*it == 'h');
    }

    // Cycle of an infinite range,
    // https://github.com/ericniebler/range-v3/issues/780
    {
        auto view = ranges::views::iota(0)
                    | ranges::views::cycle;
        CHECK(view[5] == 5);
    }

    // Composing ranges::views::cycle with ranges::views::slice
    // https://github.com/ericniebler/range-v3/issues/778
    {
        const auto length = 512;
        const auto k = 16;

        std::vector<int> input(length);

        auto output = ranges::views::cycle(input)
                    | ranges::views::slice(length + k, 2 * length + k);

        CHECK(bool(ranges::begin(output) != ranges::end(output)));
        CHECK(ranges::size(output) == 512u);
        CHECK(ranges::distance(output) == 512);
    }
    return test_result();
}
