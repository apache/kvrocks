// Range v3 library
//
//  Gonzalo Brito Gadeschi 2017
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
#include <cmath>
#include <range/v3/algorithm/equal.hpp>
#include <range/v3/functional/arithmetic.hpp>
#include <range/v3/view/linear_distribute.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/view/iota.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using ranges::views::linear_distribute;

    auto float_eq = [](double a, double b) {
        return (int)std::floor(std::abs(a - b)) == 0;
    };

    {
        auto irng = linear_distribute(0, 1, 2);
        CHECK(ranges::size(irng) == std::size_t{2});
        CPP_assert(ranges::forward_range<decltype(irng)>);
        CPP_assert(ranges::sized_range<decltype(irng)>);
        auto il = {0, 1};
        check_equal(irng, il);
    }
    {
        auto irng = linear_distribute(1, 3, 3);
        auto il = {1, 2, 3};
        check_equal(irng, il);
        CHECK(ranges::size(irng) == std::size_t{3});
    }
    {
        auto irng = linear_distribute(0, 21, 22);
        auto frng = linear_distribute(0., 21., 22);
        check_equal(irng, frng | ranges::views::transform(ranges::convert_to<int>{}));
    }
    {
        auto frng = linear_distribute(0.0, 1.0, 11);
        CPP_assert(ranges::forward_range<decltype(frng)>);
        CPP_assert(ranges::sized_range<decltype(frng)>);
        CHECK(ranges::size(frng) == std::size_t{11});
        auto il = {0., 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};
        CHECK(ranges::size(il) == std::size_t{11});
        CHECK(ranges::equal(frng, il, float_eq));
    }
    {
        using ILD = std::initializer_list<double>;
        auto frng = linear_distribute(1.0, 3.0, 21);
        CHECK(ranges::equal(frng,
                            ILD{1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9,
                                2.0, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9,
                                3.0},
                            float_eq));
    }
    {   // empty interval
        auto irng = linear_distribute(0, 0, 1);
        CHECK(ranges::size(irng) == std::size_t{1});
        check_equal(irng, {0});

        auto irng1 = linear_distribute(0, 0, 10);
        CHECK(ranges::size(irng1) == std::size_t{10});
        check_equal(irng1, {0,0,0,0,0,0,0,0,0,0});

        auto irng2 = linear_distribute(1, 1, 3);
        CHECK(ranges::size(irng2) == std::size_t{3});
        check_equal(irng2, {1,1,1});

        auto frng = linear_distribute(0., 0., 3);
        CHECK(ranges::size(frng) == std::size_t{3});
        CHECK(ranges::equal(frng, std::initializer_list<double>{0.,0.,0.}, float_eq));
    }
    {   // regression test for #1088
        auto ld = linear_distribute(1, 10, 10);
        auto const first = ranges::begin(ld);
        auto const i = ranges::next(first, 4);
        CHECK(ranges::distance(first, i) == 4);
    }
    {   // integral numbers spacing
        auto irng = linear_distribute(0, 10, 22);
        auto frng = linear_distribute(0., 10., 22);
        check_equal(irng, frng | ranges::views::transform(ranges::convert_to<int>{}));
    }

    return test_result();
}
