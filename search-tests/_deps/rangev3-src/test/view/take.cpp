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

#include <list>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/view/delimit.hpp>
#include <range/v3/utility/copy.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;

    int rgi[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    auto rng0 = rgi | views::take(6);
    has_type<int &>(*begin(rng0));
    CPP_assert(view_<decltype(rng0)>);
    CPP_assert(common_range<decltype(rng0)>);
    CPP_assert(sized_range<decltype(rng0)>);
    CPP_assert(random_access_iterator<decltype(begin(rng0))>);
    CPP_assert(range<decltype(detail::as_const(rng0))>);
    check_equal(rng0, {0, 1, 2, 3, 4, 5});
    CHECK(size(rng0) == 6u);

    auto rng0b = rgi | views::take(20);
    has_type<int &>(*begin(rng0b));
    CPP_assert(view_<decltype(rng0b)>);
    CPP_assert(common_range<decltype(rng0b)>);
    CPP_assert(sized_range<decltype(rng0b)>);
    CPP_assert(random_access_iterator<decltype(begin(rng0b))>);
    CPP_assert(range<decltype(detail::as_const(rng0b))>);
    check_equal(rng0b, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    CHECK(size(rng0b) == 11u);

    auto rng1 = rng0 | views::reverse;
    has_type<int &>(*begin(rng1));
    CPP_assert(view_<decltype(rng1)>);
    CPP_assert(common_range<decltype(rng1)>);
    CPP_assert(sized_range<decltype(rng1)>);
    CPP_assert(random_access_iterator<decltype(begin(rng1))>);
    CPP_assert(range<decltype(detail::as_const(rng1))>);
    check_equal(rng1, {5, 4, 3, 2, 1, 0});

    std::vector<int> v{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    auto rng2 = v | views::take(6) | views::reverse;
    has_type<int &>(*begin(rng2));
    CPP_assert(view_<decltype(rng2)>);
    CPP_assert(common_range<decltype(rng2)>);
    CPP_assert(sized_range<decltype(rng2)>);
    CPP_assert(random_access_iterator<decltype(begin(rng2))>);
    CPP_assert(range<decltype(detail::as_const(rng2))>);
    check_equal(rng2, {5, 4, 3, 2, 1, 0});

    std::list<int> l{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    auto rng3 = l | views::take(6);
    has_type<int &>(*begin(rng3));
    CPP_assert(view_<decltype(rng3)>);
    CPP_assert(!common_range<decltype(rng3)>);
    CPP_assert(sized_range<decltype(rng3)>);
    CPP_assert(bidirectional_iterator<decltype(begin(rng3))>);
    CPP_assert(!random_access_iterator<decltype(begin(rng3))>);
    CPP_assert(range<decltype(detail::as_const(rng3))>);
    ::check_equal(rng3, {0, 1, 2, 3, 4, 5});
    CHECK(size(rng3) == 6u);

    auto rng3b = l | views::take(20);
    has_type<int &>(*begin(rng3b));
    CPP_assert(view_<decltype(rng3b)>);
    CPP_assert(!common_range<decltype(rng3b)>);
    CPP_assert(sized_range<decltype(rng3b)>);
    CPP_assert(bidirectional_iterator<decltype(begin(rng3b))>);
    CPP_assert(!random_access_iterator<decltype(begin(rng3b))>);
    CPP_assert(range<decltype(detail::as_const(rng3b))>);
    check_equal(rng3b, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    CHECK(size(rng3b) == 11u);

    auto rng4 = views::iota(10) | views::take(10);
    CPP_assert(view_<decltype(rng4)>);
    CPP_assert(!common_range<decltype(rng4)>);
    CPP_assert(sized_range<decltype(rng4)>);
    CPP_assert(range<decltype(detail::as_const(rng4))>);
    static_assert(!ranges::is_infinite<decltype(rng4)>::value, "");
    check_equal(rng4, {10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
    CHECK(size(rng4) == 10u);

    auto rng5 = views::iota(10) | views::take(10) | views::reverse;
    CPP_assert(view_<decltype(rng5)>);
    CPP_assert(common_range<decltype(rng5)>);
    CPP_assert(sized_range<decltype(rng5)>);
    CPP_assert(!range<decltype(detail::as_const(rng5))>);
    static_assert(!ranges::is_infinite<decltype(rng5)>::value, "");
    check_equal(rng5, {19, 18, 17, 16, 15, 14, 13, 12, 11, 10});
    CHECK(size(rng5) == 10u);

    auto c_str = views::delimit("hello world", '\0');
    CPP_assert(random_access_range<decltype(c_str)>);
    CPP_assert(!sized_range<decltype(c_str)>);

    auto rng6 = c_str | views::take(5);
    CPP_assert(view_<decltype(rng6)>);
    CPP_assert(random_access_range<decltype(rng6)>);
    CPP_assert(!common_range<decltype(rng6)>);
    CPP_assert(!sized_range<decltype(rng6)>);
    CPP_assert(range<decltype(detail::as_const(rng6))>);
    check_equal(rng6, {'h','e','l','l','o'});

    auto rng7 = c_str | views::take(20);
    check_equal(rng7, {'h','e','l','l','o',' ','w','o','r','l','d'});

    subrange<std::list<int>::iterator> rl{l.begin(), l.end()};
    CPP_assert(view_<decltype(rl)>);
    CPP_assert(bidirectional_range<decltype(rl)>);
    CPP_assert(common_range<decltype(rl)>);
    CPP_assert(!sized_range<decltype(rl)>);
    CPP_assert(range<decltype(detail::as_const(rl))>);

    auto rng8 = rl | views::take(5);
    CPP_assert(view_<decltype(rng8)>);
    CPP_assert(bidirectional_range<decltype(rng8)>);
    CPP_assert(!common_range<decltype(rng8)>);
    CPP_assert(!sized_range<decltype(rng8)>);
    CPP_assert(range<decltype(detail::as_const(rng8))>);
    check_equal(rng8, {0, 1, 2, 3, 4});

    auto rng9 = rl | views::take(20);
    CPP_assert(view_<decltype(rng9)>);
    CPP_assert(bidirectional_range<decltype(rng9)>);
    CPP_assert(!common_range<decltype(rng9)>);
    CPP_assert(!sized_range<decltype(rng9)>);
    CPP_assert(range<decltype(detail::as_const(rng9))>);
    check_equal(rng9, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

    {
        auto rng = debug_input_view<int const>{rgi} | views::take(6);
        CPP_assert(!range<decltype(detail::as_const(rng))>);
        check_equal(rng, {0, 1, 2, 3, 4, 5});
    }

    return test_result();
}
