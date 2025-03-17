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
#include <string>
#include <sstream>
#include <range/v3/core.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/istream.hpp>
#include <range/v3/view/slice.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/utility/copy.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;

    int rgi[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    auto rng0 = rgi | views::slice(3, 9);
    has_type<int &>(*begin(rng0));
    CPP_assert(view_<decltype(rng0)>);
    CPP_assert(common_range<decltype(rng0)>);
    CPP_assert(sized_range<decltype(rng0)>);
    CPP_assert(random_access_iterator<decltype(begin(rng0))>);
    ::check_equal(rng0, {3, 4, 5, 6, 7, 8});

    auto rng1 = rng0 | views::reverse;
    has_type<int &>(*begin(rng1));
    CPP_assert(view_<decltype(rng1)>);
    CPP_assert(common_range<decltype(rng1)>);
    CPP_assert(sized_range<decltype(rng1)>);
    CPP_assert(random_access_iterator<decltype(begin(rng1))>);
    ::check_equal(rng1, {8, 7, 6, 5, 4, 3});

    std::vector<int> v{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    auto rng2 = v | views::slice(3, 9) | views::reverse;
    has_type<int &>(*begin(rng2));
    CPP_assert(view_<decltype(rng2)>);
    CPP_assert(common_range<decltype(rng2)>);
    CPP_assert(sized_range<decltype(rng2)>);
    CPP_assert(random_access_iterator<decltype(begin(rng2))>);
    ::check_equal(rng2, {8, 7, 6, 5, 4, 3});

    std::list<int> l{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    auto rng3 = l | views::slice(3, 9);
    has_type<int &>(*begin(rng3));
    CPP_assert(view_<decltype(rng3)>);
    CPP_assert(!common_range<decltype(rng3)>);
    CPP_assert(sized_range<decltype(rng3)>);
    CPP_assert(bidirectional_iterator<decltype(begin(rng3))>);
    CPP_assert(!random_access_iterator<decltype(begin(rng3))>);
    ::check_equal(rng3, {3, 4, 5, 6, 7, 8});

    auto rng4 = views::iota(10) | views::slice(10, 20);
    CPP_assert(view_<decltype(rng4)>);
    CPP_assert(common_range<decltype(rng4)>);
    CPP_assert(sized_range<decltype(rng4)>);
    static_assert(!ranges::is_infinite<decltype(rng4)>::value, "");
    ::check_equal(rng4, {20, 21, 22, 23, 24, 25, 26, 27, 28, 29});

    auto rng5 = views::iota(10)[{10, 20}];
    CPP_assert(view_<decltype(rng5)>);
    CPP_assert(common_range<decltype(rng5)>);
    CPP_assert(sized_range<decltype(rng5)>);
    static_assert(!ranges::is_infinite<decltype(rng5)>::value, "");
    ::check_equal(rng5, {20, 21, 22, 23, 24, 25, 26, 27, 28, 29});

    auto rng6 = views::all(l)[{3, 9}];
    has_type<int &>(*begin(rng6));
    CPP_assert(view_<decltype(rng6)>);
    CPP_assert(!common_range<decltype(rng6)>);
    CPP_assert(sized_range<decltype(rng6)>);
    CPP_assert(bidirectional_iterator<decltype(begin(rng6))>);
    CPP_assert(!random_access_iterator<decltype(begin(rng6))>);
    ::check_equal(rng6, {3, 4, 5, 6, 7, 8});

    auto rng7 = views::all(l)[{3, end}];
    has_type<int &>(*begin(rng7));
    CPP_assert(view_<decltype(rng7)>);
    CPP_assert(common_range<decltype(rng7)>);
    CPP_assert(sized_range<decltype(rng7)>);
    CPP_assert(bidirectional_iterator<decltype(begin(rng7))>);
    CPP_assert(!random_access_iterator<decltype(begin(rng7))>);
    ::check_equal(rng7, {3, 4, 5, 6, 7, 8, 9, 10});

    auto rng8 = views::all(l)[{end-5,end-2}];
    has_type<int &>(*begin(rng8));
    CPP_assert(view_<decltype(rng8)>);
    CPP_assert(!common_range<decltype(rng8)>);
    CPP_assert(sized_range<decltype(rng8)>);
    CPP_assert(bidirectional_iterator<decltype(begin(rng8))>);
    CPP_assert(!random_access_iterator<decltype(begin(rng8))>);
    ::check_equal(rng8, {6, 7, 8});

    auto rng9 = views::iota(0)[{0,end}];
    static_assert(is_infinite<decltype(rng9)>::value, "should be infinite");

    {
        std::string str{"0 1 2 3 4 5 6 7 8 9"};
        std::stringstream sin{str};
        auto rng10 = istream<int>(sin)[{3,9}];
        ::check_equal(rng10, {3, 4, 5, 6, 7, 8});
    }

    {
        std::string str{"0 1 2 3 4 5 6 7 8 9"};
        std::stringstream sin{str};
        auto rng11 = istream<int>(sin)[{3,end}];
        ::check_equal(rng11, {3, 4, 5, 6, 7, 8, 9});
    }

    {
        auto letters = views::closed_iota('a','g');
        static_assert(random_access_range<decltype(letters)> && view_<decltype(letters)>, "");
        static_assert(common_range<decltype(letters)> && view_<decltype(letters)>, "");
        ::check_equal(letters[{2,end-2}], {'c','d','e'});
    }

    {
        int const some_ints[] = {0,1,2,3,4,5,6,7,8,9};
        auto rng = debug_input_view<int const>{some_ints} | views::slice(3,10);
        ::check_equal(rng, {3, 4, 5, 6, 7, 8, 9});
    }

    return test_result();
}
