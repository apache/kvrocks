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
#include <sstream>
#include <range/v3/core.hpp>
#include <range/v3/view/common.hpp>
#include <range/v3/view/counted.hpp>
#include <range/v3/view/delimit.hpp>
#include <range/v3/view/repeat_n.hpp>
#include <range/v3/utility/copy.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;

    {
        std::stringstream sinx("1 2 3 4 5 6 7 8 9 1 2 3 4 5 6 7 8 9 1 2 3 4 42 6 7 8 9 ");
        auto rng1 = istream<int>(sinx) | views::delimit(42); // | views::common;
        CPP_assert(!common_range<decltype(rng1)>);
        CPP_assert(input_range<decltype(rng1)>);
        CPP_assert(!forward_range<decltype(rng1)>);
        auto const& crng1 = rng1;
        CPP_assert(!range<decltype(crng1)>);
        auto rng2 = rng1 | views::common;
        CPP_assert(view_<decltype(rng2)>);
        CPP_assert(common_range<decltype(rng2)>);
        CPP_assert(input_iterator<decltype(rng2.begin())>);
        CPP_assert(same_as<typename std::iterator_traits<decltype(rng2.begin())>::iterator_category,
                            std::input_iterator_tag>);
        CPP_assert(!forward_iterator<decltype(rng2.begin())>);
        ::check_equal(rng2, {1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4});
    }

    std::vector<int> v{1,2,3,4,5,6,7,8,9,0,42,64};
    {
        auto rng1 = v | views::delimit(42) | views::common;
        CPP_assert(view_<decltype(rng1)>);
        CPP_assert(common_range<decltype(rng1)>);
        CPP_assert(!sized_range<decltype(rng1)>);
        CPP_assert(forward_iterator<decltype(rng1.begin())>);
        CPP_assert(!bidirectional_iterator<decltype(rng1.begin())>);
        auto const & crng1 = rng1;
        auto i = rng1.begin(); // non-const
        auto j = crng1.begin(); // const
        j = i;
        ::check_equal(rng1, {1, 2, 3, 4, 5, 6, 7, 8, 9, 0});
    }

    {
        std::list<int> l{1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0};
        auto rng3 = views::counted(l.begin(), 10) | views::common;
        CPP_assert(view_<decltype(rng3)>);
        CPP_assert(common_range<decltype(rng3)>);
        CPP_assert(sized_range<decltype(rng3)>);
        CPP_assert(forward_iterator<decltype(rng3.begin())>);
        CPP_assert(!bidirectional_iterator<decltype(rng3.begin())>);
        CPP_assert(sized_sentinel_for<decltype(rng3.begin()), decltype(rng3.end())>);
        auto b = begin(rng3);
        auto e = end(rng3);
        CHECK((e-b) == 10);
        CHECK((b-e) == -10);
        CHECK((e-e) == 0);
        CHECK((next(b)-b) == 1);

        // Pass-through of already-common ranges is OK:
        rng3 = rng3 | views::common;
    }

    {
        auto rng4 = views::counted(begin(v), 8) | views::common;
        CPP_assert(view_<decltype(rng4)>);
        CPP_assert(common_range<decltype(rng4)>);
        CPP_assert(sized_range<decltype(rng4)>);
        CPP_assert(random_access_iterator<decltype(begin(rng4))>);
        ::check_equal(rng4, {1, 2, 3, 4, 5, 6, 7, 8});
    }

    {
        // Regression test for issue#504:
        auto rng1 = views::repeat_n( 0, 10 );
        CPP_assert(view_<decltype(rng1)>);
        CPP_assert(!common_range<decltype(rng1)>);
        CPP_assert(random_access_range<decltype(rng1)>);
        CPP_assert(sized_range<decltype(rng1)>);
        auto const& crng1 = rng1;
        CPP_assert(random_access_range<decltype(crng1)>);
        CPP_assert(sized_range<decltype(crng1)>);

        auto rng2 = rng1 | views::common;
        CPP_assert(view_<decltype(rng2)>);
        CPP_assert(common_range<decltype(rng2)>);
        CPP_assert(random_access_range<decltype(rng2)>);
        CPP_assert(sized_range<decltype(rng2)>);
        auto const& crng2 = rng2;
        CPP_assert(common_range<decltype(crng2)>);
        CPP_assert(random_access_range<decltype(crng2)>);
        CPP_assert(sized_range<decltype(crng2)>);

        ::check_equal(rng2, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    }

    {
        int const rgi[] = {1,2,3,4};
        auto rng = debug_input_view<int const>{rgi} | views::common;
        using Rng = decltype(rng);
        CPP_assert(input_range<Rng> && view_<Rng>);
        CPP_assert(!forward_range<Rng>);
        CPP_assert(common_range<Rng>);
        ::check_equal(rng, {1,2,3,4});
    }

    return ::test_result();
}
