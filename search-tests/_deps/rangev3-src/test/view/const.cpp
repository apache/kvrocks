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

#include <iterator>
#include <functional>
#include <range/v3/core.hpp>
#include <range/v3/view/const.hpp>
#include <range/v3/view/counted.hpp>
#include <range/v3/view/zip.hpp>
#include <range/v3/view/move.hpp>
#include <range/v3/utility/copy.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

int main()
{
    using namespace ranges;

    int rgi[] = {1, 2, 3, 4};

    {
        auto rng = rgi | views::const_;
        has_type<int &>(*begin(rgi));
        has_type<int const &>(*begin(rng));
        CPP_assert(same_as<range_rvalue_reference_t<decltype(rng)>, int const &&>);
        CPP_assert(view_<decltype(rng)>);
        CPP_assert(common_range<decltype(rng)>);
        CPP_assert(sized_range<decltype(rng)>);
        CPP_assert(random_access_range<decltype(rng)>);
        ::check_equal(rng, {1, 2, 3, 4});
        CHECK(&*begin(rng) == &rgi[0]);
        CHECK(rng.size() == 4u);
    }

    {
        auto rng2 = views::counted(ForwardIterator<int*>(rgi), 4) | views::const_;
        has_type<int const &>(*begin(rng2));
        CPP_assert(same_as<range_rvalue_reference_t<decltype(rng2)>, int const &&>);
        CPP_assert(view_<decltype(rng2)>);
        CPP_assert(forward_range<decltype(rng2)>);
        CPP_assert(!bidirectional_range<decltype(rng2)>);
        CPP_assert(!common_range<decltype(rng2)>);
        CPP_assert(sized_range<decltype(rng2)>);
        ::check_equal(rng2, {1, 2, 3, 4});
        CHECK(&*begin(rng2) == &rgi[0]);
        CHECK(rng2.size() == 4u);
    }

    {
        auto zip = views::zip(rgi, rgi);
        auto rng3 = zip | views::const_;
        has_type<common_pair<int &, int &>>(*begin(zip));
        has_type<common_pair<int &&, int &&>>(iter_move(begin(zip)));
        has_type<common_pair<int const &, int const &>>(*begin(rng3));
        has_type<common_pair<int const &&, int const &&>>(iter_move(begin(rng3)));
        CPP_assert(view_<decltype(rng3)>);
        CPP_assert(random_access_range<decltype(rng3)>);
        CPP_assert(common_range<decltype(rng3)>);
        CPP_assert(sized_range<decltype(rng3)>);
        using P = std::pair<int,int>;
        ::check_equal(rng3, {P{1,1}, P{2,2}, P{3,3}, P{4,4}});
        CHECK(&(*begin(rng3)).first == &rgi[0]);
        CHECK(rng3.size() == 4u);
    }

    {
        auto zip2 = views::zip(rgi, rgi) | views::move;
        auto rng4 = zip2 | views::const_;
        has_type<common_pair<int &&, int &&>>(*begin(zip2));
        has_type<common_pair<int &&, int &&>>(iter_move(begin(zip2)));
        has_type<common_pair<int const &&, int const &&>>(*begin(rng4));
        has_type<common_pair<int const &&, int const &&>>(iter_move(begin(rng4)));
        CPP_assert(view_<decltype(rng4)>);
        CPP_assert(random_access_range<decltype(rng4)>);
        CPP_assert(common_range<decltype(rng4)>);
        CPP_assert(sized_range<decltype(rng4)>);
        using P = std::pair<int,int>;
        ::check_equal(rng4, {P{1,1}, P{2,2}, P{3,3}, P{4,4}});
        CHECK(&(*begin(rng4)).first == &rgi[0]);
        CHECK(rng4.size() == 4u);
    }

    {
        auto rng = debug_input_view<int>{rgi} | views::const_;
        CPP_assert(same_as<int const&, range_reference_t<decltype(rng)>>);
        ::check_equal(rng, rgi);
    }

    return test_result();
}
