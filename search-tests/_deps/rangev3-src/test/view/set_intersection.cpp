// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//  Copyright Tomislav Ivek 2015-2016
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <vector>
#include <sstream>
#include <range/v3/core.hpp>
#include <range/v3/range_for.hpp>
#include <range/v3/algorithm/set_algorithm.hpp>
#include <range/v3/algorithm/move.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/insert_iterators.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/utility/copy.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/const.hpp>
#include <range/v3/view/drop_while.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/view/set_algorithm.hpp>
#include <range/v3/view/stride.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/view/transform.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;

    int i1_finite[] = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4};
    int i2_finite[] = { -3, 2, 4, 4, 6, 9};

    auto i1_infinite = views::ints | views::stride(3);
    auto i2_infinite = views::ints | views::transform([](int x)
    {
        return x * x;
    });

    // intersection of two finite ranges
    {
        auto res = views::set_intersection(i1_finite, i2_finite);

        CPP_assert(view_<decltype(res)>);
        CPP_assert(forward_range<decltype(res)>);
        CPP_assert(!random_access_range<decltype(res)>);
        CPP_assert(!common_range<decltype(res)>);

        using R = decltype(res);

        CPP_assert(same_as<range_value_t<R>, int>);
        CPP_assert(same_as<range_reference_t<R>, int&>);
        CPP_assert(same_as<decltype(iter_move(begin(res))), int &&>);

        static_assert(range_cardinality<R>::value == ranges::finite, "Cardinality of intersection with a finite range should be finite!");

        ::check_equal(res, {2, 4, 4});

        CHECK(&*begin(res) == &*(begin(i1_finite) + 1));
    }

    // intersection of two infinite ranges
    {
        auto res = views::set_intersection(i1_infinite, i2_infinite);

        CPP_assert(view_<decltype(res)>);
        CPP_assert(forward_range<decltype(res)>);
        CPP_assert(!random_access_range<decltype(res)>);
        CPP_assert(!common_range<decltype(res)>);

        using R = decltype(res);

        CPP_assert(same_as<range_value_t<R>, int>);
        CPP_assert(same_as<range_reference_t<R>, range_reference_t<decltype(i1_infinite)>>);
        CPP_assert(same_as<decltype(iter_move(begin(res))), range_rvalue_reference_t<decltype(i1_infinite)>>);

        static_assert(range_cardinality<R>::value == ranges::unknown, "Cardinality of intersection of infinite ranges should be unknown!");

        ::check_equal(res | views::take(5), {0, 9, 36, 81, 144});
    }

    // intersection of a finite and infinite range
    {
        auto res = views::set_intersection(i1_finite, i2_infinite);

        CPP_assert(view_<decltype(res)>);
        CPP_assert(forward_range<decltype(res)>);
        CPP_assert(!random_access_range<decltype(res)>);
        CPP_assert(!common_range<decltype(res)>);

        using R = decltype(res);

        CPP_assert(same_as<range_value_t<R>, int>);
        CPP_assert(same_as<range_reference_t<R>, range_reference_t<decltype(i1_finite)>>);
        CPP_assert(same_as<decltype(iter_move(begin(res))), range_rvalue_reference_t<decltype(i1_finite)>>);

        static_assert(range_cardinality<R>::value == ranges::finite, "Cardinality of intersection with a finite range should be finite!");

        ::check_equal(res | views::take(500), {1, 4});

        auto res2 = views::set_intersection(i1_infinite, i2_finite);

        CPP_assert(view_<decltype(res2)>);
        CPP_assert(forward_range<decltype(res2)>);
        CPP_assert(!random_access_range<decltype(res2)>);
        CPP_assert(!common_range<decltype(res2)>);

        using R2 = decltype(res2);

        CPP_assert(same_as<range_value_t<R2>, int>);
        CPP_assert(same_as<range_reference_t<R2>, range_reference_t<decltype(i1_infinite)>>);
        CPP_assert(same_as<range_rvalue_reference_t<R2>, range_rvalue_reference_t<decltype(i1_infinite)>>);

        static_assert(range_cardinality<decltype(res2)>::value == ranges::finite, "Cardinality of intersection with a finite range should be finite!");

        ::check_equal(res2 | views::take(500), {6, 9});
    }

    // intersection of a set of unknown cardinality
    {
        auto rng0 = views::iota(10) | views::drop_while([](int i)
        {
            return i < 25;
        });
        static_assert(range_cardinality<decltype(rng0)>::value == ranges::unknown, "");

        auto res = views::set_intersection(i1_finite, rng0);
        static_assert(range_cardinality<decltype(res)>::value == ranges::unknown, "Intersection with a set of unknown cardinality should have unknown cardinality!");
    }

    // test const ranges
    {
        auto res1 = views::set_intersection(views::const_(i1_finite), views::const_(i2_finite));
        using R1 = decltype(res1);
        CPP_assert(same_as<range_value_t<R1>, int>);
        CPP_assert(same_as<range_reference_t<R1>, const int&>);
        CPP_assert(same_as<range_rvalue_reference_t<R1>, const int&&>);

        auto res2 = views::set_intersection(views::const_(i1_finite), i2_finite);
        using R2 = decltype(res2);
        CPP_assert(same_as<range_value_t<R2>, int>);
        CPP_assert(same_as<range_reference_t<R2>, const int&>);
        CPP_assert(same_as<range_rvalue_reference_t<R2>, const int&&>);
    }

    // test different orderings
    {
        auto res = views::set_intersection(views::reverse(i1_finite), views::reverse(i2_finite), [](int a, int b)
        {
            return a > b;
        });
        ::check_equal(res, {4, 4, 2});
    }

    // test projections and sets with different element types
    struct S
    {
        int val;
        bool operator==(const S& other) const
        {
            return val == other.val;
        }
    };

    S s_finite[] = {S{-20}, S{-10}, S{1}, S{3}, S{3}, S{6}, S{8}, S{20}};

    {
        auto res1 = views::set_intersection(s_finite, views::ints(-2, 10),
                                           less(),
                                           &S::val,
                                           identity()
                                          );
        using R1 = decltype(res1);
        CPP_assert(same_as<range_value_t<R1>, S>);
        CPP_assert(same_as<range_reference_t<R1>, S&>);
        CPP_assert(same_as<range_rvalue_reference_t<R1>, S&&>);
        ::check_equal(res1, {S{1}, S{3}, S{6}, S{8}});

        auto res2 = views::set_intersection(views::ints(-2, 10), s_finite,
                                           less(),
                                           identity(),
                                           [](const S& x){ return x.val; }
                                          );
        using R2 = decltype(res2);
        CPP_assert(same_as<range_value_t<R2>, int>);
        CPP_assert(same_as<range_reference_t<R2>, int>);
        CPP_assert(same_as<range_rvalue_reference_t<R2>, int>);
        ::check_equal(res2, {1, 3, 6, 8});
    }

    // move
    {
        auto v0 = to<std::vector<MoveOnlyString>>({"a","b","b","c","x","x"});
        auto v1 = to<std::vector<MoveOnlyString>>({"b","x","y","z"});
        auto res = views::set_intersection(v0, v1, [](const MoveOnlyString& a, const MoveOnlyString& b){return a<b;});

        std::vector<MoveOnlyString> expected;
        move(res, back_inserter(expected));

        ::check_equal(expected, {"b","x"});
        ::check_equal(v0, {"a","","b","c","","x"});
        ::check_equal(v1, {"b","x","y","z"});

        using R = decltype(res);

        CPP_assert(same_as<range_value_t<R>, MoveOnlyString>);
        CPP_assert(same_as<range_reference_t<R>, MoveOnlyString &>);
        CPP_assert(same_as<range_rvalue_reference_t<R>, MoveOnlyString &&>);
    }

    {
        auto rng = views::set_intersection(
            debug_input_view<int const>{i1_finite},
            debug_input_view<int const>{i2_finite}
        );
        ::check_equal(rng, {2, 4, 4});
    }

    return test_result();
}
