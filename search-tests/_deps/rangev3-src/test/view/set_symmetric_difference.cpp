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
#include <range/v3/utility/common_type.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/insert_iterators.hpp>
#include <range/v3/iterator/move_iterators.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/const.hpp>
#include <range/v3/view/drop_while.hpp>
#include <range/v3/view/empty.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/view/set_algorithm.hpp>
#include <range/v3/view/stride.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/view/zip.hpp>
#include <range/v3/utility/copy.hpp>
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

    // symmetric difference between two finite ranges/sets
    {
        auto res = views::set_symmetric_difference(i1_finite, i2_finite);

        CPP_assert(view_<decltype(res)>);
        CPP_assert(forward_range<decltype(res)>);
        CPP_assert(!random_access_range<decltype(res)>);
        CPP_assert(!common_range<decltype(res)>);

        using R = decltype(res);

        CPP_assert(same_as<range_value_t<R>, int>);
        CPP_assert(same_as<range_reference_t<R>, int&>);
        CPP_assert(same_as<decltype(iter_move(begin(res))), int&&>);

        static_assert(range_cardinality<R>::value == ranges::finite, "Cardinality of symmetric difference of finite ranges should be finite!");

        ::check_equal(res, {-3, 1, 2, 3, 3, 3, 4, 4, 6, 9});

        // check if the final result agrees with the greedy algorithm
        std::vector<int> greedy_sd;
        set_symmetric_difference(i1_finite, i2_finite, back_inserter(greedy_sd));
        ::check_equal(res, greedy_sd);

        auto it = begin(res);
        CHECK(&*it == &*(begin(i2_finite)));
        ++it;
        CHECK(&*it == &*(begin(i1_finite)));
    }

    // symmetric difference between two infinite ranges
    {
        auto res = views::set_symmetric_difference(i1_infinite, i2_infinite);

        CPP_assert(view_<decltype(res)>);
        CPP_assert(forward_range<decltype(res)>);
        CPP_assert(!random_access_range<decltype(res)>);
        CPP_assert(!common_range<decltype(res)>);

        using R = decltype(res);

        CPP_assert(same_as<range_value_t<R>,
                            common_type_t<range_value_t<decltype(i1_infinite)>,
                                          range_value_t<decltype(i2_infinite)>>>);
        CPP_assert(same_as<range_reference_t<R>,
                            common_reference_t<range_reference_t<decltype(i1_infinite)>,
                                               range_reference_t<decltype(i2_infinite)>>
                           >);
        CPP_assert(same_as<range_rvalue_reference_t<R>,
                            common_reference_t<range_rvalue_reference_t<decltype(i1_infinite)>,
                                               range_rvalue_reference_t<decltype(i2_infinite)>>
                           >);

        static_assert(range_cardinality<R>::value == ranges::unknown, "Cardinality of symmetric difference between infinite ranges should be unknown!");

        ::check_equal(res | views::take(6), {1, 3, 4, 6, 12, 15});

        // check if the final result agrees with the greedy algorithm
        std::vector<int> greedy_sd;
        set_symmetric_difference(i1_infinite | views::take(10), i2_infinite | views::take(10), back_inserter(greedy_sd));
        ::check_equal(res | views::take(6), greedy_sd | views::take(6));
    }

    // symmetric difference between a finite and an infinite range
    {
        auto res1 = views::set_symmetric_difference(i1_finite, i2_infinite);

        CPP_assert(view_<decltype(res1)>);
        CPP_assert(forward_range<decltype(res1)>);
        CPP_assert(!random_access_range<decltype(res1)>);
        CPP_assert(!common_range<decltype(res1)>);

        using R1 = decltype(res1);

        CPP_assert(same_as<range_value_t<R1>, int>);
        CPP_assert(same_as<range_reference_t<R1>, int>); // our infinite range does not give out references
        CPP_assert(same_as<range_rvalue_reference_t<R1>, int>);

        static_assert(range_cardinality<R1>::value == ranges::infinite, "Cardinality of symmetric difference between a finite and an infinite range should be infinite!");

        ::check_equal(res1 | views::take(10), {0, 2, 2, 3, 3, 3, 4, 4, 4, 9});

        // now swap the operands:
        auto res2 = views::set_symmetric_difference(i2_infinite, i1_finite);

        CPP_assert(view_<decltype(res2)>);
        CPP_assert(forward_range<decltype(res2)>);
        CPP_assert(!random_access_range<decltype(res2)>);
        CPP_assert(!common_range<decltype(res2)>);

        using R2 = decltype(res2);

        CPP_assert(same_as<range_value_t<R2>, int>);
        CPP_assert(same_as<range_reference_t<R2>, int>); // our infinite range does not give out references
        CPP_assert(same_as<range_rvalue_reference_t<R2>, int>);

        static_assert(range_cardinality<R2>::value == ranges::infinite, "Cardinality of symmetric difference between a finite and an infinite range should be infinite!");

        ::check_equal(res1 | views::take(10), res2 | views::take(10));
    }

    // symmetric differences involving unknown cardinalities
    {
        auto rng0 = views::iota(10) | views::drop_while([](int i)
        {
            return i < 25;
        });
        static_assert(range_cardinality<decltype(rng0)>::value == ranges::unknown, "");

        auto res1 = views::set_symmetric_difference(i2_finite, rng0);
        static_assert(range_cardinality<decltype(res1)>::value == ranges::unknown, "Symmetric difference between a finite and unknown cardinality set should have unknown cardinality!");

        auto res2 = views::set_symmetric_difference(rng0, i2_finite);
        static_assert(range_cardinality<decltype(res2)>::value == ranges::unknown, "Symmetric difference between an unknown and finite cardinality set should have unknown cardinality!");

        auto res3 = views::set_symmetric_difference(i1_infinite, rng0);
        static_assert(range_cardinality<decltype(res3)>::value == ranges::unknown, "Symmetric difference between an infinite and unknown cardinality set should have unknown cardinality!");

        auto res4 = views::set_symmetric_difference(rng0, i1_infinite);
        static_assert(range_cardinality<decltype(res4)>::value == ranges::unknown, "Symmetric difference between an unknown and infinite cardinality set should have infinite cardinality!");

        auto res5 = views::set_symmetric_difference(rng0, rng0);
        static_assert(range_cardinality<decltype(res5)>::value == ranges::unknown, "Symmetric difference between two unknown cardinality sets should have unknown cardinality!");
    }

    // test const ranges
    {
        auto res1 = views::set_symmetric_difference(views::const_(i1_finite), views::const_(i2_finite));
        using R1 = decltype(res1);
        CPP_assert(same_as<range_value_t<R1>, int>);
        CPP_assert(same_as<range_reference_t<R1>, const int&>);
        CPP_assert(same_as<range_rvalue_reference_t<R1>, const int&&>);

        auto res2 = views::set_symmetric_difference(views::const_(i1_finite), i2_finite);
        using R2 = decltype(res2);
        CPP_assert(same_as<range_value_t<R2>, int>);
        CPP_assert(same_as<range_reference_t<R2>, const int&>);
        CPP_assert(same_as<range_rvalue_reference_t<R2>, const int&&>);
    }

    // test different orderings
    {
        auto res = views::set_symmetric_difference(views::reverse(i1_finite), views::reverse(i2_finite), [](int a, int b)
        {
            return a > b;
        });
        ::check_equal(res, {9, 6, 4, 4, 3, 3, 3, 2, 1, -3});
        CHECK(&*begin(res) == &*(begin(i2_finite) + 5));
    }

    struct B
    {
        int val;
        B(int i): val{i} {}
        bool operator==(const B& other) const
        {
            return val == other.val;
        }
    };

    struct D: public B
    {
        D(int i): B{i} {}
        D(B b): B{std::move(b)} {}
    };

    B b_finite[] = {B{-20}, B{-10}, B{1}, B{3}, B{3}, B{6}, B{8}, B{20}};
    D d_finite[] = {D{0}, D{2}, D{4}, D{6}};

    // sets with different element types, custom orderings
    {
        auto res = views::set_symmetric_difference(b_finite, d_finite, [](const B& a, const D& b){ return a.val < b.val; });
        CPP_assert(same_as<range_value_t<decltype(res)>, B>);
        CPP_assert(same_as<range_reference_t<decltype(res)>, B&>);
        CPP_assert(same_as<range_rvalue_reference_t<decltype(res)>, B&&>);
        ::check_equal(res, {B{-20}, B{-10}, B{0}, B{1}, B{2}, B{3}, B{3}, B{4}, B{8}, B{20}});
        auto it = begin(res);
        CHECK(&*it == &*begin(b_finite));
        advance(it, 2);
        CHECK(&*it == &*begin(d_finite));
    }

    // projections
    {
        auto res1 = views::set_symmetric_difference(b_finite, d_finite,
                                                   less(),
                                                   &B::val,
                                                   &D::val
                                                  );
        CPP_assert(same_as<range_value_t<decltype(res1)>, B>);
        CPP_assert(same_as<range_reference_t<decltype(res1)>, B&>);
        CPP_assert(same_as<range_rvalue_reference_t<decltype(res1)>, B&&>);
        ::check_equal(res1, {B{-20}, B{-10}, B{0}, B{1}, B{2}, B{3}, B{3}, B{4}, B{8}, B{20}});

        auto res2 = views::set_symmetric_difference(views::ints(-2, 10), b_finite,
                                                   less(),
                                                   identity(),
                                                   [](const B& x){ return x.val; }
                                                  );
        CPP_assert(same_as<range_value_t<decltype(res2)>, B>);
        CPP_assert(same_as<range_reference_t<decltype(res2)>, B>);
        CPP_assert(same_as<range_rvalue_reference_t<decltype(res2)>, B>);
        ::check_equal(res2, {B{-20}, B{-10}, B{-2}, B{-1}, B{0}, B{2}, B{3}, B{4}, B{5}, B{7}, B{9}, B{20}});
    }

    // move
    {
        auto v0 = to<std::vector<MoveOnlyString>>({"a","b","b","c","x","x"});
        auto v1 = to<std::vector<MoveOnlyString>>({"b","x","y","z"});
        auto res = views::set_symmetric_difference(v0, v1, [](const MoveOnlyString& a, const MoveOnlyString& b){return a<b;});

        std::vector<MoveOnlyString> expected;
        move(res, back_inserter(expected));

        ::check_equal(expected, {"a","b","c","x","y","z"});
        ::check_equal(v1, {"b","x","",""});
        ::check_equal(v0, {"","b","","","x",""});

        auto v0_greedy = to<std::vector<MoveOnlyString>>({"a","b","b","c","x","x"});
        auto v1_greedy = to<std::vector<MoveOnlyString>>({"b","x","y","z"});
        std::vector<MoveOnlyString> expected_greedy;
        set_symmetric_difference(v0_greedy, v1_greedy,
                                 move_into(back_inserter(expected_greedy)),
                                 [](const MoveOnlyString& a, const MoveOnlyString& b){return a<b;});
        ::check_equal(expected_greedy, expected);
        ::check_equal(v0_greedy, v0);
        ::check_equal(v1_greedy, v1);

        using R = decltype(res);

        CPP_assert(same_as<range_value_t<R>, MoveOnlyString>);
        CPP_assert(same_as<range_reference_t<R>, MoveOnlyString &>);
        CPP_assert(same_as<range_rvalue_reference_t<R>, MoveOnlyString &&>);
    }

    // WARNING: set_symmetric_difference between two infinite ranges can create infinite loops!
    // {
    //     auto empty_range = views::set_symmetric_difference(views::ints, views::ints);
    //     begin(empty_range); // infinite loop!
    // }

    // iterator (in)equality
    {
        int r1[] = {1, 2, 3};
        int r2[] = {   2, 3, 4, 5};
        auto res = views::set_symmetric_difference(r1, r2); // 1, 4, 5

        auto it1 = ranges::next(res.begin()); // *it1 == 4, member iterator into r1 points to r1.end()
        auto it2 = ranges::next(it1);         // *it2 == 5, member iterator into r1 also points to r1.end()
        auto sentinel = res.end();

        CHECK(*it1 == 4);
        CHECK(*it2 == 5);

        CHECK(it1 != it2); // should be different even though member iterators into r1 are the same

        CHECK(it1 != sentinel);
        CHECK(ranges::next(it1, 2) == sentinel);

        CHECK(it2 != sentinel);
        CHECK(ranges::next(it2, 1) == sentinel);
    }

    {
        auto rng = views::set_symmetric_difference(
            debug_input_view<int const>{i1_finite},
            debug_input_view<int const>{i2_finite}
        );
        ::check_equal(rng, {-3, 1, 2, 3, 3, 3, 4, 4, 6, 9});
    }

    return test_result();
}
