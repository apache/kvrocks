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

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/copy.hpp>
#include <range/v3/algorithm/move.hpp>
#include <range/v3/algorithm/find_if.hpp>
#include <range/v3/utility/copy.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/insert_iterators.hpp>
#include <range/v3/view/common.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/for_each.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/map.hpp>
#include <range/v3/view/move.hpp>
#include <range/v3/view/stride.hpp>
#include <range/v3/view/take_while.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/view/zip.hpp>
#include <range/v3/view/zip_with.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

#if defined(__cpp_lib_ranges) && __cpp_lib_ranges >= 201911
// See https://github.com/ericniebler/range-v3/issues/1480
void test_bug1480()
{
    std::vector<char> const first{};
    std::vector<char> const second{};

    auto zip_view = ::ranges::views::zip(first, second);
    auto fn = [&] ([[maybe_unused]] auto && ch)
    {
    };
    std::ranges::for_each(zip_view, fn);
}
#endif

int main()
{
    using namespace ranges;

    std::vector<int> vi{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<std::string> const vs{"hello", "goodbye", "hello", "goodbye"};

    // All common ranges, but one single-pass
    {
        std::stringstream str{"john paul george ringo"};
        using V = std::tuple<int, std::string, std::string>;
        auto rng = views::zip(vi, vs, istream<std::string>(str) | views::common);
        using Rng = decltype(rng);
        CPP_assert(view_<decltype(rng)>);
        CPP_assert(!common_range<decltype(rng)>);
        CPP_assert(!sized_range<decltype(rng)>);
        CPP_assert(same_as<
            range_value_t<Rng>,
            std::tuple<int, std::string, std::string>>);
        CPP_assert(same_as<
            range_reference_t<Rng>,
            common_tuple<int &, std::string const &, std::string &>>);
        CPP_assert(same_as<
            range_rvalue_reference_t<Rng>,
            common_tuple<int &&, std::string const &&, std::string &&>>);
        CPP_assert(convertible_to<range_value_t<Rng> &&,
            range_rvalue_reference_t<Rng>>);
        CPP_assert(input_iterator<decltype(begin(rng))>);
        CPP_assert(!forward_iterator<decltype(begin(rng))>);
        has_cardinality<cardinality::finite>(rng);
        auto expected = to_vector(rng);
        ::check_equal(expected, {V{0, "hello", "john"},
                                 V{1, "goodbye", "paul"},
                                 V{2, "hello", "george"},
                                 V{3, "goodbye", "ringo"}});
    }

    // Mixed ranges and common ranges
    {
        std::stringstream str{"john paul george ringo"};
        using V = std::tuple<int, std::string, std::string>;
        auto rng = views::zip(vi, vs, istream<std::string>(str));
        CPP_assert(view_<decltype(rng)>);
        CPP_assert(!sized_range<decltype(rng)>);
        CPP_assert(!common_range<decltype(rng)>);
        CPP_assert(input_iterator<decltype(begin(rng))>);
        CPP_assert(!forward_iterator<decltype(begin(rng))>);
        has_cardinality<cardinality::finite>(rng);
        std::vector<V> expected;
        copy(rng, ranges::back_inserter(expected));
        ::check_equal(expected, {V{0, "hello", "john"},
                                 V{1, "goodbye", "paul"},
                                 V{2, "hello", "george"},
                                 V{3, "goodbye", "ringo"}});
    }

    auto rnd_rng = views::zip(vi, vs);
    using Ref = range_reference_t<decltype(rnd_rng)>;
    static_assert(std::is_same<Ref, common_pair<int &,std::string const &>>::value, "");
    CPP_assert(view_<decltype(rnd_rng)>);
    CPP_assert(common_range<decltype(rnd_rng)>);
    CPP_assert(sized_range<decltype(rnd_rng)>);
    CPP_assert(random_access_iterator<decltype(begin(rnd_rng))>);
    has_cardinality<cardinality::finite>(rnd_rng);
    auto tmp = cbegin(rnd_rng) + 3;
    CHECK(std::get<0>(*tmp) == 3);
    CHECK(std::get<1>(*tmp) == "goodbye");

    CHECK((rnd_rng.end() - rnd_rng.begin()) == 4);
    CHECK((rnd_rng.begin() - rnd_rng.end()) == -4);
    CHECK(rnd_rng.size() == 4u);

    // zip_with
    {
        std::vector<std::string> v0{"a","b","c"};
        std::vector<std::string> v1{"x","y","z"};

        auto rng = views::zip_with(std::plus<std::string>{}, v0, v1);
        std::vector<std::string> expected;
        copy(rng, ranges::back_inserter(expected));
        ::check_equal(expected, {"ax","by","cz"});

        auto rng2 = views::zip_with([] { return 42; });
        static_assert(std::is_same<range_value_t<decltype(rng2)>, int>::value, "");
    }

    // Move from a zip view
    {
        auto v0 = to<std::vector<MoveOnlyString>>({"a","b","c"});
        auto v1 = to<std::vector<MoveOnlyString>>({"x","y","z"});

        auto rng = views::zip(v0, v1);
        CPP_assert(random_access_range<decltype(rng)>);
        std::vector<std::pair<MoveOnlyString, MoveOnlyString>> expected;
        move(rng, ranges::back_inserter(expected));
        ::check_equal(expected | views::keys, {"a","b","c"});
        ::check_equal(expected | views::values, {"x","y","z"});
        ::check_equal(v0, {"","",""});
        ::check_equal(v1, {"","",""});

        move(expected, rng.begin());
        ::check_equal(expected | views::keys, {"","",""});
        ::check_equal(expected | views::values, {"","",""});
        ::check_equal(v0, {"a","b","c"});
        ::check_equal(v1, {"x","y","z"});

        std::vector<MoveOnlyString> res;
        using R = decltype(rng);
        auto proj =
            [](range_reference_t<R> p) -> MoveOnlyString& {return p.first;};
        auto rng2 = rng | views::transform(proj);
        move(rng2, ranges::back_inserter(res));
        ::check_equal(res, {"a","b","c"});
        ::check_equal(v0, {"","",""});
        ::check_equal(v1, {"x","y","z"});
        using R2 = decltype(rng2);
        CPP_assert(same_as<range_value_t<R2>, MoveOnlyString>);
        CPP_assert(same_as<range_reference_t<R2>, MoveOnlyString &>);
        CPP_assert(same_as<range_rvalue_reference_t<R2>, MoveOnlyString &&>);
    }

    {
        auto const v = to<std::vector<MoveOnlyString>>({"a","b","c"});
        auto rng = views::zip(v, v);
        using Rng = decltype(rng);
        using I = iterator_t<Rng>;
        CPP_assert(indirectly_readable<I>);
        CPP_assert(same_as<
            range_value_t<Rng>,
            std::pair<MoveOnlyString, MoveOnlyString>>);
        CPP_assert(same_as<
            range_reference_t<Rng>,
            common_pair<MoveOnlyString const &, MoveOnlyString const &>>);
        CPP_assert(same_as<
            range_rvalue_reference_t<Rng>,
            common_pair<MoveOnlyString const &&, MoveOnlyString const &&>>);
        CPP_assert(same_as<
            range_common_reference_t<Rng>,
            common_pair<MoveOnlyString const &, MoveOnlyString const &>>);
    }

    {
        std::vector<int> v{1,2,3,4};
        auto moved = v | views::move;
        using Moved = decltype(moved);
        CPP_assert(same_as<range_reference_t<Moved>, int &&>);
        auto zipped = views::zip(moved);
        using Zipped = decltype(zipped);
        CPP_assert(same_as<range_reference_t<Zipped>, common_tuple<int &&> >);
    }

    // This is actually a test of the logic of view_adaptor. Since the stride view
    // does not redefine the current member function, the base range's iter_move
    // function gets picked up automatically.
    {
        auto rng0 = views::zip(vi, vs);
        auto rng1 = views::stride(rng0, 2);
        CPP_assert(same_as<range_rvalue_reference_t<decltype(rng1)>, range_rvalue_reference_t<decltype(rng0)>>);
        CPP_assert(same_as<range_value_t<decltype(rng1)>, range_value_t<decltype(rng0)>>);
    }

    // Test for noexcept iter_move
    {
        static_assert(noexcept(std::declval<std::unique_ptr<int>&>() = std::declval<std::unique_ptr<int>&&>()), "");
        std::unique_ptr<int> rg1[10], rg2[10];
        auto x = views::zip(rg1, rg2);
        std::pair<std::unique_ptr<int>, std::unique_ptr<int>> p = iter_move(x.begin());
        auto it = x.begin();
        static_assert(noexcept(iter_move(it)), "");
    }

    // Really a test for common_iterator's iter_move, but this is a good place for it.
    {
        std::unique_ptr<int> rg1[10], rg2[10];
        auto rg3 = rg2 | views::take_while([](std::unique_ptr<int> &){return true;});
        auto x = views::zip(rg1, rg3);
        CPP_assert(!common_range<decltype(x)>);
        auto y = x | views::common;
        std::pair<std::unique_ptr<int>, std::unique_ptr<int>> p = iter_move(y.begin());
        auto it = x.begin();
        static_assert(noexcept(iter_move(it)), "");
    }

    // Regression test for #439.
    {
        std::vector<int> vec{0,1,2};
        auto rng = vec | views::for_each([](int i) { return ranges::yield(i); });
        ranges::distance(views::zip(views::iota(0), rng) | views::common);
    }

    {
        int const i1[] = {0,1,2,3};
        int const i2[] = {4,5,6,7};
        auto rng = views::zip(
            debug_input_view<int const>{i1},
            debug_input_view<int const>{i2}
        );
        using P = std::pair<int, int>;
        has_cardinality<cardinality::finite>(rng);
        ::check_equal(rng, {P{0,4},P{1,5}, P{2,6}, P{3,7}});
    }

    {
        // Test with no ranges
        auto rng = views::zip();
        using R = decltype(rng);
        CPP_assert(same_as<range_value_t<R>, std::tuple<>>);
        CPP_assert(contiguous_range<R>);
        has_cardinality<cardinality(0)>(rng);
        CHECK(ranges::begin(rng) == ranges::end(rng));
        CHECK(ranges::size(rng) == 0u);
    }

    {
        // test dangling
        auto true_ = [](auto&&){ return true; };

        CHECK(!::is_dangling(ranges::find_if(views::zip(vi, vs), true_)));
        CHECK(!::is_dangling(ranges::find_if(views::zip(
            vi | views::move,
            vs | views::common
            ), true_)));
        CHECK(::is_dangling(ranges::find_if(views::zip(
            vi | views::filter(true_)), true_)));
    }

    {
        // test zip with infinite range
        int const i1[] = {0,1,2,3};
        auto rng = views::zip(i1, views::iota(4));

        has_cardinality<cardinality(4)>(rng);
        using P = std::pair<int, int>;
        ::check_equal(rng, {P{0,4},P{1,5}, P{2,6}, P{3,7}});
    }

    {
        // test zip with infinite ranges only
        auto rng = views::zip(views::iota(0), views::iota(4));

        has_cardinality<cardinality::infinite>(rng);
        using P = std::pair<int, int>;
        ::check_equal(rng | views::take(4), {P{0,4},P{1,5}, P{2,6}, P{3,7}});
    }

    {
        // test unknown cardinality
        std::stringstream str{};
        auto rng = views::zip(istream<std::string>(str));

        has_cardinality<cardinality::unknown>(rng);
    }

    return test_result();
}
