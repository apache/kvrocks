/// \file
// Range v3 library
//
//  Copyright Andrey Diduh 2019
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#include "../simple_test.hpp"
#include "../test_utils.hpp"

#include <type_traits>
#include <vector>
#include <list>
#include <forward_list>

#include <range/v3/view/drop_last.hpp>
#include <range/v3/view/take_exactly.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/view/generate_n.hpp>

using namespace ranges;

template<class Rng>
struct view_non_const_only
  : view_adaptor<view_non_const_only<Rng>, Rng>
{
private:
    friend range_access;

    adaptor_base begin_adaptor() { return {}; }
    adaptor_base end_adaptor()   { return {}; }
public:
    using view_non_const_only::view_adaptor::view_adaptor;

    CPP_auto_member
    auto CPP_fun(size)() (
        requires sized_range<Rng>)
    {
        return ranges::size(this->base());
    }
};

template<class Rng>
view_non_const_only<views::all_t<Rng>> non_const_only(Rng &&rng)
{
    return view_non_const_only<views::all_t<Rng>>{views::all(static_cast<Rng&&>(rng))};
}

template<class Rng>
void test_range(Rng&& src)
{
    // additional src copy for InputStream
    {
        auto src_ = src;
        ::check_equal(src_, {1,2,3,4});
    }
    {
        auto src_ = src;
        auto list = src_ | views::drop_last(2);
        ::check_equal(list, {1,2});
    }
    {
        auto src_ = src;
        auto list = src_ | views::drop_last(0);
        ::check_equal(list, {1,2,3,4});
    }
    {
        auto src_ = src;
        auto list = src_ | views::drop_last(4);
        CHECK(list.empty());
    }
    {
        auto src_ = src;
        auto list = src_ | views::drop_last(5);
        CHECK(list.empty());
    }
}

template<class Rng>
void test_size(Rng&& src)
{
    CHECK( (src | views::drop_last(0)).size() == std::size_t(4) );
    CHECK( (src | views::drop_last(2)).size() == std::size_t(2) );
    CHECK( (src | views::drop_last(4)).size() == std::size_t(0) );
    CHECK( (src | views::drop_last(5)).size() == std::size_t(0) );
}

template<class Rng>
void test_non_convert_range(Rng&& src)
{
    // test non-convertible const<=>non-const range
    test_range(src | views::transform([](const int& i) -> const int& {return i;}));
}

void random_acccess_test()
{
    using Src = std::vector<int>;
    static_assert(
        ranges::random_access_range<Src>
        , "Must be exactly RA.");
    static_assert(
        std::is_same<
            drop_last_view<views::all_t<Src&>>,
            drop_last_view<views::all_t<Src&>, detail::drop_last_view::mode_bidi>
        >::value
        , "Must have correct view.");

    Src src = {1,2,3,4};

    test_range(src);
    test_range(non_const_only(src));
    test_size(src);
    test_non_convert_range(src);
}

void bidirectional_test()
{
    using Src = std::list<int>;
    static_assert(
        !ranges::random_access_range<Src> &&
        ranges::bidirectional_range<Src>
        , "Must be exactly bidirectional.");
    static_assert(
        std::is_same<
            /* mode_sized for max_performance profile.
             * mode_bidi  for compatible profile.
             * See aux::drop_last::get_mode */
            drop_last_view<views::all_t<Src&>>,
            drop_last_view<views::all_t<Src&>, detail::drop_last_view::mode_bidi>
        >::value
        , "Must have correct view.");

    Src src = {1,2,3,4};

    test_range(src);
    test_range(non_const_only(src));
    test_size(src);
    test_non_convert_range(src);
}

void forward_test()
{
    using Src = std::forward_list<int>;
    static_assert(
        !ranges::bidirectional_range<Src> &&
        ranges::forward_range<Src>
        , "Must be exactly forward.");
    static_assert(
        std::is_same<
            drop_last_view<views::all_t<Src&>>,
            drop_last_view<views::all_t<Src&>, detail::drop_last_view::mode_forward>
        >::value
        , "Must have correct view.");

    Src src = {1,2,3,4};

    test_range(src);
    test_range(non_const_only(src));
    test_size(src | views::take_exactly(4));
    test_non_convert_range(src);
}

void sized_test()
{
    int i = 0;
    auto src  = views::generate_n([i]() mutable -> int { return ++i;}, 4);
    using Src = decltype(src);
    static_assert(
        !ranges::forward_range<Src> &&
        ranges::input_range<Src>
        , "Must be exactly input.");

    static_assert(
        std::is_same<
            drop_last_view<views::all_t<Src>>,
            drop_last_view<views::all_t<Src>, detail::drop_last_view::mode_sized>
        >::value
        , "Must have correct view.");

    {
        // always non-const
        auto src_ = src;
        test_range(src_);
    }
    {
        auto src_ = src;
        test_size(src_);
    }
    {
        auto src_ = src;
        test_range(non_const_only(std::move(src_)));
    }
    {
        auto src_ = src;
        test_non_convert_range(src_);
    }
}

int main()
{
    random_acccess_test();
    bidirectional_test();
    forward_test();
    sized_test();

    return test_result();
}
