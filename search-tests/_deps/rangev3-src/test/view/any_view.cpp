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

#include <map>
#include <vector>

#include <range/v3/core.hpp>
#include <range/v3/utility/copy.hpp>
#include <range/v3/view/any_view.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/map.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/view/tail.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/view/take_exactly.hpp>

#include "../simple_test.hpp"
#include "../test_utils.hpp"

namespace
{
    template<typename S, typename T, typename = void>
    struct can_convert_to : std::false_type
    {};
    template<typename S, typename T>
    struct can_convert_to<
        S, T, meta::void_<decltype(ranges::polymorphic_downcast<T>(std::declval<S>()))>>
      : std::true_type
    {};

    void test_polymorphic_downcast()
    {
        struct A
        {
            virtual ~A() = default;
        };
        struct B : A
        {};
        struct unrelated
        {};
        struct incomplete;

        CPP_assert(can_convert_to<B *, void *>());
        CPP_assert(can_convert_to<A *, void *>());
        CPP_assert(can_convert_to<B *, A *>());
        CPP_assert(can_convert_to<A *, B *>());
        CPP_assert(!can_convert_to<int, int>());
        CPP_assert(!can_convert_to<A const *, A *>());
        CPP_assert(!can_convert_to<A *, unrelated *>());
        CPP_assert(!can_convert_to<unrelated *, A *>());
        CPP_assert(!can_convert_to<incomplete *, incomplete *>());

        CPP_assert(can_convert_to<B &, A &>());
        CPP_assert(can_convert_to<A &, B &>());
        CPP_assert(!can_convert_to<A &, unrelated &>());
        CPP_assert(!can_convert_to<unrelated &, A &>());
        CPP_assert(!can_convert_to<incomplete &, incomplete &>());

#if !defined(__GNUC__) || defined(__clang__) || __GNUC__ > 4
        CPP_assert(can_convert_to<B &&, A &&>());
        CPP_assert(can_convert_to<B &, A &&>());
#endif // old GCC dynamic_cast bug
        CPP_assert(!can_convert_to<B &&, A &>());
        CPP_assert(can_convert_to<A &&, B &&>());
        CPP_assert(!can_convert_to<A &&, B &>());
        CPP_assert(can_convert_to<A &, B &&>());
        CPP_assert(!can_convert_to<A &&, unrelated &&>());
        CPP_assert(!can_convert_to<A &&, unrelated &>());
        CPP_assert(!can_convert_to<A &, unrelated &&>());
        CPP_assert(!can_convert_to<unrelated &&, A &&>());
        CPP_assert(!can_convert_to<unrelated &&, A &>());
        CPP_assert(!can_convert_to<unrelated &, A &&>());
        CPP_assert(!can_convert_to<incomplete &&, incomplete &&>());
        CPP_assert(!can_convert_to<incomplete &&, incomplete &>());
        CPP_assert(!can_convert_to<incomplete &, incomplete &&>());
    }
} // unnamed namespace

int main()
{
    using namespace ranges;
    auto const ten_ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    {
        any_view<int> ints = views::ints;
        CPP_assert(input_range<decltype(ints)> && view_<decltype(ints)>);
        CPP_assert(!(forward_range<decltype(ints)> && view_<decltype(ints)>));
        ::check_equal(std::move(ints) | views::take(10), ten_ints);
    }
    {
        any_view<int> ints = views::ints | views::take_exactly(5);
        CPP_assert(input_range<decltype(ints)> && view_<decltype(ints)>);
        CPP_assert(!(random_access_range<decltype(ints)> && view_<decltype(ints)>));
        CPP_assert(!(sized_range<decltype(ints)> && view_<decltype(ints)>));
        static_assert((get_categories<decltype(ints)>() & category::random_access) ==
                          category::input,
                      "");
        static_assert(
            (get_categories<decltype(ints)>() & category::sized) == category::none, "");
    }
    {
#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
#if defined(__clang__) && __clang_major__ < 6
        // Workaround https://bugs.llvm.org/show_bug.cgi?id=33314
        RANGES_DIAGNOSTIC_PUSH
        RANGES_DIAGNOSTIC_IGNORE_UNDEFINED_FUNC_TEMPLATE
#endif
        any_view ints = views::ints | views::take_exactly(5);
#if defined(__clang__) && __clang_major__ < 6
        RANGES_DIAGNOSTIC_POP
#endif
#else
        any_view<int, category::random_access | category::sized> ints =
            views::ints | views::take_exactly(5);
#endif
        CPP_assert(random_access_range<decltype(ints)> && view_<decltype(ints)>);
        CPP_assert(sized_range<decltype(ints)> && view_<decltype(ints)>);
        static_assert((get_categories<decltype(ints)>() & category::random_access) ==
                          category::random_access,
                      "");
        static_assert(
            (get_categories<decltype(ints)>() & category::sized) == category::sized, "");
    }
    {
        any_view<int, category::input | category::sized> ints =
            views::ints | views::take_exactly(10);
        CPP_assert(input_range<decltype(ints)> && view_<decltype(ints)>);
        CPP_assert(sized_range<decltype(ints)> && view_<decltype(ints)>);
        static_assert(
            (get_categories<decltype(ints)>() & category::input) == category::input, "");
        static_assert(
            (get_categories<decltype(ints)>() & category::sized) == category::sized, "");
    }
    {
        any_view<int, category::bidirectional> ints = views::ints;
        CPP_assert(bidirectional_range<decltype(ints)> && view_<decltype(ints)>);
        CPP_assert(!(random_access_range<decltype(ints)> && view_<decltype(ints)>));
        static_assert((get_categories<decltype(ints)>() & category::random_access) ==
                          category::bidirectional,
                      "");
    }
    {
        any_view<int> ints2 = views::ints | views::take(10);
        ::check_equal(ints2, ten_ints);
        ::check_equal(ints2, ten_ints);
    }
    {
        any_view<int, category::random_access> ints3 = views::ints | views::take(10);
        CPP_assert(view_<decltype(ints3)>);
        CPP_assert(random_access_range<decltype(ints3)>);
        CPP_assert(!common_range<decltype(ints3)>);
        ::check_equal(ints3, ten_ints);
        ::check_equal(ints3, ten_ints);
        ::check_equal(aux::copy(ints3), ten_ints);
        ::check_equal(ints3 | views::reverse, {9, 8, 7, 6, 5, 4, 3, 2, 1, 0});
    }
    {
        any_view<int&> e;
        CHECK(e.begin() == e.begin());
        CHECK(e.begin() == e.end());
    }
    {
        iterator_t<any_view<int&, category::random_access>> i{},j{};
        sentinel_t<any_view<int&, category::random_access>> k{};
        CHECK(i == j);
        CHECK(i == k);
        CHECK((i - j) == 0);
    }
    
    // Regression test for #446
    {
        auto vec = std::vector<short>{begin(ten_ints), end(ten_ints)};
        ::check_equal(any_view<int>{vec}, ten_ints);
        ::check_equal(any_view<int>{ranges::detail::as_const(vec)}, ten_ints);
    
        struct Int
        {
            int i_;
    
            Int(int i) : i_{i} {}
            operator int() const { return i_; }
        };
        auto vec2 = std::vector<Int>{begin(ten_ints), end(ten_ints)};
        ::check_equal(any_view<int>{vec2}, ten_ints);
    }
    
    {
        auto v = any_view<int>{debug_input_view<int const>{
            ten_ints.begin(), std::ptrdiff_t(ten_ints.size())
        }};
        ::check_equal(v, ten_ints);
    }
    
    // Regression test for #880
    {
        using namespace ranges;
        std::map<int, int> mm{ {0, 1}, {2, 3} };
        any_view<int, category::forward | category::sized> as_any =
            mm | views::keys;
        (void)as_any;
    }
    
    // Regression test for #1101
    {
        using namespace ranges;
        std::vector<int> v = { 1, 2, 3, 4, 5 };
    
        using SizedAnyView =
            any_view<int, category::random_access | category::sized>;
    
        SizedAnyView av1 = v;
        SizedAnyView av2 = av1 | views::transform( [](auto){ return 0; } ); // fail
        SizedAnyView av3 = av1 | views::tail; // fail
    }
    
    test_polymorphic_downcast();

    return test_result();
}
