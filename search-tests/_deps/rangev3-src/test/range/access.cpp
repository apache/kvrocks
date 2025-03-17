// Range v3 library
//
//  Copyright Casey Carter 2016
//  Copyright Eric Niebler 2018
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <range/v3/range/access.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/view/subrange.hpp>
#include <range/v3/view/ref.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/algorithm/find.hpp>
#include <vector>
#include "../simple_test.hpp"

#if defined(__clang__)
RANGES_DIAGNOSTIC_IGNORE("-Wunused-const-variable")
#endif

void test_range_access_ambiguity()
{
    std::vector<ranges::reverse_iterator<int*>> vri{};
    using namespace ranges;
    (void)begin(vri);
    (void)end(vri);
    (void)cbegin(vri);
    (void)cend(vri);
    (void)rbegin(vri);
    (void)rend(vri);
    (void)crbegin(vri);
    (void)crend(vri);
}

void test_initializer_list()
{
    std::initializer_list<int> il = {0,1,2};
    {
        int count = 0;
        for(auto p = ranges::begin(il), e = ranges::end(il); p != e; ++p) {
            CHECK(*p == count++);
        }
    }
    {
        int count = 3;
        for(auto p = ranges::rbegin(il), e = ranges::rend(il); p != e; ++p) {
            CHECK(*p == --count);
        }
    }
    CHECK(ranges::size(il) == std::size_t{3});
    CHECK(ranges::data(il) == &*il.begin());
    CHECK(ranges::empty(il) == false);
}

template<class Value, typename T, T... Is>
void test_array(std::integer_sequence<T, Is...>)
{
    Value a[sizeof...(Is)] = { Is... };
    {
        int count = 0;
        for(auto p = ranges::begin(a), e = ranges::end(a); p != e; ++p) {
            CHECK(*p == count++);
        }
    }
    {
        int count = sizeof...(Is);
        for(auto p = ranges::rbegin(a), e = ranges::rend(a); p != e; ++p) {
            CHECK(*p == --count);
        }
    }
    CHECK(ranges::size(a) == sizeof...(Is));
    CHECK(ranges::data(a) == a + 0);
    CHECK(ranges::empty(a) == false);
}

namespace begin_testing
{
    template<class R>
    CPP_requires(can_begin_,
        requires(R&& r) //
        (
            ranges::begin((R &&) r)
        ));
    template<class R>
    CPP_concept can_begin =
        CPP_requires_ref(can_begin_, R);

    template<class R>
    CPP_requires(can_cbegin_,
        requires(R&& r) //
        (
            ranges::cbegin((R &&) r)
        ));
    template<class R>
    CPP_concept can_cbegin =
        CPP_requires_ref(can_cbegin_, R);

    struct A
    {
        int* begin();
        int* end();
        int const * begin() const;
        int const * end() const;
    };

    struct B : A {};
    void* begin(B&);

    struct C : A {};
    void begin(C&);

    struct D : A {};
    char* begin(D&);

    void test()
    {
        // Valid
        CPP_assert(can_begin<int(&)[2]>);
        CPP_assert(ranges::same_as<decltype(ranges::begin(std::declval<int(&)[2]>())), int*>);
        CPP_assert(can_begin<int const(&)[2]>);
        CPP_assert(ranges::same_as<decltype(ranges::begin(std::declval<int const(&)[2]>())), int const *>);

        CPP_assert(can_cbegin<int(&)[2]>);
        CPP_assert(ranges::same_as<decltype(ranges::cbegin(std::declval<int(&)[2]>())), int const *>);
        CPP_assert(can_cbegin<int const(&)[2]>);
        CPP_assert(ranges::same_as<decltype(ranges::cbegin(std::declval<int const(&)[2]>())), int const *>);

#ifndef RANGES_WORKAROUND_MSVC_573728
        // Ill-formed: array rvalue
        CPP_assert(!can_begin<int(&&)[2]>);
        CPP_assert(!can_begin<int const(&&)[2]>);

        CPP_assert(!can_cbegin<int(&&)[2]>);
        CPP_assert(!can_cbegin<int const(&&)[2]>);
#endif // RANGES_WORKAROUND_MSVC_573728

        // Valid: only member begin
        CPP_assert(can_begin<A&>);
        CPP_assert(!can_begin<A>);
        CPP_assert(ranges::same_as<decltype(ranges::begin(std::declval<A&>())), int*>);
        CPP_assert(can_begin<const A&>);
        CPP_assert(!can_begin<const A>);
        CPP_assert(ranges::same_as<decltype(ranges::begin(std::declval<const A&>())), int const *>);

        // Valid: Both member and non-member begin, but non-member returns non-Iterator.
        CPP_assert(can_begin<B&>);
        CPP_assert(!can_begin<B>);
        CPP_assert(ranges::same_as<decltype(ranges::begin(std::declval<B&>())), int*>);
        CPP_assert(can_begin<const B&>);
        CPP_assert(!can_begin<const B>);
        CPP_assert(ranges::same_as<decltype(ranges::begin(std::declval<const B&>())), int const *>);

        // Valid: Both member and non-member begin, but non-member returns non-Iterator.
        CPP_assert(can_begin<C&>);
        CPP_assert(!can_begin<C>);
        CPP_assert(can_begin<const C&>);
        CPP_assert(!can_begin<const C>);

        // Valid: Prefer member begin
        CPP_assert(can_begin<D&>);
        CPP_assert(!can_begin<D>);
        CPP_assert(ranges::same_as<int*, decltype(ranges::begin(std::declval<D&>()))>);
        CPP_assert(can_begin<const D&>);
        CPP_assert(!can_begin<const D>);
        CPP_assert(ranges::same_as<int const *, decltype(ranges::begin(std::declval<const D&>()))>);

        {
            using T = std::initializer_list<int>;
            // Valid: begin accepts lvalue initializer_list
            CPP_assert(ranges::same_as<int const *, decltype(ranges::begin(std::declval<T&>()))>);
            CPP_assert(ranges::same_as<int const *, decltype(ranges::begin(std::declval<const T&>()))>);
            CPP_assert(!can_begin<T>);
            CPP_assert(!can_begin<T const>);
        }

        CPP_assert(can_begin<ranges::subrange<int*, int*>&>);
        CPP_assert(can_begin<const ranges::subrange<int*, int*>&>);
        CPP_assert(can_begin<ranges::subrange<int*, int*>>);
        CPP_assert(can_begin<const ranges::subrange<int*, int*>>);

        CPP_assert(can_cbegin<ranges::subrange<int*, int*>&>);
        CPP_assert(can_cbegin<const ranges::subrange<int*, int*>&>);
        CPP_assert(can_cbegin<ranges::subrange<int*, int*>>);
        CPP_assert(can_cbegin<const ranges::subrange<int*, int*>>);

        CPP_assert(can_begin<ranges::ref_view<int[5]>&>);
        CPP_assert(can_begin<const ranges::ref_view<int[5]>&>);
        CPP_assert(can_begin<ranges::ref_view<int[5]>>);
        CPP_assert(can_begin<const ranges::ref_view<int[5]>>);

        CPP_assert(can_cbegin<ranges::ref_view<int[5]>&>);
        CPP_assert(can_cbegin<const ranges::ref_view<int[5]>&>);
        CPP_assert(can_cbegin<ranges::ref_view<int[5]>>);
        CPP_assert(can_cbegin<const ranges::ref_view<int[5]>>);

        // TODO
        // CPP_assert(can_begin<ranges::iota_view<int, int>&>);
        // CPP_assert(can_begin<const ranges::iota_view<int, int>&>);
        // CPP_assert(can_begin<ranges::iota_view<int, int>>);
        // CPP_assert(can_begin<const ranges::iota_view<int, int>>);

        // CPP_assert(can_cbegin<ranges::iota_view<int, int>&>);
        // CPP_assert(can_cbegin<const ranges::iota_view<int, int>&>);
        // CPP_assert(can_cbegin<ranges::iota_view<int, int>>);
        // CPP_assert(can_cbegin<const ranges::iota_view<int, int>>);
    }
} // namespace begin_testing

namespace X
{
    template<class T, std::size_t N>
    struct array
    {
        T elements_[N];

        constexpr bool empty() const noexcept { return N == 0; }
        constexpr T* data() noexcept { return elements_; }
        constexpr T const *data() const noexcept { return elements_; }
    };

    template<class T, std::size_t N>
    constexpr T* begin(array<T, N> &a) noexcept { return a.elements_; }
    template<class T, std::size_t N>
    constexpr T* end(array<T, N> &a) noexcept { return a.elements_ + N; }
    template<class T, std::size_t N>
    constexpr T const *begin(array<T, N> const &a) noexcept { return a.elements_; }
    template<class T, std::size_t N>
    constexpr T const *end(array<T, N> const &a) noexcept { return a.elements_ + N; }
} // namespace X

using I = int*;
using CI = int const *;
CPP_assert(ranges::input_or_output_iterator<I>);
CPP_assert(ranges::input_or_output_iterator<CI>);

#if defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201603L
void test_string_view_p0970()
{
    // basic_string_views are non-dangling
    using I2 = ranges::iterator_t<std::string_view>;
    CPP_assert(ranges::same_as<I2, decltype(ranges::begin(std::declval<std::string_view>()))>);
    CPP_assert(ranges::same_as<I2, decltype(ranges::end(std::declval<std::string_view>()))>);
    CPP_assert(ranges::same_as<I2, decltype(ranges::begin(std::declval<const std::string_view>()))>);
    CPP_assert(ranges::same_as<I2, decltype(ranges::end(std::declval<const std::string_view>()))>);

    {
        const char hw[] = "Hello, World!";
        auto result = ranges::find(std::string_view{hw}, 'W');
        CPP_assert(ranges::same_as<I2, decltype(result)>);
        CHECK(result == std::string_view{hw}.begin() + 7);
    }
}
#endif

int main()
{
    using namespace ranges;

    static constexpr X::array<int, 4> some_ints = {{0,1,2,3}};
    CPP_assert(begin_testing::can_begin<X::array<int, 4> &>);
    CPP_assert(begin_testing::can_begin<X::array<int, 4> const &>);
    CPP_assert(!begin_testing::can_begin<X::array<int, 4>>);
    CPP_assert(!begin_testing::can_begin<X::array<int, 4> const>);
    CPP_assert(begin_testing::can_cbegin<X::array<int, 4> &>);
    CPP_assert(begin_testing::can_cbegin<X::array<int, 4> const &>);
    CPP_assert(!begin_testing::can_cbegin<X::array<int, 4>>);
    CPP_assert(!begin_testing::can_cbegin<X::array<int, 4> const>);

    constexpr auto first = begin(some_ints);
    constexpr auto last = end(some_ints);
    CPP_assert(ranges::same_as<const CI, decltype(first)>);
    CPP_assert(ranges::same_as<const CI, decltype(last)>);
    static_assert(first == cbegin(some_ints), "");
    static_assert(last == cend(some_ints), "");

    static_assert(noexcept(begin(some_ints)), "");
    static_assert(noexcept(end(some_ints)), "");
    static_assert(noexcept(cbegin(some_ints)), "");
    static_assert(noexcept(cend(some_ints)), "");
    static_assert(noexcept(empty(some_ints)), "");
    static_assert(noexcept(data(some_ints)), "");

    constexpr bool output = false;
    static_assert(!empty(some_ints), "");
    if(output)
        std::cout << '{';
    auto is_first = true;
    auto count = 0;
    for(auto&& i : some_ints)
    {
        CHECK(i == count++);
        if(is_first)
            is_first = false;
        else
            if(output) std::cout << ", ";
        if(output) std::cout << i;
    }
    if(output)
        std::cout << "}\n";

    test_initializer_list();
    test_array<int>(std::make_integer_sequence<int, 3>{});
    test_array<int const>(std::make_integer_sequence<int, 3>{});
    begin_testing::test();

#if defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201603L
    test_string_view_p0970();
#endif

    return ::test_result();
}
