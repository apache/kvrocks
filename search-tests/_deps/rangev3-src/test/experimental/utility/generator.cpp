// Range v3 library
//
//  Copyright Casey Carter 2017
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
#include <range/v3/detail/config.hpp>
#include <iostream>
#include <vector>
#include <range/v3/range/access.hpp>
#include <range/v3/range_for.hpp>
#include <range/v3/algorithm/copy.hpp>
#include <range/v3/algorithm/count.hpp>
#include <range/v3/algorithm/equal.hpp>
#include <range/v3/experimental/utility/generator.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/utility/swap.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/move.hpp>
#include <range/v3/view/take_exactly.hpp>
#include <range/v3/view/transform.hpp>
#include "../../simple_test.hpp"
#include "../../test_utils.hpp"

#ifdef __clang__
#pragma GCC diagnostic ignored "-Wunused-const-variable"
#endif

#if RANGES_CXX_COROUTINES < RANGES_CXX_COROUTINES_TS1
#error This test uses coroutines.
#endif

template<bool Condition>
using maybe_sized_generator = meta::if_c<Condition,
    meta::quote<ranges::experimental::sized_generator>,
    meta::quote<ranges::experimental::generator>>;

template<typename T>
constexpr bool is_copy_constructible_or_ref() noexcept
{
    return std::is_reference<T>::value ||(bool) ranges::copy_constructible<T>;
}

struct coro_fn
{
private:
    template<typename V>
    using generator_for = meta::invoke<
        maybe_sized_generator<(bool) ranges::sized_range<V>>,
        ranges::range_reference_t<V>,
        ranges::range_value_t<V>>;

    CPP_template(typename V)(
        requires ranges::input_range<V> && ranges::view_<V> &&
            (is_copy_constructible_or_ref<ranges::range_reference_t<V>>()))
    static generator_for<V> impl(V v)
    {
        if(RANGES_CONSTEXPR_IF(ranges::sized_range<V>))
            co_await static_cast<ranges::experimental::generator_size>((std::size_t)ranges::distance(v));
        auto first = ranges::begin(v);
        auto const last = ranges::end(v);
        for (; first != last; ++first)
            co_yield *first;
    }
public:
    CPP_template(typename Rng)(
        requires (
            !meta::is<ranges::uncvref_t<Rng>, ranges::experimental::generator>::value &&
            !meta::is<ranges::uncvref_t<Rng>, ranges::experimental::sized_generator>::value &&
            ranges::input_range<Rng> &&
            is_copy_constructible_or_ref<ranges::range_reference_t<Rng>>()))
    generator_for<ranges::views::all_t<Rng>> operator()(Rng &&rng) const
    {
        return impl(ranges::views::all(static_cast<Rng &&>(rng)));
    }
    template<typename R, typename V>
    ranges::experimental::generator<R, V>
    operator()(ranges::experimental::generator<R, V> g) const noexcept
    {
        return g;
    }
    template<typename R, typename V>
    ranges::experimental::sized_generator<R, V>
    operator()(ranges::experimental::sized_generator<R, V> g) const noexcept
    {
        return g;
    }
};

inline namespace function_objects
{
    RANGES_INLINE_VARIABLE(coro_fn, coro)
}

auto f(int const n)
{
    return ::coro(ranges::views::iota(0, n));
}

ranges::experimental::sized_generator<int> g(int const n)
{
    co_await static_cast<ranges::experimental::generator_size>((std::size_t) (n > 0 ? n : 0));
    for (int i = 0; i < n; ++i)
        co_yield i;
}

ranges::experimental::sized_generator<int &> h(int const n)
{
    co_await static_cast<ranges::experimental::generator_size>((std::size_t) (n > 0 ? n : 0));
    for (int i = 0; i < n; ++i)
        co_yield i;
}

CPP_template(class T)(
    requires ranges::weakly_incrementable<T>)
ranges::experimental::generator<T> iota_generator(T t)
{
    for (;; ++t)
        co_yield t;
}

CPP_template(class T, class S)(
    requires (ranges::weakly_incrementable<T> &&
        ranges::detail::weakly_equality_comparable_with_<T, S> &&
        !ranges::sized_sentinel_for<S, T> && !(ranges::integral<T> && ranges::integral<S>)))
ranges::experimental::generator<T> iota_generator(T t, S const s)
{
    for (; t != s; ++t)
        co_yield t;
}

CPP_template(class T, class S)(
    requires ranges::sized_sentinel_for<S, T> || (ranges::integral<T> && ranges::integral<S>))
ranges::experimental::sized_generator<T> iota_generator(T t, S const s)
{
    co_await static_cast<ranges::experimental::generator_size>((std::size_t) (s - t));
    for (; t != s; ++t)
        co_yield t;
}

CPP_template(class V, class F)(
    requires ranges::input_range<V> && ranges::view_<V> &&
        ranges::indirect_unary_predicate<F, ranges::iterator_t<V>>)
ranges::experimental::generator<ranges::range_reference_t<V>, ranges::range_value_t<V>>
filter(V view, F f)
{
    RANGES_FOR(auto &&i, view)
    {
        if (ranges::invoke(f, i))
            co_yield i;
    }
}

CPP_template(class V, class F)(
    requires ranges::input_range<V> && ranges::view_<V> &&
        ranges::indirectly_unary_invocable<F, ranges::iterator_t<V>>)
meta::invoke<
    maybe_sized_generator<(bool) ranges::sized_range<V>>,
    ranges::indirect_result_t<F &, ranges::iterator_t<V>>>
transform(V view, F f)
{
    if(RANGES_CONSTEXPR_IF(ranges::sized_range<V>))
        co_await static_cast<ranges::experimental::generator_size>((std::size_t) ranges::distance(view));
    RANGES_FOR(auto &&i, view)
        co_yield ranges::invoke(f, i);
}

struct MoveInt
{
    int i_;

    MoveInt(int i = 42) : i_{i}
    {}
    MoveInt(MoveInt &&that) noexcept
      : i_{ranges::exchange(that.i_, 0)}
    {}
    MoveInt &operator=(MoveInt &&that) noexcept
    {
        i_ = ranges::exchange(that.i_, 0);
        return *this;
    }

    friend bool operator==(MoveInt const &x, MoveInt const &y)
    {
        return x.i_ == y.i_;
    }
    friend bool operator!=(MoveInt const &x, MoveInt const &y)
    {
        return !(x == y);
    }

    friend std::ostream &operator<<(std::ostream &os, MoveInt const &mi)
    {
        return os << mi.i_;
    }
};

int main()
{
    using namespace ranges;

    auto even = [](int i){ return i % 2 == 0; };

#ifndef RANGES_WORKAROUND_MSVC_835948
    {
        auto rng = ::iota_generator(0, 10);
        CPP_assert(sized_range<decltype(rng)>);
        CHECK(size(rng) == 10u);
        ::check_equal(rng, {0,1,2,3,4,5,6,7,8,9});
    }
    {
        auto rng = ::coro(::coro(::coro(::iota_generator(0, 10))));
        ::has_type<decltype(::iota_generator(0, 10)) &>(rng);
        CPP_assert(sized_range<decltype(rng)>);
        CHECK(size(rng) == 10u);
        ::check_equal(rng, {0,1,2,3,4,5,6,7,8,9});
    }
    {
        auto rng = ::coro(views::ints | views::filter(even) | views::take_exactly(10));
        CPP_assert(sized_range<decltype(rng)>);
        CHECK(size(rng) == 10u);
        ::check_equal(rng, {0,2,4,6,8,10,12,14,16,18});
    }
    {
        auto const control = {1, 2, 3};
        MoveInt a[] = {{1}, {2}, {3}};
        MoveInt b[3];
        CHECK(equal(a, control, std::equal_to<int>{}, &MoveInt::i_));
        CHECK(count(b, 42, &MoveInt::i_) == 3);
        auto rng = ::coro(views::move(a));
        CPP_assert(sized_range<decltype(rng)>);
        CHECK(size(rng) == 3u);
        copy(rng, b);
        CHECK(equal(b, control, std::equal_to<int>{}, &MoveInt::i_));
        CHECK(count(a, 0, &MoveInt::i_) == 3);
    }
    {
        int some_ints[] = {0,1,2};
        auto rng = ::coro(some_ints);
        CPP_assert(sized_range<decltype(rng)>);
        CHECK(size(rng) == 3u);
        auto i = begin(rng);
        auto e = end(rng);
        CHECK(i != e);
        CHECK(&*i == &some_ints[0]);
        ++i;
        CHECK(i != e);
        CHECK(&*i == &some_ints[1]);
        ++i;
        CHECK(i != e);
        CHECK(&*i == &some_ints[2]);
        ++i;
        CHECK(i == e);
    }
    {
        std::vector<bool> vec(3, false);
        auto rng = ::coro(vec);
        CPP_assert(sized_range<decltype(rng)>);
        CHECK(size(rng) == 3u);
        ::check_equal(rng, {false,false,false});
    }

    ::check_equal(f(42), g(42));
    ::check_equal(f(42), h(42));

    {
        auto rng = h(20) | views::transform([](int &x) { return ++x; });
        ::check_equal(rng, {1,3,5,7,9,11,13,15,17,19});
    }

    {
        auto rng = f(20) | views::filter(even);
        ::check_equal(rng, {0,2,4,6,8,10,12,14,16,18});
    }
#endif // RANGES_WORKAROUND_MSVC_835948

    {
        auto square = [](int i) { return i * i; };

        int const some_ints[] = {0,1,2,3,4,5,6,7};
        auto rng = ::transform(::filter(debug_input_view<int const>{some_ints}, even), square);
        ::check_equal(rng, {0,4,16,36});
    }

    return ::test_result();
}
