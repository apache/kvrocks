/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2013-present
//  Copyright Casey Carter 2016
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_UTILITY_COMPRESSED_PAIR_HPP
#define RANGES_V3_UTILITY_COMPRESSED_PAIR_HPP

#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/utility/box.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace compressed_tuple_detail
    {
        // tagging individual elements with the complete type list disambiguates
        // base classes when composing compressed_tuples recursively.
        template<typename T, std::size_t I, typename... Ts>
        using storage = box<T, meta::list<meta::size_t<I>, Ts...>>;

        template<typename List, typename Indices>
        struct compressed_tuple_;
        template<typename... Ts, std::size_t... Is>
        struct RANGES_EMPTY_BASES
            compressed_tuple_<meta::list<Ts...>, meta::index_sequence<Is...>>
          : storage<Ts, Is, Ts...>...
        {
            static_assert(same_as<meta::index_sequence<Is...>,
                                  meta::make_index_sequence<sizeof...(Is)>>,
                          "What madness is this?!?");

            compressed_tuple_() = default;

            template<typename... Args,
                     meta::if_<meta::and_c<META_IS_CONSTRUCTIBLE(Ts, Args)...>, int> = 0>
            constexpr compressed_tuple_(Args &&... args) noexcept(
                meta::strict_and<std::is_nothrow_constructible<storage<Ts, Is, Ts...>,
                                                               Args>...>::value)
              : storage<Ts, Is, Ts...>{static_cast<Args &&>(args)}...
            {}

            template<
                typename... Us,
                meta::if_<meta::and_c<META_IS_CONSTRUCTIBLE(Us, Ts const &)...>, int> = 0>
            constexpr operator std::tuple<Us...>() const noexcept(
                meta::strict_and<std::is_nothrow_constructible<Us, Ts const &>...>::value)
            {
                return std::tuple<Us...>{get<Is>(*this)...};
            }

            template<std::size_t I, typename T = meta::at_c<meta::list<Ts...>, I>>
            friend constexpr T & get(compressed_tuple_ & tuple) noexcept
            {
                return static_cast<storage<T, I, Ts...> &>(tuple).get();
            }
            template<std::size_t I, typename T = meta::at_c<meta::list<Ts...>, I>>
            friend constexpr T const & get(compressed_tuple_ const & tuple) noexcept
            {
                return static_cast<storage<T, I, Ts...> const &>(tuple).get();
            }
            template<std::size_t I, typename T = meta::at_c<meta::list<Ts...>, I>>
            friend constexpr T && get(compressed_tuple_ && tuple) noexcept
            {
                return static_cast<storage<T, I, Ts...> &&>(tuple).get();
            }
            template<std::size_t I, typename T = meta::at_c<meta::list<Ts...>, I>>
            friend constexpr T const && get(compressed_tuple_ const && tuple) noexcept
            {
                return static_cast<storage<T, I, Ts...> const &&>(tuple).get();
            }
        };

        template<typename... Ts>
        using compressed_tuple RANGES_DEPRECATED(
            "ranges::compressed_tuple is deprecated.") =
            compressed_tuple_<meta::list<Ts...>,
                              meta::make_index_sequence<sizeof...(Ts)>>;
    } // namespace compressed_tuple_detail
    /// \endcond

    using compressed_tuple_detail::compressed_tuple;

    struct make_compressed_tuple_fn
    {
        // clang-format off
        template<typename... Args>
        constexpr auto CPP_auto_fun(operator())(Args &&... args) (const)
        (
            return compressed_tuple<bind_element_t<Args>...>{
                static_cast<Args &&>(args)...}
        )
        // clang-format on
    };

    /// \ingroup group-utility
    /// \sa `make_compressed_tuple_fn`
    RANGES_INLINE_VARIABLE(make_compressed_tuple_fn, make_compressed_tuple)

    template<typename First, typename Second>
    struct RANGES_EMPTY_BASES compressed_pair
      : box<First, meta::size_t<0>>
      , box<Second, meta::size_t<1>>
    {
        using first_type = First;
        using second_type = Second;

        compressed_pair() = default;

        template(typename U, typename V)(
            requires constructible_from<First, U> AND constructible_from<Second, V>)
        constexpr compressed_pair(U && u, V && v) //
            noexcept(noexcept(First((U &&) u)) && noexcept(Second((V &&) v)))
          : box<First, meta::size_t<0>>{(U &&) u}
          , box<Second, meta::size_t<1>>{(V &&) v}
        {}

        constexpr First & first() &
        {
            return this->box<First, meta::size_t<0>>::get();
        }
        constexpr First const & first() const &
        {
            return this->box<First, meta::size_t<0>>::get();
        }
        constexpr First && first() &&
        {
            return static_cast<First &&>(this->box<First, meta::size_t<0>>::get());
        }

        constexpr Second & second() &
        {
            return this->box<Second, meta::size_t<1>>::get();
        }
        constexpr Second const & second() const &
        {
            return this->box<Second, meta::size_t<1>>::get();
        }
        constexpr Second && second() &&
        {
            return static_cast<Second &&>(this->box<Second, meta::size_t<1>>::get());
        }

        template(typename F, typename S)(
            requires convertible_to<First const &, F> AND
                convertible_to<Second const &, S>)
            constexpr
            operator std::pair<F, S>() const
        {
            return std::pair<F, S>{first(), second()};
        }
    };

    struct make_compressed_pair_fn
    {
        // clang-format off
        template<typename First, typename Second>
        constexpr auto CPP_auto_fun(operator())(First &&f, Second &&s) (const)
        (
            return compressed_pair<bind_element_t<First>, bind_element_t<Second>>{
                static_cast<First &&>(f), static_cast<Second &&>(s)
            }
        )
        // clang-format on
    };

    /// \ingroup group-utility
    /// \sa `make_compressed_pair_fn`
    RANGES_INLINE_VARIABLE(make_compressed_pair_fn, make_compressed_pair)
} // namespace ranges

RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_MISMATCHED_TAGS
namespace std
{
    template<typename... Ts, size_t... Is>
    struct tuple_size<::ranges::compressed_tuple_detail::compressed_tuple_<
        ::meta::list<Ts...>, ::meta::index_sequence<Is...>>>
      : integral_constant<size_t, sizeof...(Ts)>
    {};

    template<size_t I, typename... Ts, size_t... Is>
    struct tuple_element<I, ::ranges::compressed_tuple_detail::compressed_tuple_<
                                ::meta::list<Ts...>, ::meta::index_sequence<Is...>>>
    {
        using type = ::meta::at_c<::meta::list<Ts...>, I>;
    };

    template<typename First, typename Second>
    struct tuple_size<::ranges::compressed_pair<First, Second>>
      : integral_constant<size_t, 2>
    {};

    template<typename First, typename Second>
    struct tuple_element<0, ::ranges::compressed_pair<First, Second>>
    {
        using type = First;
    };

    template<typename First, typename Second>
    struct tuple_element<1, ::ranges::compressed_pair<First, Second>>
    {
        using type = Second;
    };
} // namespace std
RANGES_DIAGNOSTIC_POP

#include <range/v3/detail/epilogue.hpp>

#endif
