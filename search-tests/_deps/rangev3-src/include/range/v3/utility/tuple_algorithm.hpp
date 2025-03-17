/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2013-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_UTILITY_TUPLE_ALGORITHM_HPP
#define RANGES_V3_UTILITY_TUPLE_ALGORITHM_HPP

#include <initializer_list>
#include <tuple>
#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/detail/adl_get.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-utility
    /// @{
    template<typename Tup>
    using tuple_indices_t = meta::make_index_sequence<
        std::tuple_size<typename std::remove_reference<Tup>::type>::value>;

    struct tuple_apply_fn
    {
        // clang-format off
    private:
        template<typename Fun, typename Tup, std::size_t... Is>
        static constexpr auto //
        CPP_auto_fun(impl)(Fun &&fun, Tup &&tup, meta::index_sequence<Is...>)
        (
            return invoke(static_cast<Fun &&>(fun),
                          detail::adl_get<Is>(static_cast<Tup &&>(tup))...)
        )
    public:
        template<typename Fun, typename Tup>
        constexpr auto CPP_auto_fun(operator())(Fun &&fun, Tup &&tup)(const)
        (
            return tuple_apply_fn::impl(static_cast<Fun &&>(fun),
                                        static_cast<Tup &&>(tup),
                                        tuple_indices_t<Tup>{})
        )
        // clang-format on
    };

    /// \ingroup group-utility
    /// \sa `tuple_apply_fn`
    RANGES_INLINE_VARIABLE(tuple_apply_fn, tuple_apply)

    struct tuple_transform_fn
    {
        // clang-format off
    private:
        template<typename Tup, typename Fun, std::size_t... Is>
        static constexpr auto //
        CPP_auto_fun(impl1)(Tup &&tup, Fun &fun, meta::index_sequence<Is...>)
        (
            return std::tuple<
                decltype(fun(detail::adl_get<Is>(static_cast<Tup &&>(tup))))...>{
                fun(detail::adl_get<Is>(static_cast<Tup &&>(
                    tup)))...}
        )
        template<typename Tup0, typename Tup1, typename Fun, std::size_t... Is>
        static constexpr auto CPP_auto_fun(impl2)(Tup0 &&tup0, Tup1 &&tup1, Fun &fun,
                                        meta::index_sequence<Is...>)
        (
            return std::tuple<
                decltype(fun(detail::adl_get<Is>(static_cast<Tup0 &&>(tup0)),
                             detail::adl_get<Is>(static_cast<Tup1 &&>(tup1))))...>{
                fun(detail::adl_get<Is>(static_cast<Tup0 &&>(tup0)),
                    detail::adl_get<Is>(static_cast<Tup1 &&>(tup1)))...}
        )
    public:
        template<typename Tup, typename Fun>
        constexpr auto CPP_auto_fun(operator())(Tup &&tup, Fun fun)(const)
        (
            return tuple_transform_fn::impl1(
                static_cast<Tup &&>(tup), fun,
                tuple_indices_t<Tup>{})
        )
        template<typename Tup0, typename Tup1, typename Fun>
        constexpr auto CPP_auto_fun(operator())(Tup0 &&tup0, Tup1 &&tup1, Fun fun)(const)
        (
            return tuple_transform_fn::impl2(static_cast<Tup0 &&>(tup0),
                                             static_cast<Tup1 &&>(tup1), fun,
                                             tuple_indices_t<Tup0>{})
        )
        // clang-format on
    };

    /// \ingroup group-utility
    /// \sa `tuple_transform_fn`
    RANGES_INLINE_VARIABLE(tuple_transform_fn, tuple_transform)

    struct tuple_foldl_fn
    {
    private:
        template<typename Tup, typename Val, typename Fun>
        static constexpr Val impl(Tup &&, Val val, Fun &)
        {
            return val;
        }
        // clang-format off
        template<std::size_t I0, std::size_t... Is, typename Tup, typename Val,
                 typename Fun, typename Impl = tuple_foldl_fn>
        static constexpr auto CPP_auto_fun(impl)(Tup &&tup, Val val, Fun &fun)
        (
            return Impl::template impl<Is...>(
                static_cast<Tup &&>(tup),
                fun(std::move(val), detail::adl_get<I0>(static_cast<Tup &&>(tup))),
                fun)
        )
        template<typename Tup, typename Val, typename Fun, std::size_t... Is>
        static constexpr auto CPP_auto_fun(impl2)(Tup &&tup, Val val, Fun &fun,
                                        meta::index_sequence<Is...>)
        (
            return tuple_foldl_fn::impl<Is...>(static_cast<Tup &&>(tup),
                                               std::move(val),
                                               fun)
        )
    public:
        template<typename Tup, typename Val, typename Fun>
        constexpr auto CPP_auto_fun(operator())(Tup &&tup, Val val, Fun fun)(const)
        (
            return tuple_foldl_fn::impl2(static_cast<Tup &&>(tup),
                                         std::move(val),
                                         fun,
                                         tuple_indices_t<Tup>{})
        )
        // clang-format on
    };

    /// \ingroup group-utility
    /// \sa `tuple_foldl_fn`
    RANGES_INLINE_VARIABLE(tuple_foldl_fn, tuple_foldl)

    struct tuple_for_each_fn
    {
    private:
        template<typename Tup, typename Fun, std::size_t... Is>
        static constexpr void impl(Tup && tup, Fun & fun, meta::index_sequence<Is...>)
        {
            (void)std::initializer_list<int>{
                ((void)fun(detail::adl_get<Is>(static_cast<Tup &&>(tup))), 42)...};
        }

    public:
        template<typename Tup, typename Fun>
        constexpr Fun operator()(Tup && tup, Fun fun) const
        {
            return tuple_for_each_fn::impl(
                       static_cast<Tup &&>(tup), fun, tuple_indices_t<Tup>{}),
                   fun;
        }
    };

    /// \ingroup group-utility
    /// \sa `tuple_for_each_fn`
    RANGES_INLINE_VARIABLE(tuple_for_each_fn, tuple_for_each)

    struct make_tuple_fn
    {
        // clang-format off
        template<typename... Ts>
        constexpr auto CPP_auto_fun(operator())(Ts &&... ts)(const)
        (
            return std::make_tuple(static_cast<Ts &&>(ts)...)
        )
        // clang-format on
    };

    /// \ingroup group-utility
    /// \sa `make_tuple_fn`
    RANGES_INLINE_VARIABLE(make_tuple_fn, make_tuple)
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
