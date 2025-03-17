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

#ifndef RANGES_V3_DETAIL_BIND_BACK_HPP
#define RANGES_V3_DETAIL_BIND_BACK_HPP

#include <tuple>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/invoke.hpp>
#include <range/v3/utility/tuple_algorithm.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    // bind_back like std::bind_front has no special treatment for nested
    // bind-expressions or reference_wrappers; there is no need to wrap
    // Callables with ranges::protect.
    namespace detail
    {
        template<typename Fn, typename... Args>
        struct bind_back_fn_
        {
            using tuple_t = std::tuple<Fn, Args...>;
            tuple_t fn_args_;

            template<typename... CallArgs>
            constexpr invoke_result_t<Fn, CallArgs..., Args...> //
            operator()(CallArgs &&... cargs) &&
                noexcept(is_nothrow_invocable_v<Fn, CallArgs..., Args...>)
            {
                return tuple_apply(
                    [&](auto && fn, auto &&... args) -> decltype(auto) {
                        return invoke((decltype(fn))fn,
                                      (CallArgs &&) cargs...,
                                      (decltype(args))args...);
                    },
                    (std::tuple<Fn, Args...> &&) fn_args_);
            }

            /// \overload
            template<typename... CallArgs>
            constexpr invoke_result_t<Fn &, CallArgs..., Args &...>
            operator()(CallArgs &&... cargs) &
                noexcept(is_nothrow_invocable_v<Fn &, CallArgs..., Args &...>)
            {
                return tuple_apply(
                    [&](auto & fn, auto &... args) -> decltype(auto) {
                        return invoke(fn, (CallArgs &&) cargs..., args...);
                    },
                    fn_args_);
            }

            /// \overload
            template<typename... CallArgs>
            constexpr invoke_result_t<Fn const &, CallArgs..., Args const &...>
            operator()(CallArgs &&... cargs) const & //
                noexcept(is_nothrow_invocable_v<Fn const &, CallArgs..., Args const &...>)
            {
                return tuple_apply(
                    [&](auto & fn, auto &... args) -> decltype(auto) {
                        return invoke(fn, (CallArgs &&) cargs..., args...);
                    },
                    fn_args_);
            }
        };

        /// \cond
        // Unroll a few instantiations to avoid a heavy-weight tuple instantiation
        template<typename Fn, typename Arg>
        struct bind_back_fn_<Fn, Arg>
        {
            struct tuple_t
            {
                Fn fn_;
                Arg arg_;
            };
            tuple_t fn_args_;

            template<typename... CallArgs>
            constexpr invoke_result_t<Fn, CallArgs..., Arg> //
            operator()(CallArgs &&... cargs) && //
                noexcept(is_nothrow_invocable_v<Fn, CallArgs..., Arg>)
            {
                return invoke(
                    (Fn &&) fn_args_.fn_, (CallArgs &&) cargs..., (Arg &&) fn_args_.arg_);
            }

            template<typename... CallArgs>
            constexpr invoke_result_t<Fn &, CallArgs..., Arg &> //
            operator()(CallArgs &&... cargs) & //
                noexcept(is_nothrow_invocable_v<Fn &, CallArgs..., Arg &>)
            {
                return invoke(fn_args_.fn_, (CallArgs &&) cargs..., fn_args_.arg_);
            }

            template<typename... CallArgs>
            constexpr invoke_result_t<Fn const &, CallArgs..., Arg const &> //
            operator()(CallArgs &&... cargs) const & //
                noexcept(is_nothrow_invocable_v<Fn const &, CallArgs..., Arg const &>)
            {
                return invoke(fn_args_.fn_, (CallArgs &&) cargs..., fn_args_.arg_);
            }
        };

        template<typename Fn, typename Arg0, typename Arg1>
        struct bind_back_fn_<Fn, Arg0, Arg1>
        {
            struct tuple_t
            {
                Fn fn_;
                Arg0 arg0_;
                Arg1 arg1_;
            };
            tuple_t fn_args_;

            template<typename... CallArgs>
            constexpr invoke_result_t<Fn, CallArgs..., Arg0, Arg1> //
            operator()(CallArgs &&... cargs) && //
                noexcept(is_nothrow_invocable_v<Fn, CallArgs..., Arg0, Arg1>)
            {
                return invoke((Fn &&) fn_args_.fn_,
                              (CallArgs &&) cargs...,
                              (Arg0 &&) fn_args_.arg0_,
                              (Arg1 &&) fn_args_.arg1_);
            }

            template<typename... CallArgs>
            constexpr invoke_result_t<Fn &, CallArgs..., Arg0 &, Arg1 &> //
            operator()(CallArgs &&... cargs) & //
                noexcept(is_nothrow_invocable_v<Fn &, CallArgs..., Arg0 &, Arg1 &>)
            {
                return invoke(
                    fn_args_.fn_, (CallArgs &&) cargs..., fn_args_.arg0_, fn_args_.arg1_);
            }

            template<typename... CallArgs>
            constexpr invoke_result_t<Fn const &, CallArgs..., Arg0 const &, Arg1 const &>
            operator()(CallArgs &&... cargs) const & 
                noexcept(is_nothrow_invocable_v<Fn const &,
                                                CallArgs...,
                                                Arg0 const &,
                                                Arg1 const &>)
            {
                return invoke(
                    fn_args_.fn_, (CallArgs &&) cargs..., fn_args_.arg0_, fn_args_.arg1_);
            }
        };
        /// \endcond

        template<typename Fn, typename... Args>
        using bind_back_fn = bind_back_fn_<decay_t<Fn>, decay_t<Args>...>;
    } // namespace detail

    struct bind_back_fn
    {
        template<typename Fn, typename Arg1, typename... Args>
        constexpr detail::bind_back_fn<Fn, Arg1, Args...> //
        operator()(Fn && fn, Arg1 && arg1, Args &&... args) const
        {
#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ <= 5
            using T = typename detail::bind_back_fn<Fn, Arg1, Args...>::tuple_t;
            return {T{(Fn &&) fn, (Arg1 &&) arg1, (Args &&) args...}};
#else
            return {{(Fn &&) fn, (Arg1 &&) arg1, (Args &&) args...}};
#endif
        }
    };

    /// \ingroup group-utility
    /// \sa `bind_back_fn`
    RANGES_INLINE_VARIABLE(bind_back_fn, bind_back)

} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif // RANGES_V3_DETAIL_BIND_BACK_HPP
