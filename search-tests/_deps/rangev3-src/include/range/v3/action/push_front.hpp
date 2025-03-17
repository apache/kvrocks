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

#ifndef RANGES_V3_ACTION_PUSH_FRONT_HPP
#define RANGES_V3_ACTION_PUSH_FRONT_HPP

#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/action/action.hpp>
#include <range/v3/action/insert.hpp>
#include <range/v3/detail/with_braced_init_args.hpp>
#include <range/v3/functional/bind_back.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-actions
    /// @{

    /// \cond
    namespace adl_push_front_detail
    {
        /// \endcond
        template<typename Cont, typename T>
        using push_front_t = decltype(static_cast<void>(
            unwrap_reference(std::declval<Cont &>()).push_front(std::declval<T>())));

        template<typename Cont, typename Rng>
        using insert_t = decltype(static_cast<void>(
            ranges::insert(std::declval<Cont &>(), std::declval<iterator_t<Cont>>(),
                           std::declval<Rng>())));

        template(typename Cont, typename T)(
            requires lvalue_container_like<Cont> AND
                (!range<T>) AND constructible_from<range_value_t<Cont>, T>)
        push_front_t<Cont, T> push_front(Cont && cont, T && t)
        {
            unwrap_reference(cont).push_front(static_cast<T &&>(t));
        }

        template(typename Cont, typename Rng)(
            requires lvalue_container_like<Cont> AND range<Rng>)
        insert_t<Cont, Rng> push_front(Cont && cont, Rng && rng)
        {
            ranges::insert(cont, begin(cont), static_cast<Rng &&>(rng));
        }

        /// \cond
        // clang-format off
        /// \concept can_push_front_frag_
        /// \brief The \c can_push_front_frag_ concept
        template<typename Rng, typename T>
        CPP_requires(can_push_front_frag_,
            requires(Rng && rng, T && t) //
            (
                push_front(rng, (T &&) t)
            ));
        /// \concept can_push_front_
        /// \brief The \c can_push_front_ concept
        template<typename Rng, typename T>
        CPP_concept can_push_front_ =
            CPP_requires_ref(adl_push_front_detail::can_push_front_frag_, Rng, T);
        // clang-format on
        /// \endcond

        struct push_front_fn
        {
            template<typename T>
            constexpr auto operator()(T && val) const
            {
                return make_action_closure(
                    bind_back(push_front_fn{}, static_cast<T &&>(val)));
            }

            template<typename T>
            constexpr auto operator()(std::initializer_list<T> val) const
            {
                return make_action_closure(bind_back(push_front_fn{}, val));
            }

            template(typename T)(
                requires range<T &>)
            constexpr auto operator()(T & t) const
            {
                return make_action_closure(
                    bind_back(push_front_fn{}, detail::reference_wrapper_<T>(t)));
            }

            template(typename Rng, typename T)(
                requires input_range<Rng> AND can_push_front_<Rng, T> AND
                    (range<T> || constructible_from<range_value_t<Rng>, T>)) //
            Rng operator()(Rng && rng, T && t) const //
            {
                push_front(rng, static_cast<T &&>(t));
                return static_cast<Rng &&>(rng);
            }

            template(typename Rng, typename T)(
                requires input_range<Rng> AND
                    can_push_front_<Rng, std::initializer_list<T>> AND
                    constructible_from<range_value_t<Rng>, T const &>)
            Rng operator()(Rng && rng, std::initializer_list<T> t) const //
            {
                push_front(rng, t);
                return static_cast<Rng &&>(rng);
            }

            /// \cond
            template<typename Rng, typename T>
            invoke_result_t<push_front_fn, Rng, T &> //
            operator()(Rng && rng, detail::reference_wrapper_<T> r) const
            {
                return (*this)(static_cast<Rng &&>(rng), r.get());
            }
            /// \endcond
        };
    /// \cond
    } // namespace adl_push_front_detail
    /// \endcond

    namespace actions
    {
        RANGES_INLINE_VARIABLE(adl_push_front_detail::push_front_fn, push_front)
    } // namespace actions

    using actions::push_front;

    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
