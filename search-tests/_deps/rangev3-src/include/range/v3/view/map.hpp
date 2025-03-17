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

#ifndef RANGES_V3_VIEW_MAP_HPP
#define RANGES_V3_VIEW_MAP_HPP

#include <utility>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

// TODO: Reuse subrange's pair_like concept here and have get_first and get_second
// dispatch through get<>()

namespace ranges
{
    /// \cond
    namespace detail
    {
        template<typename T>
        constexpr T & get_first_second_helper(T & t, std::true_type) noexcept
        {
            return t;
        }

        template(typename T)(
            requires move_constructible<T>)
        constexpr T get_first_second_helper(T & t, std::false_type) //
            noexcept(std::is_nothrow_move_constructible<T>::value)
        {
            return std::move(t);
        }

        template<typename P, typename E>
        using get_first_second_tag = meta::bool_<std::is_lvalue_reference<P>::value ||
                                                 std::is_lvalue_reference<E>::value>;

        struct get_first
        {
            // clang-format off
            template<typename Pair>
            constexpr auto CPP_auto_fun(operator())(Pair &&p)(const)
            (
                return get_first_second_helper(
                    p.first,
                    get_first_second_tag<Pair, decltype(p.first)>{})
            )
            // clang-format on
        };

        struct get_second
        {
            // clang-format off
            template<typename Pair>
            constexpr auto CPP_auto_fun(operator())(Pair &&p)(const)
            (
                return get_first_second_helper(
                    p.second,
                    get_first_second_tag<Pair, decltype(p.second)>{})
            )
            // clang-format on
        };

        // clang-format off
        /// \concept kv_pair_like_
        /// \brief The \c kv_pair_like_ concept
        template<typename T>
        CPP_concept kv_pair_like_ =
            invocable<get_first const &, T> &&
            invocable<get_second const &, T>;
        // clang-format on
    } // namespace detail
    /// \endcond

    /// \addtogroup group-views
    /// @{
    namespace views
    {
        struct keys_fn
        {
            template(typename Rng)(
                requires viewable_range<Rng> AND input_range<Rng> AND
                    detail::kv_pair_like_<range_reference_t<Rng>>)
            keys_range_view<all_t<Rng>> operator()(Rng && rng) const
            {
                return {all(static_cast<Rng &&>(rng)), detail::get_first{}};
            }
        };

        struct values_fn
        {
            template(typename Rng)(
                requires viewable_range<Rng> AND input_range<Rng> AND
                    detail::kv_pair_like_<range_reference_t<Rng>>)
            values_view<all_t<Rng>> operator()(Rng && rng) const
            {
                return {all(static_cast<Rng &&>(rng)), detail::get_second{}};
            }
        };

        /// \relates keys_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(view_closure<keys_fn>, keys)

        /// \relates values_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(view_closure<values_fn>, values)
    } // namespace views

    template<typename Rng>
    RANGES_INLINE_VAR constexpr bool enable_borrowed_range<keys_range_view<Rng>> =
        enable_borrowed_range<Rng>;
    template<typename Rng>
    RANGES_INLINE_VAR constexpr bool enable_borrowed_range<values_view<Rng>> =
        enable_borrowed_range<Rng>;

    namespace cpp20
    {
        namespace views
        {
            using ranges::views::keys;
            using ranges::views::values;
        } // namespace views
        // TODO(@cjdb): provide implementation for elements_view
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
