/// \file
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
//
#ifndef RANGES_V3_VIEW_FACADE_HPP
#define RANGES_V3_VIEW_FACADE_HPP

#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/basic_iterator.hpp>
#include <range/v3/iterator/default_sentinel.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/view/interface.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
        template<typename Derived>
        using begin_cursor_t = detail::decay_t<decltype(
            range_access::begin_cursor(std::declval<Derived &>()))>;

        template<typename Derived>
        using end_cursor_t = detail::decay_t<decltype(
            range_access::end_cursor(std::declval<Derived &>()))>;

        template<typename Derived>
        using facade_iterator_t = basic_iterator<begin_cursor_t<Derived>>;

        template<typename Derived>
        using facade_sentinel_t =
            meta::if_c<same_as<begin_cursor_t<Derived>, end_cursor_t<Derived>>,
                       facade_iterator_t<Derived>, end_cursor_t<Derived>>;
    } // namespace detail
    /// \endcond

    /// \addtogroup group-views
    /// @{

    /// \brief A utility for constructing a view from a (derived) type that
    /// implements begin and end cursors.
    /// \tparam Derived A type that derives from `view_facade` and implements
    /// begin and end cursors. This type is permitted to be incomplete.
    /// \tparam Cardinality The cardinality of this view: `finite`, `infinite`,
    /// or `unknown`. See `ranges::cardinality`.
    template<typename Derived, cardinality Cardinality>
    struct view_facade : view_interface<Derived, Cardinality>
    {
    protected:
        friend range_access;
        struct view_as_cursor : Derived
        {
            view_as_cursor() = default;
            explicit view_as_cursor(Derived const * derived)
              : Derived(*derived)
            {}
            explicit operator bool() = delete;
            explicit operator bool() const = delete;
        };
        // Default implementations
        constexpr view_as_cursor begin_cursor() const
        {
            return view_as_cursor{static_cast<Derived const *>(this)};
        }
        constexpr default_sentinel_t end_cursor() const
        {
            return {};
        }

    public:
        /// Let `d` be `static_cast<Derived &>(*this)`. Let `b` be
        /// `std::as_const(d).begin_cursor()` if that expression is well-formed;
        /// otherwise, let `b` be `d.begin_cursor()`. Let `B` be the type of
        /// `b`.
        /// \return `ranges::basic_iterator<B>(b)`
        template(typename D = Derived)(
            requires same_as<D, Derived>)
        constexpr auto begin() -> detail::facade_iterator_t<D>
        {
            return detail::facade_iterator_t<D>{
                range_access::begin_cursor(*static_cast<Derived *>(this))};
        }
        /// \overload
        template(typename D = Derived)(
            requires same_as<D, Derived>)
        constexpr auto begin() const -> detail::facade_iterator_t<D const>
        {
            return detail::facade_iterator_t<D const>{
                range_access::begin_cursor(*static_cast<Derived const *>(this))};
        }
        /// Let `d` be `static_cast<Derived &>(*this)`. Let `e` be
        /// `std::as_const(d).end_cursor()` if that expression is well-formed;
        /// otherwise, let `e` be `d.end_cursor()`. Let `E` be the type of
        /// `e`.
        /// \return `ranges::basic_iterator<E>(e)` if `E` is the same
        /// as `B` computed above for `begin()`; otherwise, return `e`.
        template(typename D = Derived)(
            requires same_as<D, Derived>)
        constexpr auto end() -> detail::facade_sentinel_t<D>
        {
            return static_cast<detail::facade_sentinel_t<D>>(
                range_access::end_cursor(*static_cast<Derived *>(this)));
        }
        /// \overload
        template(typename D = Derived)(
            requires same_as<D, Derived>)
        constexpr auto end() const -> detail::facade_sentinel_t<D const>
        {
            return static_cast<detail::facade_sentinel_t<D const>>(
                range_access::end_cursor(*static_cast<Derived const *>(this)));
        }
    };

    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
