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
#ifndef RANGES_V3_VIEW_COMMON_HPP
#define RANGES_V3_VIEW_COMMON_HPP

#include <type_traits>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/common_iterator.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/interface.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{

    /// \cond
    namespace detail
    {
        // clang-format off
        /// \concept random_access_and_sized_range
        /// \brief The \c random_access_and_sized_range concept
        template<typename R>
        CPP_concept random_access_and_sized_range =
            random_access_range<R> && sized_range<R>;
        // clang-format on

        template<typename R>
        using common_view_iterator_t =
            meta::if_c<random_access_and_sized_range<R>, iterator_t<R>,
                       common_iterator_t<iterator_t<R>, sentinel_t<R>>>;

        template<typename Rng>
        struct is_common_range : meta::bool_<common_range<Rng>>
        {};
    } // namespace detail
    /// \endcond

    template<typename Rng, bool = detail::is_common_range<Rng>::value>
    struct common_view : view_interface<common_view<Rng>, range_cardinality<Rng>::value>
    {
    private:
        CPP_assert(view_<Rng>);
        CPP_assert(!(common_range<Rng> && view_<Rng>));
        Rng rng_;

        sentinel_t<Rng> end_(std::false_type)
        {
            return ranges::end(rng_);
        }
        iterator_t<Rng> end_(std::true_type)
        {
            return ranges::begin(rng_) + ranges::distance(rng_);
        }
        template(bool Const = true)(
            requires Const AND range<meta::const_if_c<Const, Rng>>)
        sentinel_t<meta::const_if_c<Const, Rng>> end_(std::false_type) const
        {
            return ranges::end(rng_);
        }
        template(bool Const = true)(
            requires Const AND range<meta::const_if_c<Const, Rng>>)
        iterator_t<meta::const_if_c<Const, Rng>> end_(std::true_type) const
        {
            return ranges::begin(rng_) + ranges::distance(rng_);
        }

    public:
        common_view() = default;
        explicit common_view(Rng rng)
          : rng_(detail::move(rng))
        {}
        Rng base() const
        {
            return rng_;
        }

        detail::common_view_iterator_t<Rng> begin()
        {
            return detail::common_view_iterator_t<Rng>{ranges::begin(rng_)};
        }
        detail::common_view_iterator_t<Rng> end()
        {
            return detail::common_view_iterator_t<Rng>{
                end_(meta::bool_<detail::random_access_and_sized_range<Rng>>{})};
        }
        CPP_auto_member
        auto CPP_fun(size)()(
            requires sized_range<Rng>)
        {
            return ranges::size(rng_);
        }

        template(bool Const = true)(
            requires range<meta::const_if_c<Const, Rng>>)
        auto begin() const
            -> detail::common_view_iterator_t<meta::const_if_c<Const, Rng>>
        {
            return detail::common_view_iterator_t<meta::const_if_c<Const, Rng>>{
                ranges::begin(rng_)};
        }
        template(bool Const = true)(
            requires range<meta::const_if_c<Const, Rng>>)
        auto end() const
            -> detail::common_view_iterator_t<meta::const_if_c<Const, Rng>>
        {
            return detail::common_view_iterator_t<meta::const_if_c<Const, Rng>>{
                end_(meta::bool_<detail::random_access_and_sized_range<
                         meta::const_if_c<Const, Rng>>>{})};
        }
        CPP_auto_member
        auto CPP_fun(size)()(const
            requires sized_range<Rng const>)
        {
            return ranges::size(rng_);
        }
    };

    template<typename Rng, bool B>
    RANGES_INLINE_VAR constexpr bool enable_borrowed_range<common_view<Rng, B>> = //
        enable_borrowed_range<Rng>;

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template(typename Rng)(
        requires (!common_range<Rng>)) //
        common_view(Rng &&)
            ->common_view<views::all_t<Rng>>;
#endif

    template<typename Rng>
    struct common_view<Rng, true> : identity_adaptor<Rng>
    {
        CPP_assert(common_range<Rng>);
        using identity_adaptor<Rng>::identity_adaptor;
    };

    namespace views
    {
        struct cpp20_common_fn
        {
            template(typename Rng)(
                requires viewable_range<Rng> AND common_range<Rng>)
            all_t<Rng> operator()(Rng && rng) const
            {
                return all(static_cast<Rng &&>(rng));
            }

            template(typename Rng)(
                requires viewable_range<Rng> AND (!common_range<Rng>)) //
            common_view<all_t<Rng>> operator()(Rng && rng) const
            {
                return common_view<all_t<Rng>>{all(static_cast<Rng &&>(rng))};
            }
        };

        struct common_fn
        {
            template(typename Rng)(
                requires viewable_range<Rng>)
            common_view<all_t<Rng>> operator()(Rng && rng) const
            {
                return common_view<all_t<Rng>>{all(static_cast<Rng &&>(rng))};
            }
        };

        /// \relates common_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(view_closure<common_fn>, common)
    } // namespace views
    /// @}

    /// \cond
    template<typename Rng>
    using bounded_view RANGES_DEPRECATED(
        "The name bounded_view is deprecated. "
        "Please use common_view instead.") = common_view<Rng>;
    /// \endcond

    namespace views
    {
        /// \cond
        namespace
        {
            RANGES_DEPRECATED(
                "The name views::bounded is deprecated. "
                "Please use views::common instead.")
            RANGES_INLINE_VAR constexpr auto & bounded = common;
        } // namespace

        template<typename Rng>
        using bounded_t RANGES_DEPRECATED("The name views::bounded_t is deprecated.") =
            decltype(common(std::declval<Rng>()));
        /// \endcond
    } // namespace views

    namespace cpp20
    {
        namespace views
        {
            RANGES_INLINE_VARIABLE(
                ranges::views::view_closure<ranges::views::cpp20_common_fn>, common)
        }
        template(typename Rng)(
            requires view_<Rng> && (!common_range<Rng>)) //
            using common_view = ranges::common_view<Rng>;
    } // namespace cpp20
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::common_view)

#endif
