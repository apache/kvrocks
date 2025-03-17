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

#ifndef RANGES_V3_VIEW_INDIRECT_HPP
#define RANGES_V3_VIEW_INDIRECT_HPP

#include <iterator>
#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/range/access.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/move.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/adaptor.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{
    template<typename Rng>
    struct indirect_view : view_adaptor<indirect_view<Rng>, Rng>
    {
    private:
        friend range_access;

        template<bool IsConst>
        struct adaptor : adaptor_base
        {
            friend adaptor<!IsConst>;
            using CRng = meta::const_if_c<IsConst, Rng>;

            adaptor() = default;
            template(bool Other)(
                requires IsConst && CPP_NOT(Other)) //
            constexpr adaptor(adaptor<Other>) noexcept
            {}

            // clang-format off
            constexpr auto CPP_auto_fun(read)(iterator_t<CRng> const &it)(const)
            (
                return **it
            )
            constexpr auto CPP_auto_fun(iter_move)(iterator_t<CRng> const &it)(const)
            (
                return ranges::iter_move(*it)
            )
            // clang-format on
        };

        CPP_member
        constexpr auto begin_adaptor() noexcept //
            -> CPP_ret(adaptor<false>)(
                requires (!simple_view<Rng>()))
        {
            return {};
        }
        CPP_member
        constexpr auto begin_adaptor() const noexcept //
            -> CPP_ret(adaptor<true>)(
                requires range<Rng const>)
        {
            return {};
        }

        CPP_member
        constexpr auto end_adaptor() noexcept //
            -> CPP_ret(adaptor<false>)(
                requires (!simple_view<Rng>()))
        {
            return {};
        }
        CPP_member
        constexpr auto end_adaptor() const noexcept //
            -> CPP_ret(adaptor<true>)(
                requires range<Rng const>)
        {
            return {};
        }

    public:
        indirect_view() = default;
        constexpr explicit indirect_view(Rng rng)
          : indirect_view::view_adaptor{detail::move(rng)}
        {}
        CPP_auto_member
        constexpr auto CPP_fun(size)()(const //
            requires sized_range<Rng const>)
        {
            return ranges::size(this->base());
        }
        CPP_auto_member
        constexpr auto CPP_fun(size)()(
            requires sized_range<Rng>)
        {
            return ranges::size(this->base());
        }
    };

    template<typename Rng>
    RANGES_INLINE_VAR constexpr bool enable_borrowed_range<indirect_view<Rng>> = //
        enable_borrowed_range<Rng>;

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template<typename Rng>
    indirect_view(Rng &&) //
        -> indirect_view<views::all_t<Rng>>;
#endif

    namespace views
    {
        struct indirect_fn
        {
            template(typename Rng)(
                requires viewable_range<Rng> AND input_range<Rng> AND
                // We shouldn't need to strip references to test if something
                // is readable. https://github.com/ericniebler/stl2/issues/594
                // indirectly_readable<range_reference_t<Rng>>)
                ((bool)indirectly_readable<range_value_t<Rng>>)) // Cast to bool needed
                                                                 // for GCC (???))
            constexpr auto operator()(Rng && rng) const
            {
                return indirect_view<all_t<Rng>>{all(static_cast<Rng &&>(rng))};
            }
        };

        /// \relates indirect_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(view_closure<indirect_fn>, indirect)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::indirect_view)

#endif
