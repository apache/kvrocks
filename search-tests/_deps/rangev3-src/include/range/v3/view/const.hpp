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

#ifndef RANGES_V3_VIEW_CONST_HPP
#define RANGES_V3_VIEW_CONST_HPP

#include <type_traits>
#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/utility/common_type.hpp>
#include <range/v3/utility/move.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/adaptor.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{
    template<typename Rng>
    struct const_view : view_adaptor<const_view<Rng>, Rng>
    {
    private:
        friend range_access;
        template<bool Const>
        struct adaptor : adaptor_base
        {
            using CRng = meta::const_if_c<Const, Rng>;
            using value_ = range_value_t<CRng>;
            using reference_ =
                common_reference_t<value_ const &&, range_reference_t<CRng>>;
            using rvalue_reference_ =
                common_reference_t<value_ const &&, range_rvalue_reference_t<CRng>>;
            adaptor() = default;
            template(bool Other)(
                requires Const && CPP_NOT(Other)) //
                constexpr adaptor(adaptor<Other>)
            {}
            reference_ read(iterator_t<CRng> const & it) const
            {
                return *it;
            }
            rvalue_reference_ iter_move(iterator_t<CRng> const & it) const
                noexcept(noexcept(rvalue_reference_(ranges::iter_move(it))))
            {
                return ranges::iter_move(it);
            }
        };
        adaptor<simple_view<Rng>()> begin_adaptor()
        {
            return {};
        }
        CPP_member
        auto begin_adaptor() const //
            -> CPP_ret(adaptor<true>)(
                requires range<Rng const>)
        {
            return {};
        }
        adaptor<simple_view<Rng>()> end_adaptor()
        {
            return {};
        }
        CPP_member
        auto end_adaptor() const //
            -> CPP_ret(adaptor<true>)(
                requires range<Rng const>)
        {
            return {};
        }

    public:
        const_view() = default;
        explicit const_view(Rng rng)
          : const_view::view_adaptor{std::move(rng)}
        {}
        CPP_auto_member
        constexpr auto CPP_fun(size)()(
            requires sized_range<Rng>)
        {
            return ranges::size(this->base());
        }
        CPP_auto_member
        constexpr auto CPP_fun(size)()(const
            requires sized_range<Rng const>)
        {
            return ranges::size(this->base());
        }
    };

    template<typename Rng>
    RANGES_INLINE_VAR constexpr bool enable_borrowed_range<const_view<Rng>> = //
        enable_borrowed_range<Rng>;

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template<typename Rng>
    const_view(Rng &&) //
        -> const_view<views::all_t<Rng>>;
#endif

    namespace views
    {
        struct const_fn
        {
            template(typename Rng)(
                requires viewable_range<Rng> AND input_range<Rng>)
            const_view<all_t<Rng>> operator()(Rng && rng) const
            {
                return const_view<all_t<Rng>>{all(static_cast<Rng &&>(rng))};
            }
        };

        /// \relates const_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(view_closure<const_fn>, const_)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>
#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::const_view)

#endif
