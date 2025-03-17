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

#ifndef RANGES_V3_VIEW_ADJACENT_FILTER_HPP
#define RANGES_V3_VIEW_ADJACENT_FILTER_HPP

#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/adjacent_find.hpp>
#include <range/v3/functional/bind_back.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/utility/semiregular_box.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/adaptor.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
        // clang-format off
        /// \concept adjacent_filter_constraints_
        /// \brief The \c adjacent_filter_constraints_ concept
        template(typename Rng, typename Pred)(
        concept (adjacent_filter_constraints_)(Rng, Pred),
            indirect_binary_predicate_<Pred, iterator_t<Rng>, iterator_t<Rng>>
        );
        /// \concept adjacent_filter_constraints
        /// \brief The \c adjacent_filter_constraints concept
        template<typename Rng, typename Pred>
        CPP_concept adjacent_filter_constraints =
            viewable_range<Rng> && forward_range<Rng> &&
            CPP_concept_ref(detail::adjacent_filter_constraints_, Rng, Pred);
        // clang-format on
    } // namespace detail
    /// \endcond

    /// \addtogroup group-views
    /// @{
    template<typename Rng, typename Pred>
    struct RANGES_EMPTY_BASES adjacent_filter_view
      : view_adaptor<adjacent_filter_view<Rng, Pred>, Rng,
                     is_finite<Rng>::value ? finite : range_cardinality<Rng>::value>
      , private box<semiregular_box_t<Pred>, adjacent_filter_view<Rng, Pred>>
    {
    private:
        friend range_access;

        template<bool Const>
        struct adaptor : adaptor_base
        {
        private:
            friend struct adaptor<!Const>;
            using CRng = meta::const_if_c<Const, Rng>;
            using Parent = meta::const_if_c<Const, adjacent_filter_view>;
            Parent * rng_;

        public:
            adaptor() = default;
            constexpr adaptor(Parent * rng) noexcept
              : rng_(rng)
            {}
            template(bool Other)(
                requires Const && CPP_NOT(Other)) //
                constexpr adaptor(adaptor<Other> that)
              : rng_(that.rng_)
            {}
            constexpr void next(iterator_t<CRng> & it) const
            {
                auto const last = ranges::end(rng_->base());
                auto & pred = rng_->adjacent_filter_view::box::get();
                RANGES_EXPECT(it != last);
                for(auto tmp = it; ++it != last; tmp = it)
                    if(invoke(pred, *tmp, *it))
                        break;
            }
            CPP_member
            constexpr auto prev(iterator_t<CRng> & it) const //
                -> CPP_ret(void)(
                    requires bidirectional_range<CRng>)
            {
                auto const first = ranges::begin(rng_->base());
                auto & pred = rng_->adjacent_filter_view::box::get();
                RANGES_EXPECT(it != first);
                --it;
                while(it != first)
                {
                    auto tmp = it;
                    if(invoke(pred, *--tmp, *it))
                        break;
                    it = tmp;
                }
            }
            void distance_to() = delete;
        };
        constexpr adaptor<false> begin_adaptor() noexcept
        {
            return {this};
        }
        CPP_member
        constexpr auto begin_adaptor() const noexcept //
            -> CPP_ret(adaptor<true>)(
                requires detail::adjacent_filter_constraints<Rng const, Pred const>)
        {
            return {this};
        }
        constexpr adaptor<false> end_adaptor() noexcept
        {
            return {this};
        }
        CPP_member
        constexpr auto end_adaptor() const noexcept //
            -> CPP_ret(adaptor<true>)(
                requires detail::adjacent_filter_constraints<Rng const, Pred const>)
        {
            return {this};
        }

    public:
        adjacent_filter_view() = default;
        constexpr adjacent_filter_view(Rng rng, Pred pred)
          : adjacent_filter_view::view_adaptor{detail::move(rng)}
          , adjacent_filter_view::box(detail::move(pred))
        {}
    };

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template(typename Rng, typename Fun)(
        requires copy_constructible<Rng>)
        adjacent_filter_view(Rng &&, Fun)
            ->adjacent_filter_view<views::all_t<Rng>, Fun>;
#endif

    namespace views
    {
        struct adjacent_filter_base_fn
        {
            template(typename Rng, typename Pred)(
                requires detail::adjacent_filter_constraints<Rng, Pred>)
            constexpr adjacent_filter_view<all_t<Rng>, Pred> //
            operator()(Rng && rng, Pred pred) const
            {
                return {all(static_cast<Rng &&>(rng)), std::move(pred)};
            }
        };

        struct adjacent_filter_fn : adjacent_filter_base_fn
        {
            using adjacent_filter_base_fn::operator();

            template<typename Pred>
            constexpr auto operator()(Pred pred) const
            {
                return make_view_closure(
                    bind_back(adjacent_filter_base_fn{}, std::move(pred)));
            }
        };

        /// \relates adjacent_filter_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(adjacent_filter_fn, adjacent_filter)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::adjacent_filter_view)

#endif
