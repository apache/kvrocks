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

#ifndef RANGES_V3_VIEW_REMOVE_IF_HPP
#define RANGES_V3_VIEW_REMOVE_IF_HPP

#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/bind_back.hpp>
#include <range/v3/functional/compose.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/box.hpp>
#include <range/v3/utility/optional.hpp>
#include <range/v3/utility/semiregular_box.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/adaptor.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{
    template<typename Rng, typename Pred>
    struct RANGES_EMPTY_BASES remove_if_view
      : view_adaptor<remove_if_view<Rng, Pred>, Rng,
                     is_finite<Rng>::value ? finite : range_cardinality<Rng>::value>
      , private box<semiregular_box_t<Pred>>
    {
        remove_if_view() = default;
        constexpr remove_if_view(Rng rng, Pred pred)
          : remove_if_view::view_adaptor{detail::move(rng)}
          , remove_if_view::box(detail::move(pred))
        {}

    private:
        friend range_access;

        struct adaptor : adaptor_base
        {
            adaptor() = default;
            constexpr adaptor(remove_if_view * rng) noexcept
              : rng_(rng)
            {}
            static constexpr iterator_t<Rng> begin(remove_if_view & rng)
            {
                return *rng.begin_;
            }
            constexpr void next(iterator_t<Rng> & it) const
            {
                RANGES_ASSERT(it != ranges::end(rng_->base()));
                rng_->satisfy_forward(++it);
            }
            CPP_member
            constexpr auto prev(iterator_t<Rng> & it) const //
                -> CPP_ret(void)(
                    requires bidirectional_range<Rng>)
            {
                rng_->satisfy_reverse(it);
            }
            void advance() = delete;
            void distance_to() = delete;

        private:
            remove_if_view * rng_;
        };
        constexpr adaptor begin_adaptor()
        {
            cache_begin();
            return {this};
        }
        CPP_member
        constexpr auto end_adaptor() const noexcept //
            -> CPP_ret(adaptor_base)(
                requires (!common_range<Rng>))
        {
            return {};
        }
        CPP_member
        constexpr auto end_adaptor() //
            -> CPP_ret(adaptor)(
                requires common_range<Rng>)
        {
            if(bidirectional_range<Rng>)
                cache_begin();
            return {this};
        }

        constexpr void satisfy_forward(iterator_t<Rng> & it)
        {
            auto const last = ranges::end(this->base());
            auto & pred = this->remove_if_view::box::get();
            while(it != last && invoke(pred, *it))
                ++it;
        }
        constexpr void satisfy_reverse(iterator_t<Rng> & it)
        {
            RANGES_ASSERT(begin_);
            auto const & first = *begin_;
            auto & pred = this->remove_if_view::box::get();
            do
            {
                RANGES_ASSERT(it != first);
                (void)first;
                --it;
            } while(invoke(pred, *it));
        }

        constexpr void cache_begin()
        {
            if(begin_)
                return;
            auto it = ranges::begin(this->base());
            satisfy_forward(it);
            begin_.emplace(std::move(it));
        }

        detail::non_propagating_cache<iterator_t<Rng>> begin_;
    };

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template(typename Rng, typename Pred)(
        requires copy_constructible<Pred>)
    remove_if_view(Rng &&, Pred)
        -> remove_if_view<views::all_t<Rng>, Pred>;
#endif

    namespace views
    {
        /// Given a source range, unary predicate, and optional projection,
        /// present a view of the elements that do not satisfy the predicate.
        struct remove_if_base_fn
        {
            template(typename Rng, typename Pred)(
                requires viewable_range<Rng> AND input_range<Rng> AND
                    indirect_unary_predicate<Pred, iterator_t<Rng>>)
            constexpr remove_if_view<all_t<Rng>, Pred> operator()(Rng && rng, Pred pred)
                const
            {
                return remove_if_view<all_t<Rng>, Pred>{all(static_cast<Rng &&>(rng)),
                                                        std::move(pred)};
            }
            template(typename Rng, typename Pred, typename Proj)(
                requires viewable_range<Rng> AND input_range<Rng> AND
                    indirect_unary_predicate<Pred, projected<iterator_t<Rng>, Proj>>)
            constexpr remove_if_view<all_t<Rng>, composed<Pred, Proj>> //
            operator()(Rng && rng, Pred pred, Proj proj) const
            {
                return remove_if_view<all_t<Rng>, composed<Pred, Proj>>{
                    all(static_cast<Rng &&>(rng)),
                    compose(std::move(pred), std::move(proj))};
            }
        };

        struct remove_if_bind_fn
        {
            template<typename Pred>
            constexpr auto operator()(Pred pred) const // TODO: underconstrained
            {
                return make_view_closure(bind_back(remove_if_base_fn{}, std::move(pred)));
            }
            template(typename Pred, typename Proj)(
                requires (!range<Pred>)) // TODO: underconstrained
            constexpr auto operator()(Pred && pred, Proj proj) const
            {
                return make_view_closure(bind_back(
                    remove_if_base_fn{}, static_cast<Pred &&>(pred), std::move(proj)));
            }
        };

        struct RANGES_EMPTY_BASES remove_if_fn
          : remove_if_base_fn, remove_if_bind_fn
        {
            using remove_if_base_fn::operator();
            using remove_if_bind_fn::operator();
        };

        /// \relates remove_if_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(remove_if_fn, remove_if)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::remove_if_view)

#include <range/v3/detail/epilogue.hpp>

#endif
