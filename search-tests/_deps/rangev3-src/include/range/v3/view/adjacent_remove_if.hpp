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

#ifndef RANGES_V3_VIEW_ADJACENT_REMOVE_IF_HPP
#define RANGES_V3_VIEW_ADJACENT_REMOVE_IF_HPP

#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/bind_back.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/range/access.hpp>
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
    struct RANGES_EMPTY_BASES adjacent_remove_if_view
      : view_adaptor<adjacent_remove_if_view<Rng, Pred>, Rng,
                     is_finite<Rng>::value ? finite : range_cardinality<Rng>::value>
      , private box<semiregular_box_t<Pred>, adjacent_remove_if_view<Rng, Pred>>
    {
        adjacent_remove_if_view() = default;
        constexpr adjacent_remove_if_view(Rng rng, Pred pred)
          : adjacent_remove_if_view::view_adaptor{detail::move(rng)}
          , adjacent_remove_if_view::box(detail::move(pred))
        {}

    private:
        friend range_access;

        struct adaptor : adaptor_base
        {
        private:
            adjacent_remove_if_view * rng_;

        public:
            adaptor() = default;
            constexpr adaptor(adjacent_remove_if_view * rng) noexcept
              : rng_(rng)
            {}
            static constexpr iterator_t<Rng> begin(adjacent_remove_if_view & rng)
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
        };
        constexpr adaptor begin_adaptor()
        {
            cache_begin();
            return {this};
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
        CPP_member
        constexpr auto end_adaptor() noexcept //
            -> CPP_ret(adaptor_base)(
                requires (!common_range<Rng>))
        {
            return {};
        }

        constexpr void satisfy_forward(iterator_t<Rng> & it)
        {
            auto const last = ranges::end(this->base());
            if(it == last)
                return;
            auto & pred = this->adjacent_remove_if_view::box::get();
            for(auto nxt = it; ++nxt != last && invoke(pred, *it, *nxt); it = nxt)
                ;
        }
        constexpr void satisfy_reverse(iterator_t<Rng> & it)
        {
            auto const & first = *begin_;
            RANGES_ASSERT(it != first);
            (void)first;
            auto prv = it;
            --it;
            if(prv == ranges::end(this->base()))
            {
                return;
            }
            auto & pred = this->adjacent_remove_if_view::box::get();
            for(; invoke(pred, *it, *prv); prv = it, --it)
                RANGES_ASSERT(it != first);
        }

        void cache_begin()
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
    template(typename Rng, typename Fun)(
        requires copy_constructible<Rng>)
    adjacent_remove_if_view(Rng &&, Fun)
        -> adjacent_remove_if_view<views::all_t<Rng>, Fun>;
#endif

    namespace views
    {
        struct adjacent_remove_if_base_fn
        {
            template(typename Rng, typename Pred)(
                requires viewable_range<Rng> AND forward_range<Rng> AND
                    indirect_binary_predicate_<Pred, iterator_t<Rng>, iterator_t<Rng>>)
            constexpr adjacent_remove_if_view<all_t<Rng>, Pred> //
            operator()(Rng && rng, Pred pred) const
            {
                return {all(static_cast<Rng &&>(rng)), std::move(pred)};
            }
        };

        struct adjacent_remove_if_fn : adjacent_remove_if_base_fn
        {
            using adjacent_remove_if_base_fn::operator();

            template<typename Pred>
            constexpr auto operator()(Pred pred) const
            {
                return make_view_closure(
                    bind_back(adjacent_remove_if_base_fn{}, std::move(pred)));
            }
        };

        /// \relates adjacent_remove_if_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(adjacent_remove_if_fn, adjacent_remove_if)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>
#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::adjacent_remove_if_view)

#endif
