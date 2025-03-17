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

#ifndef RANGES_V3_VIEW_DROP_WHILE_HPP
#define RANGES_V3_VIEW_DROP_WHILE_HPP

#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/find_if_not.hpp>
#include <range/v3/functional/bind_back.hpp>
#include <range/v3/functional/compose.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/optional.hpp>
#include <range/v3/utility/semiregular_box.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/interface.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{
    template<typename Rng, typename Pred>
    struct drop_while_view
      : view_interface<drop_while_view<Rng, Pred>,
                       is_finite<Rng>::value ? finite : unknown>
    {
    private:
        Rng rng_;
        RANGES_NO_UNIQUE_ADDRESS semiregular_box_t<Pred> pred_;
        detail::non_propagating_cache<iterator_t<Rng>> begin_;

        iterator_t<Rng> get_begin_()
        {
            if(!begin_)
                begin_ = find_if_not(rng_, std::ref(pred_));
            return *begin_;
        }

    public:
        drop_while_view() = default;
        drop_while_view(Rng rng, Pred pred)
          : rng_(std::move(rng))
          , pred_(std::move(pred))
        {}
        iterator_t<Rng> begin()
        {
            return get_begin_();
        }
        sentinel_t<Rng> end()
        {
            return ranges::end(rng_);
        }
        Rng base() const
        {
            return rng_;
        }
    };

    // unlike take_while_view, drop_while_view is transparently safe because we only
    // need the predicate to find begin()
    template<typename Rng, typename Pred>
    RANGES_INLINE_VAR constexpr bool enable_borrowed_range<drop_while_view<Rng, Pred>> =
        enable_borrowed_range<Rng>;

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template(typename Rng, typename Fun)(
        requires copy_constructible<Fun>)
    drop_while_view(Rng &&, Fun)
        -> drop_while_view<views::all_t<Rng>, Fun>;
#endif

    template<typename Rng, typename Pred>
    RANGES_INLINE_VAR constexpr bool disable_sized_range<drop_while_view<Rng, Pred>> =
        true;

    namespace views
    {
        struct drop_while_base_fn
        {
            template(typename Rng, typename Pred)(
                requires viewable_range<Rng> AND input_range<Rng> AND
                    indirect_unary_predicate<Pred, iterator_t<Rng>>)
            auto operator()(Rng && rng, Pred pred) const
                -> drop_while_view<all_t<Rng>, Pred>
            {
                return {all(static_cast<Rng &&>(rng)), std::move(pred)};
            }
            template(typename Rng, typename Pred, typename Proj)(
                requires viewable_range<Rng> AND input_range<Rng> AND
                    indirect_unary_predicate<composed<Pred, Proj>, iterator_t<Rng>>)
            auto operator()(Rng && rng, Pred pred, Proj proj) const
                -> drop_while_view<all_t<Rng>, composed<Pred, Proj>>
            {
                return {all(static_cast<Rng &&>(rng)),
                        compose(std::move(pred), std::move(proj))};
            }
        };

        struct drop_while_bind_fn
        {
            template<typename Pred>
            constexpr auto operator()(Pred pred) const // TODO: underconstrained
            {
                return make_view_closure(
                    bind_back(drop_while_base_fn{}, std::move(pred)));
            }
            template(typename Pred, typename Proj)(
                requires (!range<Pred>)) // TODO: underconstrained
            constexpr auto operator()(Pred && pred, Proj proj) const
            {
                return make_view_closure(bind_back(
                    drop_while_base_fn{}, static_cast<Pred &&>(pred), std::move(proj)));
            }
        };

        struct RANGES_EMPTY_BASES drop_while_fn
           : drop_while_base_fn, drop_while_bind_fn
        {
            using drop_while_base_fn::operator();
            using drop_while_bind_fn::operator();
        };

        /// \relates drop_while_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(drop_while_fn, drop_while)
    } // namespace views

    namespace cpp20
    {
        namespace views
        {
            using ranges::views::drop_while;
        }
        template(typename Rng, typename Pred)(
            requires viewable_range<Rng> AND input_range<Rng> AND
                indirect_unary_predicate<Pred, iterator_t<Rng>>)
            using drop_while_view = ranges::drop_while_view<Rng, Pred>;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>
#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::drop_while_view)

#endif
