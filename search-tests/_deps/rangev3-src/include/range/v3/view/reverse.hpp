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

#ifndef RANGES_V3_VIEW_REVERSE_HPP
#define RANGES_V3_VIEW_REVERSE_HPP

#include <iterator>
#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/reverse_iterator.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/box.hpp>
#include <range/v3/utility/get.hpp>
#include <range/v3/utility/optional.hpp>
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
    struct RANGES_EMPTY_BASES reverse_view
      : view_interface<reverse_view<Rng>, range_cardinality<Rng>::value>
      , private detail::non_propagating_cache<iterator_t<Rng>, reverse_view<Rng>,
                                              !common_range<Rng>>
    {
    private:
        CPP_assert(bidirectional_range<Rng>);
        Rng rng_;
        constexpr reverse_iterator<iterator_t<Rng>> begin_(std::true_type)
        {
            return make_reverse_iterator(ranges::end(rng_));
        }
        constexpr reverse_iterator<iterator_t<Rng>> begin_(std::false_type)
        {
            using cache_t =
                detail::non_propagating_cache<iterator_t<Rng>, reverse_view<Rng>>;
            auto & end_ = static_cast<cache_t &>(*this);
            if(!end_)
            {
#if defined(_MSC_VER)
                auto tmp = ranges::begin(rng_);
                auto e = ranges::end(rng_);
                while(tmp != e)
                    ++tmp;
#else
                auto tmp = ranges::next(ranges::begin(rng_), ranges::end(rng_));
#endif
                end_ = std::move(tmp);
            }
            return make_reverse_iterator(*end_);
        }

    public:
        reverse_view() = default;
        constexpr explicit reverse_view(Rng rng)
          : rng_(detail::move(rng))
        {}
        Rng base() const
        {
            return rng_;
        }
        constexpr reverse_iterator<iterator_t<Rng>> begin()
        {
            return begin_(meta::bool_<(bool)common_range<Rng>>{});
        }
        template(bool Const = true)(
            requires Const AND common_range<meta::const_if_c<Const, Rng>>)
        constexpr reverse_iterator<iterator_t<meta::const_if_c<Const, Rng>>> begin() const
        {
            return make_reverse_iterator(ranges::end(rng_));
        }
        constexpr reverse_iterator<iterator_t<Rng>> end()
        {
            return make_reverse_iterator(ranges::begin(rng_));
        }
        template(bool Const = true)(
            requires Const AND common_range<meta::const_if_c<Const, Rng>>)
        constexpr reverse_iterator<iterator_t<meta::const_if_c<Const, Rng>>> end() const
        {
            return make_reverse_iterator(ranges::begin(rng_));
        }
        CPP_auto_member
        constexpr auto CPP_fun(size)()(
            requires sized_range<Rng>)
        {
            return ranges::size(rng_);
        }
        CPP_auto_member
        constexpr auto CPP_fun(size)()(const //
            requires sized_range<Rng const>)
        {
            return ranges::size(rng_);
        }
    };

    template<typename Rng>
    struct reverse_view<reverse_view<Rng>> : Rng
    {
        CPP_assert(bidirectional_range<Rng>);
        CPP_assert(
            same_as<detail::decay_t<decltype(std::declval<reverse_view<Rng>>().base())>,
                    Rng>);

        reverse_view() = default;
        constexpr explicit reverse_view(reverse_view<Rng> rng)
          : Rng(rng.base())
        {}

        constexpr reverse_view<Rng> base() const
        {
            return reverse_view<Rng>{*this};
        }
    };

    template<typename Rng>
    RANGES_INLINE_VAR constexpr bool enable_borrowed_range<reverse_view<Rng>> =
        enable_borrowed_range<Rng>;

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template<typename Rng>
    reverse_view(Rng &&) //
        -> reverse_view<views::all_t<Rng>>;

    template<typename Rng>
    reverse_view(reverse_view<Rng>)
        -> reverse_view<reverse_view<Rng>>;
#endif

    namespace views
    {
        struct reverse_fn
        {
            template(typename Rng)(
                requires viewable_range<Rng> AND bidirectional_range<Rng>)
            constexpr reverse_view<all_t<Rng>> operator()(Rng && rng) const
            {
                return reverse_view<all_t<Rng>>{all(static_cast<Rng &&>(rng))};
            }
        };

        /// \relates reverse_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(view_closure<reverse_fn>, reverse)
    } // namespace views

    namespace cpp20
    {
        namespace views
        {
            using ranges::views::reverse;
        }
        template(typename Rng)(
            requires view_<Rng> AND bidirectional_range<Rng>)
        using reverse_view = ranges::reverse_view<Rng>;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>
#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::reverse_view)

#endif
