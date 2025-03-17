/// \file cycle.hpp
// Range v3 library
//
//  Copyright Eric Niebler 2013-present
//  Copyright Gonzalo Brito Gadeschi 2015
//  Copyright Casey Carter 2015
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_VIEW_CYCLE_HPP
#define RANGES_V3_VIEW_CYCLE_HPP

#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/unreachable_sentinel.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/box.hpp>
#include <range/v3/utility/get.hpp>
#include <range/v3/utility/optional.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/facade.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{
    template<typename Rng, bool /* = (bool) is_infinite<Rng>() */>
    struct RANGES_EMPTY_BASES cycled_view
      : view_facade<cycled_view<Rng>, infinite>
      , private detail::non_propagating_cache<iterator_t<Rng>, cycled_view<Rng>,
                                              !common_range<Rng>>
    {
    private:
        CPP_assert(forward_range<Rng> && !is_infinite<Rng>::value);
        friend range_access;
        Rng rng_;

        using cache_t = detail::non_propagating_cache<iterator_t<Rng>, cycled_view<Rng>,
                                                      !common_range<Rng>>;

        template<bool IsConst>
        struct cursor
        {
        private:
            friend struct cursor<!IsConst>;
            template<typename T>
            using constify_if = meta::const_if_c<IsConst, T>;
            using cycled_view_t = constify_if<cycled_view>;
            using CRng = constify_if<Rng>;
            using iterator = iterator_t<CRng>;

            cycled_view_t * rng_{};
            iterator it_{};
            std::intmax_t n_ = 0;

            iterator get_end_(std::true_type, bool = false) const
            {
                return ranges::end(rng_->rng_);
            }
            template<bool CanBeEmpty = false>
            iterator get_end_(std::false_type, meta::bool_<CanBeEmpty> = {}) const
            {
                auto & end_ = static_cast<cache_t &>(*rng_);
                RANGES_EXPECT(CanBeEmpty || end_);
                if(CanBeEmpty && !end_)
                    end_ = ranges::next(it_, ranges::end(rng_->rng_));
                return *end_;
            }
            void set_end_(std::true_type) const
            {}
            void set_end_(std::false_type) const
            {
                auto & end_ = static_cast<cache_t &>(*rng_);
                if(!end_)
                    end_ = it_;
            }

        public:
            cursor() = default;
            cursor(cycled_view_t * rng)
              : rng_(rng)
              , it_(ranges::begin(rng->rng_))
            {}
            template(bool Other)(
                requires IsConst AND CPP_NOT(Other)) //
            cursor(cursor<Other> that)
              : rng_(that.rng_)
              , it_(std::move(that.it_))
            {}
            // clang-format off
            auto CPP_auto_fun(read)()(const)
            (
                return *it_
            )
            // clang-format on
            CPP_member
            auto equal(cursor const & pos) const //
                -> CPP_ret(bool)(
                    requires equality_comparable<iterator>)
            {
                RANGES_EXPECT(rng_ == pos.rng_);
                return n_ == pos.n_ && it_ == pos.it_;
            }
            void next()
            {
                auto const last = ranges::end(rng_->rng_);
                RANGES_EXPECT(it_ != last);
                if(++it_ == last)
                {
                    ++n_;
                    this->set_end_(meta::bool_<(bool)common_range<CRng>>{});
                    it_ = ranges::begin(rng_->rng_);
                }
            }
            CPP_member
            auto prev() //
                -> CPP_ret(void)(
                    requires bidirectional_range<CRng>)
            {
                if(it_ == ranges::begin(rng_->rng_))
                {
                    RANGES_EXPECT(n_ > 0); // decrementing the begin iterator?!
                    --n_;
                    it_ = this->get_end_(meta::bool_<(bool)common_range<CRng>>{});
                }
                --it_;
            }
            template(typename Diff)(
                requires random_access_range<CRng> AND
                    detail::integer_like_<Diff>)
            void advance(Diff n)
            {
                auto const first = ranges::begin(rng_->rng_);
                auto const last = this->get_end_(meta::bool_<(bool)common_range<CRng>>{},
                                                 meta::bool_<true>());
                auto const dist = last - first;
                auto const d = it_ - first;
                auto const off = (d + n) % dist;
                n_ += (d + n) / dist;
                RANGES_EXPECT(n_ >= 0);
                using D = range_difference_t<Rng>;
                it_ = first + static_cast<D>(off < 0 ? off + dist : off);
            }
            CPP_auto_member
            auto CPP_fun(distance_to)(cursor const & that)(const //
                requires sized_sentinel_for<iterator, iterator>)
            {
                RANGES_EXPECT(that.rng_ == rng_);
                auto const first = ranges::begin(rng_->rng_);
                auto const last = this->get_end_(meta::bool_<(bool)common_range<Rng>>{},
                                                 meta::bool_<true>());
                auto const dist = last - first;
                return (that.n_ - n_) * dist + (that.it_ - it_);
            }
        };

        CPP_member
        auto begin_cursor() //
            -> CPP_ret(cursor<false>)(
                requires (!simple_view<Rng>() || !common_range<Rng const>))
        {
            return {this};
        }
        CPP_member
        auto begin_cursor() const //
            -> CPP_ret(cursor<true>)(
                requires common_range<Rng const>)
        {
            return {this};
        }
        unreachable_sentinel_t end_cursor() const
        {
            return unreachable;
        }

    public:
        cycled_view() = default;
        /// \pre <tt>!empty(rng)</tt>
        explicit cycled_view(Rng rng)
          : rng_(std::move(rng))
        {
            RANGES_EXPECT(!ranges::empty(rng_));
        }
    };

    template<typename Rng>
    struct cycled_view<Rng, true> : identity_adaptor<Rng>
    {
        CPP_assert(is_infinite<Rng>::value);
        using identity_adaptor<Rng>::identity_adaptor;
    };

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template<typename Rng>
    cycled_view(Rng &&) //
        -> cycled_view<views::all_t<Rng>>;
#endif

    namespace views
    {
        /// Returns an infinite range that endlessly repeats the source
        /// range.
        struct cycle_fn
        {
            /// \pre <tt>!empty(rng)</tt>
            template(typename Rng)(
                requires viewable_range<Rng> AND forward_range<Rng>)
            cycled_view<all_t<Rng>> operator()(Rng && rng) const
            {
                return cycled_view<all_t<Rng>>{all(static_cast<Rng &&>(rng))};
            }
        };

        /// \relates cycle_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(view_closure<cycle_fn>, cycle)
    } // namespace views
      /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::cycled_view)

#endif
