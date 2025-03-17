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
#ifndef RANGES_V3_VIEW_CACHE1_HPP
#define RANGES_V3_VIEW_CACHE1_HPP

#include <range/v3/range_fwd.hpp>

#include <range/v3/detail/range_access.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/optional.hpp>
#include <range/v3/view/adaptor.hpp>
#include <range/v3/view/all.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{
    template<typename Rng>
    struct cache1_view : view_facade<cache1_view<Rng>, range_cardinality<Rng>::value>
    {
    private:
        CPP_assert(view_<Rng>);
        CPP_assert(input_range<Rng>);
        CPP_assert(constructible_from<range_value_t<Rng>, range_reference_t<Rng>>);
        friend range_access;
        Rng rng_;
        bool dirty_ = true;
        detail::non_propagating_cache<range_value_t<Rng>> cache_;

        CPP_member
        auto update_(range_reference_t<Rng> && val) //
            -> CPP_ret(void)(
                requires assignable_from<range_value_t<Rng> &, range_reference_t<Rng>>)
        {
            if(!cache_)
                cache_.emplace(static_cast<range_reference_t<Rng> &&>(val));
            else
                *cache_ = static_cast<range_reference_t<Rng> &&>(val);
        }
        CPP_member
        auto update_(range_reference_t<Rng> && val) //
            -> CPP_ret(void)(
                requires (!assignable_from<range_value_t<Rng> &, range_reference_t<Rng>>))
        {
            cache_.emplace(static_cast<range_reference_t<Rng> &&>(val));
        }

        struct cursor;

        struct sentinel
        {
        private:
            friend cursor;
            sentinel_t<Rng> last_;

        public:
            sentinel() = default;
            constexpr explicit sentinel(sentinel_t<Rng> last)
              : last_(std::move(last))
            {}
        };

        struct cursor
        {
        private:
            cache1_view * parent_;
            iterator_t<Rng> current_;

        public:
            using value_type = range_value_t<Rng>;
            using single_pass = std::true_type;
            using difference_type = range_difference_t<Rng>;

            cursor() = default;

            constexpr explicit cursor(cache1_view * parent, iterator_t<Rng> current)
              : parent_(parent)
              , current_(std::move(current))
            {}
            range_value_t<Rng> && read() const
            {
                if(parent_->dirty_)
                {
                    parent_->update_(*current_);
                    parent_->dirty_ = false;
                }
                return std::move(*parent_->cache_);
            }
            void next()
            {
                ++current_;
                parent_->dirty_ = true;
            }
            bool equal(cursor const & that) const
            {
                return current_ == that.current_;
            }
            bool equal(sentinel const & that) const
            {
                return current_ == that.last_;
            }
            CPP_member
            auto distance_to(cursor const & that) const //
                -> CPP_ret(difference_type)(
                    requires sized_sentinel_for<iterator_t<Rng>, iterator_t<Rng>>)
            {
                return that.current_ - current_;
            }
            CPP_member
            auto distance_to(sentinel const & that) const //
                -> CPP_ret(difference_type)(
                    requires sized_sentinel_for<sentinel_t<Rng>, iterator_t<Rng>>)
            {
                return that.last_ - current_;
            }
        };

        cursor begin_cursor()
        {
            dirty_ = true;
            return cursor{this, ranges::begin(rng_)};
        }

        cursor end_cursor_impl(std::true_type)
        {
            return cursor{this, ranges::end(rng_)};
        }
        sentinel end_cursor_impl(std::false_type)
        {
            return sentinel{ranges::end(rng_)};
        }
        auto end_cursor()
        {
            return end_cursor_impl(meta::bool_<(bool)common_range<Rng>>{});
        }

    public:
        cache1_view() = default;
        constexpr explicit cache1_view(Rng rng)
          : rng_{std::move(rng)}
        {}
        CPP_auto_member
        constexpr auto CPP_fun(size)()(
            requires sized_range<Rng>)
        {
            return ranges::size(rng_);
        }
    };

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template<typename Rng>
    cache1_view(Rng &&) //
        -> cache1_view<views::all_t<Rng>>;
#endif

    namespace views
    {
        struct cache1_fn
        {
            /// \brief Caches the most recent element within the view so that
            /// dereferencing the view's iterator multiple times doesn't incur any
            /// recomputation. This can be useful in adaptor pipelines that include
            /// combinations of \c view::filter and \c view::transform, for instance.
            /// \note \c views::cache1 is always single-pass.
            template(typename Rng)(
                requires viewable_range<Rng> AND input_range<Rng> AND
                    constructible_from<range_value_t<Rng>, range_reference_t<Rng>>)
            constexpr cache1_view<all_t<Rng>> operator()(Rng && rng) const //
            {
                return cache1_view<all_t<Rng>>{all(static_cast<Rng &&>(rng))};
            }
        };

        /// \relates cache1_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(view_closure<cache1_fn>, cache1)
    } // namespace views

    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>
#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::cache1_view)

#endif
