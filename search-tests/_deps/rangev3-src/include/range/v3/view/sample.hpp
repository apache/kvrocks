/// \file
// Range v3 library
//
//  Copyright Casey Carter 2016
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_VIEW_SAMPLE_HPP
#define RANGES_V3_VIEW_SAMPLE_HPP

#include <meta/meta.hpp>

#include <range/v3/algorithm/shuffle.hpp>
#include <range/v3/functional/bind_back.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/default_sentinel.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/facade.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
        template<typename Rng,
                 bool = (bool)sized_sentinel_for<sentinel_t<Rng>, iterator_t<Rng>>>
        class size_tracker
        {
            range_difference_t<Rng> size_;

        public:
            CPP_assert(forward_range<Rng> || sized_range<Rng>);
            size_tracker() = default;
            size_tracker(Rng & rng)
              : size_(ranges::distance(rng))
            {}
            void decrement()
            {
                --size_;
            }
            range_difference_t<Rng> get(Rng &, iterator_t<Rng> &) const
            {
                return size_;
            }
        };

        // Impl for sized_sentinel_for (no need to store anything)
        template<typename Rng>
        class size_tracker<Rng, true>
        {
        public:
            size_tracker() = default;
            size_tracker(Rng &)
            {}
            void decrement()
            {}
            range_difference_t<Rng> get(Rng & rng, iterator_t<Rng> const & it) const
            {
                return ranges::end(rng) - it;
            }
        };
    } // namespace detail
    /// \endcond

    /// \addtogroup group-views
    /// @{

    // Take a random sampling from another view
    template<typename Rng, typename URNG>
    class sample_view : public view_facade<sample_view<Rng, URNG>, finite>
    {
        friend range_access;
        using D = range_difference_t<Rng>;
        Rng rng_;
        // Mutable is OK here because sample_view is an Input view.
        mutable range_difference_t<Rng> size_;
        URNG * engine_;

        template<bool IsConst>
        class cursor
        {
            friend cursor<!IsConst>;

            using Base = meta::const_if_c<IsConst, Rng>;
            meta::const_if_c<IsConst, sample_view> * parent_;
            iterator_t<Base> current_;
            RANGES_NO_UNIQUE_ADDRESS detail::size_tracker<Base> size_;

            D pop_size()
            {
                return size_.get(parent_->rng_, current_);
            }
            void advance()
            {
                if(parent_->size_ > 0)
                {
                    using Dist = std::uniform_int_distribution<D>;
                    Dist dist{};
                    URNG & engine = *parent_->engine_;

                    for(;; ++current_, size_.decrement())
                    {
                        RANGES_ASSERT(current_ != ranges::end(parent_->rng_));
                        auto n = pop_size();
                        RANGES_EXPECT(n > 0);
                        typename Dist::param_type const interval{0, n - 1};
                        if(dist(engine, interval) < parent_->size_)
                            break;
                    }
                }
            }

        public:
            using value_type = range_value_t<Rng>;
            using difference_type = D;

            cursor() = default;
            explicit cursor(meta::const_if_c<IsConst, sample_view> * rng)
              : parent_(rng)
              , current_(ranges::begin(rng->rng_))
              , size_{rng->rng_}
            {
                auto n = pop_size();
                if(rng->size_ > n)
                    rng->size_ = n;
                advance();
            }
            template(bool Other)(
                requires IsConst AND CPP_NOT(Other)) //
            cursor(cursor<Other> that)
              : parent_(that.parent_)
              , current_(std::move(that.current_))
              , size_(that.size_)
            {}
            range_reference_t<Rng> read() const
            {
                return *current_;
            }
            bool equal(default_sentinel_t) const
            {
                RANGES_EXPECT(parent_);
                return parent_->size_ <= 0;
            }
            void next()
            {
                RANGES_EXPECT(parent_);
                RANGES_EXPECT(parent_->size_ > 0);
                --parent_->size_;
                RANGES_ASSERT(current_ != ranges::end(parent_->rng_));
                ++current_;
                size_.decrement();
                advance();
            }
        };

        cursor<false> begin_cursor()
        {
            return cursor<false>{this};
        }
        template(bool Const = true)(
            requires Const AND
            (sized_range<meta::const_if_c<Const, Rng>> ||
             sized_sentinel_for<sentinel_t<meta::const_if_c<Const, Rng>>,
                                iterator_t<meta::const_if_c<Const, Rng>>> ||
             forward_range<meta::const_if_c<Const, Rng>>)) //
        cursor<Const> begin_cursor() const
        {
            return cursor<true>{this};
        }

    public:
        sample_view() = default;

        explicit sample_view(Rng rng, D sample_size, URNG & generator)
          : rng_(std::move(rng))
          , size_(sample_size)
          , engine_(std::addressof(generator))
        {
            RANGES_EXPECT(sample_size >= 0);
        }

        Rng base() const
        {
            return rng_;
        }
    };

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template<typename Rng, typename URNG>
    sample_view(Rng &&, range_difference_t<Rng>, URNG &)
        ->sample_view<views::all_t<Rng>, URNG>;
#endif

    namespace views
    {
        /// Returns a random sample of a range of length `size(range)`.
        struct sample_base_fn
        {
            template(typename Rng, typename URNG = detail::default_random_engine)(
                requires viewable_range<Rng> AND input_range<Rng> AND
                    uniform_random_bit_generator<URNG> AND
                    convertible_to<invoke_result_t<URNG &>, range_difference_t<Rng>> AND
                    (sized_range<Rng> ||
                     sized_sentinel_for<sentinel_t<Rng>, iterator_t<Rng>> ||
                     forward_range<Rng>)) //
            sample_view<all_t<Rng>, URNG> operator()(
                Rng && rng,
                range_difference_t<Rng> sample_size,
                URNG & generator = detail::get_random_engine()) const
            {
                return sample_view<all_t<Rng>, URNG>{
                    all(static_cast<Rng &&>(rng)), sample_size, generator};
            }

            /// \cond
            template<typename Rng, typename URNG>
            invoke_result_t<sample_base_fn, Rng, range_difference_t<Rng>, URNG &> //
            operator()(
                Rng && rng,
                range_difference_t<Rng> sample_size,
                detail::reference_wrapper_<URNG> r) const
            {
                return (*this)(static_cast<Rng &&>(rng), sample_size, r.get());
            }
            /// \endcond
        };

        struct sample_fn : sample_base_fn
        {
            using sample_base_fn::operator();

            template(typename Size, typename URNG = detail::default_random_engine)(
                requires integral<Size> AND uniform_random_bit_generator<URNG>)
            constexpr auto operator()(
                Size n,
                URNG & urng = detail::get_random_engine()) const //
            {
                return make_view_closure(bind_back(
                    sample_base_fn{}, n, detail::reference_wrapper_<URNG>(urng)));
            }
        };

        /// \relates sample_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(sample_fn, sample)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>
#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::sample_view)

#endif
