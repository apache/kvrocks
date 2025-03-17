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

#ifndef RANGES_V3_VIEW_CHUNK_HPP
#define RANGES_V3_VIEW_CHUNK_HPP

#include <limits>
#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/bind_back.hpp>
#include <range/v3/iterator/default_sentinel.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/box.hpp>
#include <range/v3/utility/optional.hpp> // for non_propagating_cache
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/adaptor.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/facade.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
        template<typename Rng, bool Const>
        constexpr bool can_sized_sentinel_() noexcept
        {
            using I = iterator_t<meta::const_if_c<Const, Rng>>;
            return (bool)sized_sentinel_for<I, I>;
        }

        template<typename T>
        struct zero
        {
            zero() = default;
            constexpr explicit zero(T const &) noexcept
            {}
            constexpr zero & operator=(T const &) noexcept
            {
                return *this;
            }
            constexpr zero const & operator=(T const &) const noexcept
            {
                return *this;
            }
            constexpr operator T() const
            {
                return T(0);
            }
            constexpr T exchange(T const &) const
            {
                return T(0);
            }
        };
    } // namespace detail
    /// \endcond

    /// \addtogroup group-views
    /// @{
    template<typename Rng, bool IsForwardRange>
    struct chunk_view_
      : view_adaptor<chunk_view_<Rng, IsForwardRange>, Rng,
                     is_finite<Rng>::value ? finite : range_cardinality<Rng>::value>
    {
    private:
        friend range_access;
        CPP_assert(forward_range<Rng>);

        template<bool Const>
        using offset_t =
            meta::if_c<bidirectional_range<meta::const_if_c<Const, Rng>> ||
                           detail::can_sized_sentinel_<Rng, Const>(),
                       range_difference_t<Rng>, detail::zero<range_difference_t<Rng>>>;

        range_difference_t<Rng> n_ = 0;

        template<bool Const>
        struct RANGES_EMPTY_BASES adaptor
          : adaptor_base
          , private box<offset_t<Const>>
        {
        private:
            friend adaptor<!Const>;
            using CRng = meta::const_if_c<Const, Rng>;

            range_difference_t<CRng> n_;
            sentinel_t<CRng> end_;

            constexpr offset_t<Const> const & offset() const
            {
                offset_t<Const> const & result = this->box<offset_t<Const>>::get();
                RANGES_EXPECT(0 <= result && result < n_);
                return result;
            }
            constexpr offset_t<Const> & offset()
            {
                return const_cast<offset_t<Const> &>(
                    const_cast<adaptor const &>(*this).offset());
            }

        public:
            adaptor() = default;
            constexpr adaptor(meta::const_if_c<Const, chunk_view_> * cv)
              : box<offset_t<Const>>{0}
              , n_((RANGES_EXPECT(0 < cv->n_), cv->n_))
              , end_(ranges::end(cv->base()))
            {}
            template(bool Other)(
                requires Const AND CPP_NOT(Other)) //
            constexpr adaptor(adaptor<Other> that)
              : box<offset_t<Const>>(that.offset())
              , n_(that.n_)
              , end_(that.end_)
            {}
            constexpr auto read(iterator_t<CRng> const & it) const
                -> decltype(views::take(make_subrange(it, end_), n_))
            {
                RANGES_EXPECT(it != end_);
                RANGES_EXPECT(0 == offset());
                return views::take(make_subrange(it, end_), n_);
            }
            constexpr void next(iterator_t<CRng> & it)
            {
                RANGES_EXPECT(it != end_);
                RANGES_EXPECT(0 == offset());
                offset() = ranges::advance(it, n_, end_);
            }
            CPP_member
            constexpr auto prev(iterator_t<CRng> & it) //
                -> CPP_ret(void)(
                    requires bidirectional_range<CRng>)
            {
                ranges::advance(it, -n_ + offset());
                offset() = 0;
            }
            CPP_member
            constexpr auto distance_to(iterator_t<CRng> const & here,
                                       iterator_t<CRng> const & there,
                                       adaptor const & that) const
                -> CPP_ret(range_difference_t<Rng>)(
                    requires (detail::can_sized_sentinel_<Rng, Const>()))
            {
                auto const delta = (there - here) + (that.offset() - offset());
                // This can fail for cyclic base ranges when the chunk size does not
                // divide the cycle length. Such iterator pairs are NOT in the domain of
                // -.
                RANGES_ENSURE(0 == delta % n_);
                return delta / n_;
            }
            CPP_member
            constexpr auto advance(iterator_t<CRng> & it, range_difference_t<Rng> n) //
                -> CPP_ret(void)(
                    requires random_access_range<CRng>)
            {
                using Limits = std::numeric_limits<range_difference_t<CRng>>;
                if(0 < n)
                {
                    RANGES_EXPECT(0 == offset());
                    RANGES_EXPECT(n <= Limits::max() / n_);
                    auto const remainder = ranges::advance(it, n * n_, end_) % n_;
                    RANGES_EXPECT(0 <= remainder && remainder < n_);
                    offset() = remainder;
                }
                else if(0 > n)
                {
                    RANGES_EXPECT(n >= Limits::min() / n_);
                    ranges::advance(it, n * n_ + offset());
                    offset() = 0;
                }
            }
        };

        constexpr adaptor<simple_view<Rng>()> begin_adaptor()
        {
            return adaptor<simple_view<Rng>()>{this};
        }
        CPP_member
        constexpr auto begin_adaptor() const //
            -> CPP_ret(adaptor<true>)(
                requires forward_range<Rng const>)
        {
            return adaptor<true>{this};
        }
        template<typename Size>
        constexpr Size size_(Size base_size) const
        {
            auto const n = static_cast<Size>(n_);
            return base_size / n + (0 != (base_size % n));
        }

    public:
        chunk_view_() = default;
        constexpr chunk_view_(Rng rng, range_difference_t<Rng> n)
          : chunk_view_::view_adaptor(detail::move(rng))
          , n_((RANGES_EXPECT(0 < n), n))
        {}
        CPP_auto_member
        constexpr auto CPP_fun(size)()(const
            requires sized_range<Rng const>)
        {
            return size_(ranges::size(this->base()));
        }
        CPP_auto_member
        constexpr auto CPP_fun(size)()(
            requires sized_range<Rng>)
        {
            return size_(ranges::size(this->base()));
        }
    };

    template<typename Rng>
    struct chunk_view_<Rng, false>
      : view_facade<chunk_view_<Rng, false>,
                    is_finite<Rng>::value ? finite : range_cardinality<Rng>::value>
    {
    private:
        friend range_access;
        CPP_assert(input_range<Rng> && !forward_range<Rng>);

        using iter_cache_t = detail::non_propagating_cache<iterator_t<Rng>>;

        Rng base_;
        range_difference_t<Rng> n_;
        range_difference_t<Rng> remainder_;
        mutable iter_cache_t it_cache_;

        constexpr iterator_t<Rng> & it() noexcept
        {
            return *it_cache_;
        }
        constexpr iterator_t<Rng> const & it() const noexcept
        {
            return *it_cache_;
        }

        struct outer_cursor
        {
        private:
            struct inner_view : view_facade<inner_view, finite>
            {
            private:
                friend range_access;

                using value_type = range_value_t<Rng>;

                chunk_view_ * rng_ = nullptr;

                constexpr bool done() const noexcept
                {
                    RANGES_EXPECT(rng_);
                    return rng_->remainder_ == 0;
                }
                constexpr bool equal(default_sentinel_t) const noexcept
                {
                    return done();
                }
                constexpr iter_reference_t<iterator_t<Rng>> read() const
                {
                    RANGES_EXPECT(!done());
                    return *rng_->it();
                }
                constexpr iter_rvalue_reference_t<iterator_t<Rng>> move() const
                {
                    RANGES_EXPECT(!done());
                    return ranges::iter_move(rng_->it());
                }
                constexpr void next()
                {
                    RANGES_EXPECT(!done());
                    ++rng_->it();
                    --rng_->remainder_;
                    if(rng_->remainder_ != 0 && rng_->it() == ranges::end(rng_->base_))
                        rng_->remainder_ = 0;
                }
                CPP_member
                constexpr auto distance_to(default_sentinel_t) const
                    -> CPP_ret(range_difference_t<Rng>)(
                        requires sized_sentinel_for<sentinel_t<Rng>, iterator_t<Rng>>)
                {
                    RANGES_EXPECT(rng_);
                    auto const d = ranges::end(rng_->base_) - rng_->it();
                    return ranges::min(d, rng_->remainder_);
                }

            public:
                inner_view() = default;
                constexpr explicit inner_view(chunk_view_ * view) noexcept
                  : rng_{view}
                {}
                CPP_auto_member
                constexpr auto CPP_fun(size)()(
                    requires sized_sentinel_for<sentinel_t<Rng>, iterator_t<Rng>>)
                {
                    using size_type = detail::iter_size_t<iterator_t<Rng>>;
                    return static_cast<size_type>(distance_to(default_sentinel_t{}));
                }
            };

            chunk_view_ * rng_ = nullptr;

        public:
            using value_type = inner_view;

            outer_cursor() = default;
            constexpr explicit outer_cursor(chunk_view_ * view) noexcept
              : rng_{view}
            {}
            constexpr inner_view read() const
            {
                RANGES_EXPECT(!done());
                return inner_view{rng_};
            }
            constexpr bool done() const
            {
                RANGES_EXPECT(rng_);
                return rng_->it() == ranges::end(rng_->base_) && rng_->remainder_ != 0;
            }
            constexpr bool equal(default_sentinel_t) const
            {
                return done();
            }
            constexpr void next()
            {
                RANGES_EXPECT(!done());
                ranges::advance(rng_->it(), rng_->remainder_, ranges::end(rng_->base_));
                rng_->remainder_ = rng_->n_;
            }
            CPP_member
            constexpr auto distance_to(default_sentinel_t) const
                -> CPP_ret(range_difference_t<Rng>)(
                    requires sized_sentinel_for<sentinel_t<Rng>, iterator_t<Rng>>)
            {
                RANGES_EXPECT(rng_);
                auto d = ranges::end(rng_->base_) - rng_->it();
                if(d < rng_->remainder_)
                    return 1;

                d -= rng_->remainder_;
                d = (d + rng_->n_ - 1) / rng_->n_;
                d += (rng_->remainder_ != 0);
                return d;
            }
        };

        constexpr outer_cursor begin_cursor() noexcept
        {
            it_cache_ = ranges::begin(base_);
            return outer_cursor{this};
        }
        template<typename Size>
        constexpr Size size_(Size base_size) const
        {
            auto const n = static_cast<Size>(this->n_);
            return base_size / n + (0 != base_size % n);
        }

    public:
        chunk_view_() = default;
        constexpr chunk_view_(Rng rng, range_difference_t<Rng> n)
          : base_(detail::move(rng))
          , n_((RANGES_EXPECT(0 < n), n))
          , remainder_(n)
          , it_cache_{nullopt}
        {}
        CPP_auto_member
        constexpr auto CPP_fun(size)()(const
            requires sized_range<Rng const>)
        {
            return size_(ranges::size(base_));
        }
        CPP_auto_member
        constexpr auto CPP_fun(size)()(
            requires sized_range<Rng>)
        {
            return size_(ranges::size(base_));
        }
        Rng base() const
        {
            return base_;
        }
    };

    template<typename Rng>
    struct chunk_view : chunk_view_<Rng, (bool)forward_range<Rng>>
    {
        chunk_view() = default;
        constexpr chunk_view(Rng rng, range_difference_t<Rng> n)
          : chunk_view_<Rng, (bool)forward_range<Rng>>(static_cast<Rng &&>(rng), n)
        {}
    };

    // Need to keep extra state for input_range, but forward_range is transparent
    template<typename Rng>
    RANGES_INLINE_VAR constexpr bool enable_borrowed_range<chunk_view<Rng>> =
        enable_borrowed_range<Rng> && forward_range<Rng>;

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template<typename Rng>
    chunk_view(Rng &&, range_difference_t<Rng>)
        -> chunk_view<views::all_t<Rng>>;
#endif

    namespace views
    {
        // In:  range<T>
        // Out: range<range<T>>, where each inner range has $n$ elements.
        //                       The last range may have fewer.
        struct chunk_base_fn
        {
            template(typename Rng)(
                requires viewable_range<Rng> AND input_range<Rng>)
            constexpr chunk_view<all_t<Rng>> //
            operator()(Rng && rng, range_difference_t<Rng> n) const
            {
                return {all(static_cast<Rng &&>(rng)), n};
            }
        };

        struct chunk_fn : chunk_base_fn
        {
            using chunk_base_fn::operator();

            template(typename Int)(
                requires detail::integer_like_<Int>)
            constexpr auto operator()(Int n) const
            {
                return make_view_closure(bind_back(chunk_base_fn{}, n));
            }
        };

        /// \relates chunk_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(chunk_fn, chunk)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>
#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::chunk_view)

#endif
