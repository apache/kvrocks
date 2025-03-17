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
#ifndef RANGES_V3_ITERATOR_OPERATIONS_HPP
#define RANGES_V3_ITERATOR_OPERATIONS_HPP

#include <type_traits>
#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/range/concepts.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-iterator
    /// @{

    /// \cond
    template<typename I>
        // requires input_or_output_iterator<I>
    struct counted_iterator;
    /// \endcond

    struct advance_fn
    {
#if RANGES_CXX_IF_CONSTEXPR >= RANGES_CXX_IF_CONSTEXPR_17
        template(typename I)(
            requires input_or_output_iterator<I>)
        constexpr void operator()(I & i, iter_difference_t<I> n) const
        // [[expects: n >= 0 || bidirectional_iterator<I>]]
        {
            if constexpr(random_access_iterator<I>)
            {
                i += n;
            }
            else
            {
                if constexpr(bidirectional_iterator<I>)
                    for(; 0 > n; ++n)
                        --i;
                RANGES_EXPECT(0 <= n);
                for(; 0 < n; --n)
                    ++i;
            }
        }

        template(typename I, typename S)(
            requires sentinel_for<S, I>)
        constexpr void operator()(I & i, S bound) const
        // [[expects axiom: reachable(i, bound)]]
        {
            if constexpr(assignable_from<I &, S>)
            {
                i = std::move(bound);
            }
            else if constexpr(sized_sentinel_for<S, I>)
            {
                iter_difference_t<I> d = bound - i;
                RANGES_EXPECT(0 <= d);
                (*this)(i, d);
            }
            else
                while(i != bound)
                    ++i;
        }

        template(typename I, typename S)(
            requires sentinel_for<S, I>)
        constexpr iter_difference_t<I> //
        operator()(I & i, iter_difference_t<I> n, S bound) const
        // [[expects axiom: 0 == n ||
        //     (0 < n && reachable(i, bound)) ||
        //     (0 > n && same_as<I, S> && bidirectional_iterator<I> && reachable(bound,
        //     i))]]
        {
            if constexpr(sized_sentinel_for<S, I>)
            {
                if(0 == n)
                    return 0;
                const auto d = bound - i;
                if constexpr(bidirectional_iterator<I> && same_as<I, S>)
                {
                    RANGES_EXPECT(0 <= n ? 0 <= d : 0 >= d);
                    if(0 <= n ? d <= n : d >= n)
                    {
                        i = std::move(bound);
                        return n - d;
                    }
                }
                else
                {
                    RANGES_EXPECT(0 <= n && 0 <= d);
                    if(d <= n)
                    {
                        (*this)(i, std::move(bound));
                        return n - d;
                    }
                }
                (*this)(i, n);
                return 0;
            }
            else
            {
                if constexpr(bidirectional_iterator<I> && same_as<I, S>)
                {
                    if(0 > n)
                    {
                        do
                        {
                            --i;
                            ++n;
                        } while(0 != n && i != bound);
                        return n;
                    }
                }
                RANGES_EXPECT(0 <= n);
                while(0 != n && i != bound)
                {
                    ++i;
                    --n;
                }
                return n;
            }
        }
#else
    private:
        template<typename I>
        static constexpr void n_(I & i, iter_difference_t<I> n, std::input_iterator_tag);
        template<typename I>
        static constexpr void n_(I & i, iter_difference_t<I> n,
                                 std::bidirectional_iterator_tag);
        template<typename I>
        static constexpr void n_(I & i, iter_difference_t<I> n,
                                 std::random_access_iterator_tag);
        template<typename I, typename S>
        static constexpr void to_impl_(I & i, S s, sentinel_tag);
        template<typename I, typename S>
        static constexpr void to_impl_(I & i, S s, sized_sentinel_tag);
        template<typename I, typename S>
        static constexpr void to_(I & i, S s, std::true_type); // assignable
        template<typename I, typename S>
        static constexpr void to_(I & i, S s, std::false_type); // !assignable
        template<typename I, typename S>
        static constexpr iter_difference_t<I> bounded_(I & it, iter_difference_t<I> n,
                                                       S bound, sentinel_tag,
                                                       std::input_iterator_tag);
        template<typename I>
        static constexpr iter_difference_t<I> bounded_(I & it, iter_difference_t<I> n,
                                                       I bound, sentinel_tag,
                                                       std::bidirectional_iterator_tag);
        template<typename I, typename S, typename Concept>
        static constexpr iter_difference_t<I> bounded_(I & it, iter_difference_t<I> n,
                                                       S bound, sized_sentinel_tag,
                                                       Concept);

    public:
        // Advance a certain number of steps:
        template(typename I)(
            requires input_or_output_iterator<I>)
        constexpr void operator()(I & i, iter_difference_t<I> n) const
        {
            advance_fn::n_(i, n, iterator_tag_of<I>{});
        }
        // Advance to a certain position:
        template(typename I, typename S)(
            requires sentinel_for<S, I>)
        constexpr void operator()(I & i, S s) const
        {
            advance_fn::to_(
                i, static_cast<S &&>(s), meta::bool_<assignable_from<I &, S>>());
        }
        // Advance a certain number of times, with a bound:
        template(typename I, typename S)(
            requires sentinel_for<S, I>)
        constexpr iter_difference_t<I> //
        operator()(I & it, iter_difference_t<I> n, S bound) const
        {
            return advance_fn::bounded_(it,
                                        n,
                                        static_cast<S &&>(bound),
                                        sentinel_tag_of<S, I>(),
                                        iterator_tag_of<I>());
        }
#endif

        template(typename I)(
            requires input_or_output_iterator<I>)
        constexpr void operator()(counted_iterator<I> & i, iter_difference_t<I> n) const;
    };

    /// \sa `advance_fn`
    RANGES_INLINE_VARIABLE(advance_fn, advance)

#if RANGES_CXX_IF_CONSTEXPR < RANGES_CXX_IF_CONSTEXPR_17
    template<typename I>
    constexpr void advance_fn::n_(I & i, iter_difference_t<I> n, std::input_iterator_tag)
    {
        RANGES_EXPECT(n >= 0);
        for(; n > 0; --n)
            ++i;
    }
    template<typename I>
    constexpr void advance_fn::n_(I & i, iter_difference_t<I> n,
                                  std::bidirectional_iterator_tag)
    {
        if(n > 0)
            for(; n > 0; --n)
                ++i;
        else
            for(; n < 0; ++n)
                --i;
    }
    template<typename I>
    constexpr void advance_fn::n_(I & i, iter_difference_t<I> n,
                                  std::random_access_iterator_tag)
    {
        i += n;
    }
    template<typename I, typename S>
    constexpr void advance_fn::to_impl_(I & i, S s, sentinel_tag)
    {
        while(i != s)
            ++i;
    }
    template<typename I, typename S>
    constexpr void advance_fn::to_impl_(I & i, S s, sized_sentinel_tag)
    {
        iter_difference_t<I> d = s - i;
        RANGES_EXPECT(0 <= d);
        advance(i, d);
    }
    // Advance to a certain position:
    template<typename I, typename S>
    constexpr void advance_fn::to_(I & i, S s, std::true_type)
    {
        i = static_cast<S &&>(s);
    }
    template<typename I, typename S>
    constexpr void advance_fn::to_(I & i, S s, std::false_type)
    {
        advance_fn::to_impl_(i, static_cast<S &&>(s), sentinel_tag_of<S, I>());
    }
    template<typename I, typename S>
    constexpr iter_difference_t<I> advance_fn::bounded_(I & it, iter_difference_t<I> n,
                                                        S bound, sentinel_tag,
                                                        std::input_iterator_tag)
    {
        RANGES_EXPECT(0 <= n);
        for(; 0 != n && it != bound; --n)
            ++it;
        return n;
    }
    template<typename I>
    constexpr iter_difference_t<I> advance_fn::bounded_(I & it, iter_difference_t<I> n,
                                                        I bound, sentinel_tag,
                                                        std::bidirectional_iterator_tag)
    {
        if(0 <= n)
            for(; 0 != n && it != bound; --n)
                ++it;
        else
            for(; 0 != n && it != bound; ++n)
                --it;
        return n;
    }
    template<typename I, typename S, typename Concept>
    constexpr iter_difference_t<I> advance_fn::bounded_(I & it, iter_difference_t<I> n,
                                                        S bound, sized_sentinel_tag,
                                                        Concept)
    {
        RANGES_EXPECT(((bool)same_as<I, S> || 0 <= n));
        if(n == 0)
            return 0;
        iter_difference_t<I> d = bound - it;
        RANGES_EXPECT(0 <= n ? 0 <= d : 0 >= d);
        if(0 <= n ? n >= d : n <= d)
        {
            advance(it, static_cast<S &&>(bound));
            return n - d;
        }
        advance(it, n);
        return 0;
    }
#endif

    struct next_fn
    {
        template(typename I)(
            requires input_or_output_iterator<I>)
        constexpr I operator()(I it) const
        {
            return ++it;
        }
        template(typename I)(
            requires input_or_output_iterator<I>)
        constexpr I operator()(I it, iter_difference_t<I> n) const
        {
            advance(it, n);
            return it;
        }
        template(typename I, typename S)(
            requires sentinel_for<S, I>)
        constexpr I operator()(I it, S s) const
        {
            advance(it, static_cast<S &&>(s));
            return it;
        }
        template(typename I, typename S)(
            requires sentinel_for<S, I>)
        constexpr I operator()(I it, iter_difference_t<I> n, S bound) const
        {
            advance(it, n, static_cast<S &&>(bound));
            return it;
        }
    };

    /// \sa `next_fn`
    RANGES_INLINE_VARIABLE(next_fn, next)

    struct prev_fn
    {
        template(typename I)(
            requires bidirectional_iterator<I>)
        constexpr I operator()(I it) const
        {
            return --it;
        }
        template(typename I)(
            requires bidirectional_iterator<I>)
        constexpr I operator()(I it, iter_difference_t<I> n) const
        {
            advance(it, -n);
            return it;
        }
        template(typename I)(
            requires bidirectional_iterator<I>)
        constexpr I operator()(I it, iter_difference_t<I> n, I bound) const
        {
            advance(it, -n, static_cast<I &&>(bound));
            return it;
        }
    };

    /// \sa `prev_fn`
    RANGES_INLINE_VARIABLE(prev_fn, prev)

    struct iter_enumerate_fn
    {
    private:
        template(typename I, typename S)(
            requires (!sized_sentinel_for<I, I>)) //
        static constexpr std::pair<iter_difference_t<I>, I> //
        impl_i(I first, S last, sentinel_tag)
        {
            iter_difference_t<I> d = 0;
            for(; first != last; ++first)
                ++d;
            return {d, first};
        }
        template(typename I, typename S)(
            requires sized_sentinel_for<I, I>)
        static constexpr std::pair<iter_difference_t<I>, I> //
        impl_i(I first, S end_, sentinel_tag)
        {
            I last = ranges::next(first, end_);
            auto n = static_cast<iter_difference_t<I>>(last - first);
            RANGES_EXPECT(((bool)same_as<I, S> || 0 <= n));
            return {n, last};
        }
        template<typename I, typename S>
        static constexpr std::pair<iter_difference_t<I>, I> //
        impl_i(I first, S last, sized_sentinel_tag)
        {
            auto n = static_cast<iter_difference_t<I>>(last - first);
            RANGES_EXPECT(((bool)same_as<I, S> || 0 <= n));
            return {n, ranges::next(first, last)};
        }

    public:
        template(typename I, typename S)(
            requires sentinel_for<S, I>)
        constexpr std::pair<iter_difference_t<I>, I> operator()(I first, S last) const
        {
            return iter_enumerate_fn::impl_i(static_cast<I &&>(first),
                                             static_cast<S &&>(last),
                                             sentinel_tag_of<S, I>());
        }
    };

    /// \sa `iter_enumerate_fn`
    RANGES_INLINE_VARIABLE(iter_enumerate_fn, iter_enumerate)

    struct iter_distance_fn
    {
    private:
        template<typename I, typename S>
        static constexpr iter_difference_t<I> impl_i(I first, S last, sentinel_tag)
        {
            return iter_enumerate(static_cast<I &&>(first), static_cast<S &&>(last))
                .first;
        }
        template<typename I, typename S>
        static constexpr iter_difference_t<I> impl_i(I first, S last, sized_sentinel_tag)
        {
            auto n = static_cast<iter_difference_t<I>>(last - first);
            RANGES_EXPECT(((bool)same_as<I, S> || 0 <= n));
            return n;
        }

    public:
        template(typename I, typename S)(
            requires input_or_output_iterator<I> AND sentinel_for<S, I>)
        constexpr iter_difference_t<I> operator()(I first, S last) const
        {
            return iter_distance_fn::impl_i(static_cast<I &&>(first),
                                            static_cast<S &&>(last),
                                            sentinel_tag_of<S, I>());
        }
    };

    /// \sa `iter_distance_fn`
    RANGES_INLINE_VARIABLE(iter_distance_fn, iter_distance)

    struct iter_distance_compare_fn
    {
    private:
        template<typename I, typename S>
        static constexpr int impl_i(I first, S last, iter_difference_t<I> n, sentinel_tag)
        {
            if(n < 0)
                return 1;
            for(; n > 0; --n, ++first)
            {
                if(first == last)
                    return -1;
            }
            return first == last ? 0 : 1;
        }
        template<typename I, typename S>
        static constexpr int impl_i(I first, S last, iter_difference_t<I> n,
                                    sized_sentinel_tag)
        {
            iter_difference_t<I> dist = last - first;
            if(n < dist)
                return 1;
            if(dist < n)
                return -1;
            return 0;
        }

    public:
        template(typename I, typename S)(
            requires input_iterator<I> AND sentinel_for<S, I>)
        constexpr int operator()(I first, S last, iter_difference_t<I> n) const
        {
            return iter_distance_compare_fn::impl_i(static_cast<I &&>(first),
                                                    static_cast<S &&>(last),
                                                    n,
                                                    sentinel_tag_of<S, I>());
        }
    };

    /// \sa `iter_distance_compare_fn`
    RANGES_INLINE_VARIABLE(iter_distance_compare_fn, iter_distance_compare)

    // Like distance(b,e), but guaranteed to be O(1)
    struct iter_size_fn
    {
        template(typename I, typename S)(
            requires sized_sentinel_for<S, I>)
        constexpr meta::_t<std::make_unsigned<iter_difference_t<I>>> //
        operator()(I const & first, S last) const
        {
            using size_type = meta::_t<std::make_unsigned<iter_difference_t<I>>>;
            iter_difference_t<I> n = last - first;
            RANGES_EXPECT(0 <= n);
            return static_cast<size_type>(n);
        }
    };

    /// \sa `iter_size_fn`
    RANGES_INLINE_VARIABLE(iter_size_fn, iter_size)

    /// \cond
    namespace adl_uncounted_recounted_detail
    {
        template<typename I>
        constexpr I uncounted(I i)
        {
            return i;
        }

        template<typename I>
        constexpr I recounted(I const &, I i, iter_difference_t<I>)
        {
            return i;
        }

        struct uncounted_fn
        {
            template<typename I>
            constexpr auto operator()(I i) const -> decltype(uncounted((I &&) i))
            {
                return uncounted((I &&) i);
            }
        };

        struct recounted_fn
        {
            template<typename I, typename J>
            constexpr auto operator()(I i, J j, iter_difference_t<J> n) const
                -> decltype(recounted((I &&) i, (J &&) j, n))
            {
                return recounted((I &&) i, (J &&) j, n);
            }
        };
    } // namespace adl_uncounted_recounted_detail
    /// \endcond

    RANGES_INLINE_VARIABLE(adl_uncounted_recounted_detail::uncounted_fn, uncounted)
    RANGES_INLINE_VARIABLE(adl_uncounted_recounted_detail::recounted_fn, recounted)

    struct enumerate_fn : iter_enumerate_fn
    {
    private:
        template<typename Rng>
        static constexpr std::pair<range_difference_t<Rng>, iterator_t<Rng>> impl_r(
            Rng & rng, range_tag, range_tag)
        {
            return iter_enumerate(begin(rng), end(rng));
        }
        template<typename Rng>
        static constexpr std::pair<range_difference_t<Rng>, iterator_t<Rng>> impl_r(
            Rng & rng, common_range_tag, sized_range_tag)
        {
            return {static_cast<range_difference_t<Rng>>(size(rng)), end(rng)};
        }

    public:
        using iter_enumerate_fn::operator();

        template(typename Rng)(
            requires range<Rng>)
        constexpr std::pair<range_difference_t<Rng>, iterator_t<Rng>> operator()(Rng && rng) const
        {
            // Better not be trying to compute the distance of an infinite range:
            RANGES_EXPECT(!is_infinite<Rng>::value);
            return enumerate_fn::impl_r(
                rng, common_range_tag_of<Rng>(), sized_range_tag_of<Rng>());
        }
    };

    /// \sa `enumerate_fn`
    RANGES_INLINE_VARIABLE(enumerate_fn, enumerate)

    struct distance_fn : iter_distance_fn
    {
    private:
        template<typename Rng>
        static range_difference_t<Rng> impl_r(Rng & rng, range_tag)
        {
            return enumerate(rng).first;
        }
        template<typename Rng>
        static constexpr range_difference_t<Rng> impl_r(Rng & rng, sized_range_tag)
        {
            return static_cast<range_difference_t<Rng>>(size(rng));
        }

    public:
        using iter_distance_fn::operator();

        template(typename Rng)(
            requires range<Rng>)
        constexpr range_difference_t<Rng> operator()(Rng && rng) const
        {
            // Better not be trying to compute the distance of an infinite range:
            RANGES_EXPECT(!is_infinite<Rng>::value);
            return distance_fn::impl_r(rng, sized_range_tag_of<Rng>());
        }
    };

    /// \sa `distance_fn`
    RANGES_INLINE_VARIABLE(distance_fn, distance)

    // The interface of distance_compare is taken from Util.listLengthCmp in the GHC API.
    struct distance_compare_fn : iter_distance_compare_fn
    {
    private:
        template<typename Rng>
        static constexpr int impl_r(Rng & rng, range_difference_t<Rng> n, range_tag)
        {
            // Infinite ranges are always compared to be larger than a finite number.
            return is_infinite<Rng>::value
                       ? 1
                       : iter_distance_compare(begin(rng), end(rng), n);
        }
        template<typename Rng>
        static constexpr int impl_r(Rng & rng, range_difference_t<Rng> n, sized_range_tag)
        {
            auto dist = distance(rng); // O(1) since rng is a sized_range
            if(dist > n)
                return 1;
            else if(dist < n)
                return -1;
            else
                return 0;
        }

    public:
        using iter_distance_compare_fn::operator();

        template(typename Rng)(
            requires range<Rng>)
        constexpr int operator()(Rng && rng, range_difference_t<Rng> n) const
        {
            return distance_compare_fn::impl_r(rng, n, sized_range_tag_of<Rng>());
        }
    };

    /// \sa `distance_compare_fn`
    RANGES_INLINE_VARIABLE(distance_compare_fn, distance_compare)

    namespace cpp20
    {
        using ranges::advance;
        using ranges::distance;
        using ranges::next;
        using ranges::prev;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif // RANGES_V3_ITERATOR_OPERATIONS_HPP
