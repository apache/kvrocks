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
//  Copyright (c) 1994
//  Hewlett-Packard Company
//
//  Permission to use, copy, modify, distribute and sell this software
//  and its documentation for any purpose is hereby granted without fee,
//  provided that the above copyright notice appear in all copies and
//  that both that copyright notice and this permission notice appear
//  in supporting documentation.  Hewlett-Packard Company makes no
//  representations about the suitability of this software for any
//  purpose.  It is provided "as is" without express or implied warranty.
//
//  Copyright (c) 1996
//  Silicon Graphics Computer Systems, Inc.
//
//  Permission to use, copy, modify, distribute and sell this software
//  and its documentation for any purpose is hereby granted without fee,
//  provided that the above copyright notice appear in all copies and
//  that both that copyright notice and this permission notice appear
//  in supporting documentation.  Silicon Graphics makes no
//  representations about the suitability of this software for any
//  purpose.  It is provided "as is" without express or implied warranty.
//

#ifndef RANGES_V3_ALGORITHM_SORT_HPP
#define RANGES_V3_ALGORITHM_SORT_HPP

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/heap_algorithm.hpp>
#include <range/v3/algorithm/move_backward.hpp>
#include <range/v3/algorithm/partial_sort.hpp>
#include <range/v3/functional/comparisons.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/dangling.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
        template<typename I, typename C, typename P>
        inline constexpr I unguarded_partition(I first, I last, C & pred, P & proj)
        {
            I mid = first + (last - first) / 2, penultimate = ranges::prev(last);
            auto &&x = *first, &&y = *mid, &&z = *penultimate;
            auto &&a = invoke(proj, (decltype(x) &&)x),
                 &&b = invoke(proj, (decltype(y) &&)y),
                 &&c = invoke(proj, (decltype(z) &&)z);

            // Find the median:
            I pivot_pnt =
                invoke(pred, a, b)
                    ? (invoke(pred, b, c) ? mid
                                          : (invoke(pred, a, c) ? penultimate : first))
                    : (invoke(pred, a, c) ? first
                                          : (invoke(pred, b, c) ? penultimate : mid));

            // Do the partition:
            while(true)
            {
                auto && v = *pivot_pnt;
                auto && pivot = invoke(proj, (decltype(v) &&)v);
                while(invoke(pred, invoke(proj, *first), pivot))
                    ++first;
                --last;
                while(invoke(pred, pivot, invoke(proj, *last)))
                    --last;
                if(!(first < last))
                    return first;
                ranges::iter_swap(first, last);
                pivot_pnt =
                    pivot_pnt == first ? last : (pivot_pnt == last ? first : pivot_pnt);
                ++first;
            }
        }

        template<typename I, typename C, typename P>
        inline constexpr void unguarded_linear_insert(I last, 
                                                      iter_value_t<I> val, 
                                                      C & pred,
                                                      P & proj)
        {
            I next_ = prev(last);
            while(invoke(pred, invoke(proj, val), invoke(proj, *next_)))
            {
                *last = iter_move(next_);
                last = next_;
                --next_;
            }
            *last = std::move(val);
        }

        template<typename I, typename C, typename P>
        inline constexpr void linear_insert(I first, I last, C & pred, P & proj)
        {
            iter_value_t<I> val = iter_move(last);
            if(invoke(pred, invoke(proj, val), invoke(proj, *first)))
            {
                move_backward(first, last, last + 1);
                *first = std::move(val);
            }
            else
                detail::unguarded_linear_insert(last, std::move(val), pred, proj);
        }

        template<typename I, typename C, typename P>
        inline constexpr void insertion_sort(I first, I last, C & pred, P & proj)
        {
            if(first == last)
                return;
            for(I i = next(first); i != last; ++i)
                detail::linear_insert(first, i, pred, proj);
        }

        template<typename I, typename C, typename P>
        inline constexpr void unguarded_insertion_sort(I first, I last, C & pred, P & proj)
        {
            for(I i = first; i != last; ++i)
                detail::unguarded_linear_insert(i, iter_move(i), pred, proj);
        }

        constexpr int introsort_threshold()
        {
            return 16;
        }

        template<typename I, typename C, typename P>
        inline constexpr void final_insertion_sort(I first, I last, C & pred, P & proj)
        {
            if(last - first > detail::introsort_threshold())
            {
                detail::insertion_sort(
                    first, first + detail::introsort_threshold(), pred, proj);
                detail::unguarded_insertion_sort(
                    first + detail::introsort_threshold(), last, pred, proj);
            }
            else
                detail::insertion_sort(first, last, pred, proj);
        }

        template<typename Size>
        inline constexpr Size log2(Size n)
        {
            Size k = 0;
            for(; n != 1; n >>= 1)
                ++k;
            return k;
        }

        template<typename I, typename Size, typename C, typename P>
        inline constexpr void introsort_loop(I first, I last, Size depth_limit, C & pred, P & proj)
        {
            while(last - first > detail::introsort_threshold())
            {
                if(depth_limit == 0)
                    return partial_sort(
                               first, last, last, std::ref(pred), std::ref(proj)),
                           void();
                I cut = detail::unguarded_partition(first, last, pred, proj);
                detail::introsort_loop(cut, last, --depth_limit, pred, proj);
                last = cut;
            }
        }
    } // namespace detail
    /// \endcond

    /// \addtogroup group-algorithms
    /// @{

    // Introsort: Quicksort to a certain depth, then Heapsort. Insertion
    // sort below a certain threshold.
    // TODO Forward iterators, like EoP?

    RANGES_FUNC_BEGIN(sort)

        /// \brief function template \c sort
        template(typename I, typename S, typename C = less, typename P = identity)(
            requires sortable<I, C, P> AND random_access_iterator<I> AND
                sentinel_for<S, I>)
        constexpr I RANGES_FUNC(sort)(I first, S end_, C pred = C{}, P proj = P{})
        {
            I last = ranges::next(first, std::move(end_));
            if(first != last)
            {
                detail::introsort_loop(
                    first, last, detail::log2(last - first) * 2, pred, proj);
                detail::final_insertion_sort(first, last, pred, proj);
            }
            return last;
        }

        /// \overload
        template(typename Rng, typename C = less, typename P = identity)(
            requires sortable<iterator_t<Rng>, C, P> AND random_access_range<Rng>)
        constexpr borrowed_iterator_t<Rng> //
        RANGES_FUNC(sort)(Rng && rng, C pred = C{}, P proj = P{}) //
        {
            return (*this)(begin(rng), end(rng), std::move(pred), std::move(proj));
        }

    RANGES_FUNC_END(sort)

    namespace cpp20
    {
        using ranges::sort;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
