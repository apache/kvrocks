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

//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#ifndef RANGES_V3_ALGORITHM_NTH_ELEMENT_HPP
#define RANGES_V3_ALGORITHM_NTH_ELEMENT_HPP

#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/min_element.hpp>
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
#include <range/v3/utility/optional.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/utility/swap.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
        // stable, 2-3 compares, 0-2 swaps

        template(typename I, typename C, typename P)(
            requires forward_iterator<I> AND indirect_relation<C, projected<I, P>>)
        unsigned sort3(I x, I y, I z, C & pred, P & proj)
        {
            unsigned r = 0;
            if(!invoke(pred, invoke(proj, *y), invoke(proj, *x))) // if x <= y
            {
                if(!invoke(pred, invoke(proj, *z), invoke(proj, *y))) // if y <= z
                    return r;                                         // x <= y && y <= z
                                                                      // x <= y && y > z
                ranges::iter_swap(y, z);                              // x <= z && y < z
                r = 1;
                if(invoke(pred, invoke(proj, *y), invoke(proj, *x))) // if x > y
                {
                    ranges::iter_swap(x, y); // x < y && y <= z
                    r = 2;
                }
                return r; // x <= y && y < z
            }
            if(invoke(pred, invoke(proj, *z), invoke(proj, *y))) // x > y, if y > z
            {
                ranges::iter_swap(x, z); // x < y && y < z
                r = 1;
                return r;
            }
            ranges::iter_swap(x, y);                             // x > y && y <= z
            r = 1;                                               // x < y && x <= z
            if(invoke(pred, invoke(proj, *z), invoke(proj, *y))) // if y > z
            {
                ranges::iter_swap(y, z); // x <= y && y < z
                r = 2;
            }
            return r;
        } // x <= y && y <= z

        template(typename I, typename C, typename P)(
            requires bidirectional_iterator<I> AND indirect_relation<C, projected<I, P>>)
        void selection_sort(I first, I last, C & pred, P & proj)
        {
            RANGES_EXPECT(first != last);
            for(I lm1 = ranges::prev(last); first != lm1; ++first)
            {
                I i = ranges::min_element(first, last, std::ref(pred), std::ref(proj));
                if(i != first)
                    ranges::iter_swap(first, i);
            }
        }
    } // namespace detail
    /// \endcond

    /// \addtogroup group-algorithms
    /// @{
    RANGES_FUNC_BEGIN(nth_element)

        /// \brief function template \c nth_element
        template(typename I, typename S, typename C = less, typename P = identity)(
            requires random_access_iterator<I> AND sortable<I, C, P>)
        constexpr I RANGES_FUNC(nth_element)(
            I first, I nth, S end_, C pred = C{}, P proj = P{}) //
        {
            I last = ranges::next(nth, end_), end_orig = last;
            // C is known to be a reference type
            using difference_type = iter_difference_t<I>;
            difference_type const limit = 7;
            while(true)
            {
                if(nth == last)
                    return end_orig;
                difference_type len = last - first;
                switch(len)
                {
                case 0:
                case 1:
                    return end_orig;
                case 2:
                    if(invoke(pred, invoke(proj, *--last), invoke(proj, *first)))
                        ranges::iter_swap(first, last);
                    return end_orig;
                case 3:
                {
                    I m = first;
                    detail::sort3(first, ++m, --last, pred, proj);
                    return end_orig;
                }
                }
                if(len <= limit)
                {
                    detail::selection_sort(first, last, pred, proj);
                    return end_orig;
                }
                // len > limit >= 3
                I m = first + len / 2;
                I lm1 = last;
                unsigned n_swaps = detail::sort3(first, m, --lm1, pred, proj);
                // *m is median
                // partition [first, m) < *m and *m <= [m, last)
                //(this inhibits tossing elements equivalent to m around unnecessarily)
                I i = first;
                I j = lm1;
                // j points beyond range to be tested, *lm1 is known to be <= *m
                // The search going up is known to be guarded but the search coming down
                // isn't. Prime the downward search with a guard.
                if(!invoke(pred, invoke(proj, *i), invoke(proj, *m))) // if *first == *m
                {
                    // *first == *m, *first doesn't go in first part
                    // manually guard downward moving j against i
                    while(true)
                    {
                        if(i == --j)
                        {
                            // *first == *m, *m <= all other elements
                            // Parition instead into [first, i) == *first and *first < [i,
                            // last)
                            ++i; // first + 1
                            j = last;
                            if(!invoke(
                                   pred,
                                   invoke(proj, *first),
                                   invoke(
                                       proj,
                                       *--j))) // we need a guard if *first == *(last-1)
                            {
                                while(true)
                                {
                                    if(i == j)
                                        return end_orig; // [first, last) all equivalent
                                                         // elements
                                    if(invoke(
                                           pred, invoke(proj, *first), invoke(proj, *i)))
                                    {
                                        ranges::iter_swap(i, j);
                                        ++n_swaps;
                                        ++i;
                                        break;
                                    }
                                    ++i;
                                }
                            }
                            // [first, i) == *first and *first < [j, last) and j == last -
                            // 1
                            if(i == j)
                                return end_orig;
                            while(true)
                            {
                                while(
                                    !invoke(pred, invoke(proj, *first), invoke(proj, *i)))
                                    ++i;
                                while(invoke(
                                    pred, invoke(proj, *first), invoke(proj, *--j)))
                                    ;
                                if(i >= j)
                                    break;
                                ranges::iter_swap(i, j);
                                ++n_swaps;
                                ++i;
                            }
                            // [first, i) == *first and *first < [i, last)
                            // The first part is sorted,
                            if(nth < i)
                                return end_orig;
                            // nth_element the second part
                            // nth_element<C>(i, nth, last, pred);
                            first = i;
                            continue;
                        }
                        if(invoke(pred, invoke(proj, *j), invoke(proj, *m)))
                        {
                            ranges::iter_swap(i, j);
                            ++n_swaps;
                            break; // found guard for downward moving j, now use unguarded
                                   // partition
                        }
                    }
                }
                ++i;
                // j points beyond range to be tested, *lm1 is known to be <= *m
                // if not yet partitioned...
                if(i < j)
                {
                    // known that *(i - 1) < *m
                    while(true)
                    {
                        // m still guards upward moving i
                        while(invoke(pred, invoke(proj, *i), invoke(proj, *m)))
                            ++i;
                        // It is now known that a guard exists for downward moving j
                        while(!invoke(pred, invoke(proj, *--j), invoke(proj, *m)))
                            ;
                        if(i >= j)
                            break;
                        ranges::iter_swap(i, j);
                        ++n_swaps;
                        // It is known that m != j
                        // If m just moved, follow it
                        if(m == i)
                            m = j;
                        ++i;
                    }
                }
                // [first, i) < *m and *m <= [i, last)
                if(i != m && invoke(pred, invoke(proj, *m), invoke(proj, *i)))
                {
                    ranges::iter_swap(i, m);
                    ++n_swaps;
                }
                // [first, i) < *i and *i <= [i+1, last)
                if(nth == i)
                    return end_orig;
                const auto optional_return = [&]() -> ranges::optional<I> {
                    if(n_swaps == 0)
                    {
                        // We were given a perfectly partitioned sequence.  Coincidence?
                        if(nth < i)
                        {
                            // Check for [first, i) already sorted
                            j = m = first;
                            while(++j != i)
                            {
                                if(invoke(pred, invoke(proj, *j), invoke(proj, *m)))
                                    // not yet sorted, so sort
                                    return ranges::nullopt;
                                m = j;
                            }
                            // [first, i) sorted
                            return end_orig;
                        }
                        else
                        {
                            // Check for [i, last) already sorted
                            j = m = i;
                            while(++j != last)
                            {
                                if(invoke(pred, invoke(proj, *j), invoke(proj, *m)))
                                    // not yet sorted, so sort
                                    return ranges::nullopt;
                                m = j;
                            }
                            // [i, last) sorted
                            return end_orig;
                        }
                    }
                    return ranges::nullopt;
                }();
                if(optional_return)
                {
                    return *optional_return;
                }
                // nth_element on range containing nth
                if(nth < i)
                {
                    // nth_element<C>(first, nth, i, pred);
                    last = i;
                }
                else
                {
                    // nth_element<C>(i+1, nth, last, pred);
                    first = ++i;
                }
            }
            return end_orig;
        }

        /// \overload
        template(typename Rng, typename C = less, typename P = identity)(
            requires random_access_range<Rng> AND sortable<iterator_t<Rng>, C, P>)
        constexpr borrowed_iterator_t<Rng> RANGES_FUNC(nth_element)(
            Rng && rng, iterator_t<Rng> nth, C pred = C{}, P proj = P{}) //
        {
            return (*this)(
                begin(rng), std::move(nth), end(rng), std::move(pred), std::move(proj));
        }

    RANGES_FUNC_END(nth_element)

    namespace cpp20
    {
        using ranges::nth_element;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
