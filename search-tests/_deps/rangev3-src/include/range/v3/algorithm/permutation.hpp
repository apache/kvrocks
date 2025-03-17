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
//===-------------------------- algorithm ---------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
#ifndef RANGES_V3_ALGORITHM_PERMUTATION_HPP
#define RANGES_V3_ALGORITHM_PERMUTATION_HPP

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/mismatch.hpp>
#include <range/v3/algorithm/reverse.hpp>
#include <range/v3/functional/comparisons.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/utility/swap.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-algorithms
    /// @{

    /// \cond
    namespace detail
    {
        template<typename I1, typename S1, typename I2, typename S2, typename C,
                 typename P1, typename P2>
        constexpr bool is_permutation_impl(I1 begin1, 
                                           S1 end1, 
                                           I2 begin2, 
                                           S2 end2, 
                                           C pred, 
                                           P1 proj1,
                                           P2 proj2)
        {
            // shorten sequences as much as possible by lopping off any equal parts
            const auto mismatch =
                ranges::mismatch(begin1, end1, begin2, end2, pred, proj1, proj2);
            begin1 = mismatch.in1;
            begin2 = mismatch.in2;
            if(begin1 == end1 || begin2 == end2)
            {
                return begin1 == end1 && begin2 == end2;
            }
            // begin1 != end1 && begin2 != end2 && *begin1 != *begin2
            auto l1 = distance(begin1, end1);
            auto l2 = distance(begin2, end2);
            if(l1 != l2)
                return false;

            // For each element in [f1, l1) see if there are the same number of
            //    equal elements in [f2, l2)
            for(I1 i = begin1; i != end1; ++i)
            {
                // Have we already counted the number of *i in [f1, l1)?
                const auto should_continue = [&] {
                    for(I1 j = begin1; j != i; ++j)
                        if(invoke(pred, invoke(proj1, *j), invoke(proj1, *i)))
                            return true;
                    return false;
                }();
                if(should_continue)
                {
                    continue;
                }
                // Count number of *i in [f2, l2)
                iter_difference_t<I2> c2 = 0;
                for(I2 j = begin2; j != end2; ++j)
                    if(invoke(pred, invoke(proj1, *i), invoke(proj2, *j)))
                        ++c2;
                if(c2 == 0)
                    return false;
                // Count number of *i in [i, l1) (we can start with 1)
                iter_difference_t<I1> c1 = 1;
                for(I1 j = next(i); j != end1; ++j)
                    if(invoke(pred, invoke(proj1, *i), invoke(proj1, *j)))
                        ++c1;
                if(c1 != c2)
                    return false;
            }
            return true;
        }
    } // namespace detail
    /// \endcond

    RANGES_FUNC_BEGIN(is_permutation)

        /// \brief function template \c is_permutation
        template(typename I1,
                 typename S1,
                 typename I2,
                 typename C = equal_to,
                 typename P1 = identity,
                 typename P2 = identity)(
            requires forward_iterator<I1> AND sentinel_for<S1, I1> AND
                forward_iterator<I2> AND indirectly_comparable<I1, I2, C, P1, P2>)
        RANGES_DEPRECATED(
            "Use the variant of ranges::is_permutation that takes an upper bound "
            "for both sequences")
        bool RANGES_FUNC(is_permutation)(I1 begin1,
                                         S1 end1,
                                         I2 begin2,
                                         C pred = C{},
                                         P1 proj1 = P1{},
                                         P2 proj2 = P2{}) //
        {
            // shorten sequences as much as possible by lopping off any equal parts
            for(; begin1 != end1; ++begin1, ++begin2)
                if(!invoke(pred, invoke(proj1, *begin1), invoke(proj2, *begin2)))
                    goto not_done;
            return true;
        not_done:
            // begin1 != end1 && *begin1 != *begin2
            auto l1 = distance(begin1, end1);
            if(l1 == 1)
                return false;
            I2 end2 = next(begin2, l1);
            // For each element in [f1, l1) see if there are the same number of
            //    equal elements in [f2, l2)
            for(I1 i = begin1; i != end1; ++i)
            {
                // Have we already counted the number of *i in [f1, l1)?
                for(I1 j = begin1; j != i; ++j)
                    if(invoke(pred, invoke(proj1, *j), invoke(proj1, *i)))
                        goto next_iter;
                {
                    // Count number of *i in [f2, l2)
                    iter_difference_t<I2> c2 = 0;
                    for(I2 j = begin2; j != end2; ++j)
                        if(invoke(pred, invoke(proj1, *i), invoke(proj2, *j)))
                            ++c2;
                    if(c2 == 0)
                        return false;
                    // Count number of *i in [i, l1) (we can start with 1)
                    iter_difference_t<I1> c1 = 1;
                    for(I1 j = next(i); j != end1; ++j)
                        if(invoke(pred, invoke(proj1, *i), invoke(proj1, *j)))
                            ++c1;
                    if(c1 != c2)
                        return false;
                }
            next_iter:;
            }
            return true;
        }

        /// \overload
        template(typename I1,
                 typename S1,
                 typename I2,
                 typename S2,
                 typename C = equal_to,
                 typename P1 = identity,
                 typename P2 = identity)(
            requires forward_iterator<I1> AND sentinel_for<S1, I1> AND
                forward_iterator<I2> AND sentinel_for<S2, I2> AND
                indirectly_comparable<I1, I2, C, P1, P2>)
        constexpr bool RANGES_FUNC(is_permutation)(I1 begin1,
                                                   S1 end1,
                                                   I2 begin2,
                                                   S2 end2,
                                                   C pred = C{},
                                                   P1 proj1 = P1{},
                                                   P2 proj2 = P2{}) //
        {
            if(RANGES_CONSTEXPR_IF(sized_sentinel_for<S1, I1> &&
                                   sized_sentinel_for<S2, I2>))
            {
                RANGES_DIAGNOSTIC_PUSH
                RANGES_DIAGNOSTIC_IGNORE_DEPRECATED_DECLARATIONS
                return distance(begin1, end1) == distance(begin2, end2) &&
                       (*this)(std::move(begin1),
                               std::move(end1),
                               std::move(begin2),
                               std::move(pred),
                               std::move(proj1),
                               std::move(proj2));
                RANGES_DIAGNOSTIC_POP
            }
            return detail::is_permutation_impl(std::move(begin1),
                                               std::move(end1),
                                               std::move(begin2),
                                               std::move(end2),
                                               std::move(pred),
                                               std::move(proj1),
                                               std::move(proj2));
        }

        /// \overload
        template(typename Rng1,
                     typename I2Ref,
                     typename C = equal_to,
                     typename P1 = identity,
                     typename P2 = identity)(
            requires forward_range<Rng1> AND forward_iterator<uncvref_t<I2Ref>> AND
                indirectly_comparable<iterator_t<Rng1>, uncvref_t<I2Ref>, C, P1, P2>)
        RANGES_DEPRECATED(
            "Use the variant of ranges::is_permutation that takes an upper bound "
            "for both sequences")
        bool RANGES_FUNC(is_permutation)(Rng1 && rng1,
                                         I2Ref && begin2,
                                         C pred = C{},
                                         P1 proj1 = P1{},
                                         P2 proj2 = P2{}) //
        {
            RANGES_DIAGNOSTIC_PUSH
            RANGES_DIAGNOSTIC_IGNORE_DEPRECATED_DECLARATIONS
            return (*this)(begin(rng1),
                           end(rng1),
                           (I2Ref &&) begin2,
                           std::move(pred),
                           std::move(proj1),
                           std::move(proj2));
            RANGES_DIAGNOSTIC_POP
        }

        /// \overload
        template(typename Rng1,
                     typename Rng2,
                     typename C = equal_to,
                     typename P1 = identity,
                     typename P2 = identity)(
            requires forward_range<Rng1> AND forward_range<Rng2> AND
                indirectly_comparable<iterator_t<Rng1>, iterator_t<Rng2>, C, P1, P2>)
        constexpr bool RANGES_FUNC(is_permutation)(
            Rng1 && rng1, Rng2 && rng2, C pred = C{}, P1 proj1 = P1{}, P2 proj2 = P2{}) //
        {
            if(RANGES_CONSTEXPR_IF(sized_range<Rng1> && sized_range<Rng2>))
            {
                RANGES_DIAGNOSTIC_PUSH
                RANGES_DIAGNOSTIC_IGNORE_DEPRECATED_DECLARATIONS
                return distance(rng1) == distance(rng2) && (*this)(begin(rng1),
                                                                   end(rng1),
                                                                   begin(rng2),
                                                                   std::move(pred),
                                                                   std::move(proj1),
                                                                   std::move(proj2));
                RANGES_DIAGNOSTIC_POP
            }
            return detail::is_permutation_impl(begin(rng1),
                                               end(rng1),
                                               begin(rng2),
                                               end(rng2),
                                               std::move(pred),
                                               std::move(proj1),
                                               std::move(proj2));
        }

    RANGES_FUNC_END(is_permutation)

    RANGES_FUNC_BEGIN(next_permutation)

        /// \brief function template \c next_permutation
        template(typename I, typename S, typename C = less, typename P = identity)(
            requires bidirectional_iterator<I> AND sentinel_for<S, I> AND
                sortable<I, C, P>)
        constexpr bool RANGES_FUNC(next_permutation)(I first, S end_, C pred = C{}, P proj = P{}) //
        {
            if(first == end_)
                return false;
            I last = ranges::next(first, end_), i = last;
            if(first == --i)
                return false;
            while(true)
            {
                I ip1 = i;
                if(invoke(pred, invoke(proj, *--i), invoke(proj, *ip1)))
                {
                    I j = last;
                    while(!invoke(pred, invoke(proj, *i), invoke(proj, *--j)))
                        ;
                    ranges::iter_swap(i, j);
                    ranges::reverse(ip1, last);
                    return true;
                }
                if(i == first)
                {
                    ranges::reverse(first, last);
                    return false;
                }
            }
        }

        /// \overload
        template(typename Rng, typename C = less, typename P = identity)(
            requires bidirectional_range<Rng> AND sortable<iterator_t<Rng>, C, P>)
        constexpr bool RANGES_FUNC(next_permutation)(Rng && rng, C pred = C{}, P proj = P{}) //
        {
            return (*this)(begin(rng), end(rng), std::move(pred), std::move(proj));
        }

    RANGES_FUNC_END(next_permutation)

    RANGES_FUNC_BEGIN(prev_permutation)

        /// \brief function template \c prev_permutation
        template(typename I, typename S, typename C = less, typename P = identity)(
            requires bidirectional_iterator<I> AND sentinel_for<S, I> AND
                sortable<I, C, P>)
        constexpr bool RANGES_FUNC(prev_permutation)(I first, S end_, C pred = C{}, P proj = P{}) //
        {
            if(first == end_)
                return false;
            I last = ranges::next(first, end_), i = last;
            if(first == --i)
                return false;
            while(true)
            {
                I ip1 = i;
                if(invoke(pred, invoke(proj, *ip1), invoke(proj, *--i)))
                {
                    I j = last;
                    while(!invoke(pred, invoke(proj, *--j), invoke(proj, *i)))
                        ;
                    ranges::iter_swap(i, j);
                    ranges::reverse(ip1, last);
                    return true;
                }
                if(i == first)
                {
                    ranges::reverse(first, last);
                    return false;
                }
            }
        }

        /// \overload
        template(typename Rng, typename C = less, typename P = identity)(
            requires bidirectional_range<Rng> AND sortable<iterator_t<Rng>, C, P>)
        constexpr bool RANGES_FUNC(prev_permutation)(Rng && rng, C pred = C{}, P proj = P{}) //
        {
            return (*this)(begin(rng), end(rng), std::move(pred), std::move(proj));
        }

    RANGES_FUNC_END(prev_permutation)

    namespace cpp20
    {
        using ranges::is_permutation;
        using ranges::next_permutation;
        using ranges::prev_permutation;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
