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
#ifndef RANGES_V3_ALGORITHM_SEARCH_HPP
#define RANGES_V3_ALGORITHM_SEARCH_HPP

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/comparisons.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/subrange.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-algorithms
    /// @{

    /// \cond
    namespace detail
    {
        template<typename I1, typename S1, typename D1, typename I2, typename S2,
                 typename D2, typename C, typename P1, typename P2>
        constexpr subrange<I1> search_sized_impl(I1 const begin1_, 
                                                 S1 end1,
                                                 D1 const d1_,
                                                 I2 begin2, 
                                                 S2 end2, 
                                                 D2 d2, 
                                                 C & pred,
                                                 P1 & proj1, 
                                                 P2 & proj2)
        {
            D1 d1 = d1_;
            auto begin1 = uncounted(begin1_);
            while(true)
            {
                // Find first element in sequence 1 that matches *begin2, with a mininum
                // of loop checks
                while(true)
                {
                    if(d1 < d2) // return the last if we've run out of room
                    {
                        auto e =
                            ranges::next(recounted(begin1_, std::move(begin1), d1_ - d1),
                                         std::move(end1));
                        return {e, e};
                    }
                    if(invoke(pred, invoke(proj1, *begin1), invoke(proj2, *begin2)))
                        break;
                    ++begin1;
                    --d1;
                }
                // *begin1 matches *begin2, now match elements after here
                auto m1 = begin1;
                I2 m2 = begin2;
                while(true)
                {
                    if(++m2 == end2) // If pattern exhausted, begin1 is the answer (works
                                     // for 1 element pattern)
                    {
                        return {recounted(begin1_, std::move(begin1), d1_ - d1),
                                recounted(begin1_, std::move(++m1), d1_ - d1)};
                    }
                    ++m1; // No need to check, we know we have room to match successfully
                    if(!invoke(
                           pred,
                           invoke(proj1, *m1),
                           invoke(
                               proj2,
                               *m2))) // if there is a mismatch, restart with a new begin1
                    {
                        ++begin1;
                        --d1;
                        break;
                    } // else there is a match, check next elements
                }
            }
        }

        template<typename I1, typename S1, typename I2, typename S2, typename C,
                 typename P1, typename P2>
        constexpr subrange<I1> search_impl(I1 begin1, 
                                           S1 end1, 
                                           I2 begin2, 
                                           S2 end2, 
                                           C & pred,
                                           P1 & proj1, 
                                           P2 & proj2)
        {
            while(true)
            {
                // Find first element in sequence 1 that matches *begin2, with a mininum
                // of loop checks
                while(true)
                {
                    if(begin1 == end1) // return end1 if no element matches *begin2
                        return {begin1, begin1};
                    if(invoke(pred, invoke(proj1, *begin1), invoke(proj2, *begin2)))
                        break;
                    ++begin1;
                }
                // *begin1 matches *begin2, now match elements after here
                I1 m1 = begin1;
                I2 m2 = begin2;
                while(true)
                {
                    if(++m2 == end2) // If pattern exhausted, begin1 is the answer (works
                                     // for 1 element pattern)
                        return {begin1, ++m1};
                    if(++m1 == end1) // Otherwise if source exhausted, pattern not found
                        return {m1, m1};
                    if(!invoke(
                           pred,
                           invoke(proj1, *m1),
                           invoke(
                               proj2,
                               *m2))) // if there is a mismatch, restart with a new begin1
                    {
                        ++begin1;
                        break;
                    } // else there is a match, check next elements
                }
            }
        }
    } // namespace detail
    /// \endcond

    RANGES_FUNC_BEGIN(search)

        /// \brief function template \c search
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
        constexpr subrange<I1> RANGES_FUNC(search)(I1 begin1,
                                                   S1 end1,
                                                   I2 begin2,
                                                   S2 end2,
                                                   C pred = C{},
                                                   P1 proj1 = P1{},
                                                   P2 proj2 = P2{}) //
        {
            if(begin2 == end2)
                return {begin1, begin1};
            if(RANGES_CONSTEXPR_IF(sized_sentinel_for<S1, I1> &&
                                   sized_sentinel_for<S2, I2>))
                return detail::search_sized_impl(std::move(begin1),
                                                 std::move(end1),
                                                 distance(begin1, end1),
                                                 std::move(begin2),
                                                 std::move(end2),
                                                 distance(begin2, end2),
                                                 pred,
                                                 proj1,
                                                 proj2);
            else
                return detail::search_impl(std::move(begin1),
                                           std::move(end1),
                                           std::move(begin2),
                                           std::move(end2),
                                           pred,
                                           proj1,
                                           proj2);
        }

        /// \overload
        template(typename Rng1,
                 typename Rng2,
                 typename C = equal_to,
                 typename P1 = identity,
                 typename P2 = identity)(
            requires forward_range<Rng1> AND forward_range<Rng2> AND
                indirectly_comparable<iterator_t<Rng1>, iterator_t<Rng2>, C, P1, P2>)
        constexpr borrowed_subrange_t<Rng1> RANGES_FUNC(search)(
            Rng1 && rng1, Rng2 && rng2, C pred = C{}, P1 proj1 = P1{}, P2 proj2 = P2{}) //
        {
            if(empty(rng2))
                return subrange<iterator_t<Rng1>>{begin(rng1), begin(rng1)};
            if(RANGES_CONSTEXPR_IF(sized_range<Rng1> && sized_range<Rng2>))
                return detail::search_sized_impl(begin(rng1),
                                                 end(rng1),
                                                 distance(rng1),
                                                 begin(rng2),
                                                 end(rng2),
                                                 distance(rng2),
                                                 pred,
                                                 proj1,
                                                 proj2);
            else
                return detail::search_impl(
                    begin(rng1), end(rng1), begin(rng2), end(rng2), pred, proj1, proj2);
        }

    RANGES_FUNC_END(search)

    namespace cpp20
    {
        using ranges::search;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
