/// \file
// Range v3 library
//
//  Copyright Johel Guerrero 2019
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
#ifndef RANGES_V3_ALGORITHM_ENDS_WITH_HPP
#define RANGES_V3_ALGORITHM_ENDS_WITH_HPP

#include <utility>

#include <concepts/concepts.hpp>

#include <range/v3/algorithm/equal.hpp>
#include <range/v3/detail/config.hpp>
#include <range/v3/functional/comparisons.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-algorithms
    /// @{
    RANGES_FUNC_BEGIN(ends_with)

        /// \brief function template \c ends_with
        template(typename I0,
                 typename S0,
                 typename I1,
                 typename S1,
                 typename C = equal_to,
                 typename P0 = identity,
                 typename P1 = identity)(
            requires ((forward_iterator<I0> && sentinel_for<S0, I0>) ||
                      (input_iterator<I0> && sized_sentinel_for<S0, I0>)) AND
                ((forward_iterator<I1> && sentinel_for<S1, I1>) ||
                 (input_iterator<I1> && sized_sentinel_for<S1, I1>)) AND
                indirectly_comparable<I0, I1, C, P0, P1>)
        constexpr bool RANGES_FUNC(ends_with)(I0 begin0,
                                              S0 end0,
                                              I1 begin1,
                                              S1 end1,
                                              C pred = C{},
                                              P0 proj0 = P0{},
                                              P1 proj1 = P1{}) //
        {
            const auto drop = distance(begin0, end0) - distance(begin1, end1);
            if(drop < 0)
                return false;
            return equal(next(std::move(begin0), drop),
                         std::move(end0),
                         std::move(begin1),
                         std::move(end1),
                         std::move(pred),
                         std::move(proj0),
                         std::move(proj1));
        }

        /// \overload
        template(typename Rng0,
                 typename Rng1,
                 typename C = equal_to,
                 typename P0 = identity,
                 typename P1 = identity)(
            requires (forward_range<Rng0> || (input_range<Rng0> && sized_range<Rng0>)) AND
                (forward_range<Rng1> || (input_range<Rng1> && sized_range<Rng1>)) AND
                indirectly_comparable<iterator_t<Rng0>, iterator_t<Rng1>, C, P0, P1>)
        constexpr bool RANGES_FUNC(ends_with)(
            Rng0 && rng0, Rng1 && rng1, C pred = C{}, P0 proj0 = P0{}, P1 proj1 = P1{}) //
        {
            const auto drop = distance(rng0) - distance(rng1);
            if(drop < 0)
                return false;
            return equal(next(begin(rng0), drop),
                         end(rng0),
                         begin(rng1),
                         end(rng1),
                         std::move(pred),
                         std::move(proj0),
                         std::move(proj1));
        }

    RANGES_FUNC_END(ends_with)
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
