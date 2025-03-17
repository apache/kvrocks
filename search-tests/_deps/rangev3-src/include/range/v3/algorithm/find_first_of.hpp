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
#ifndef RANGES_V3_ALGORITHM_FIND_FIRST_OF_HPP
#define RANGES_V3_ALGORITHM_FIND_FIRST_OF_HPP

#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/comparisons.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/dangling.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-algorithms
    /// @{
    RANGES_FUNC_BEGIN(find_first_of)
        // Rationale: return I0 instead of pair<I0,I1> because find_first_of need
        // not actually compute the end of [I1,S0); therefore, it is not necessarily
        // losing information. E.g., if begin0 == end0, we can return begin0 immediately.
        // If we returned pair<I0,I1>, we would need to do an O(N) scan to find the
        // end position.

        /// \brief function template \c find_first_of
        template(typename I0,
                 typename S0,
                 typename I1,
                 typename S1,
                 typename R = equal_to,
                 typename P0 = identity,
                 typename P1 = identity)(
            requires input_iterator<I0> AND sentinel_for<S0, I0> AND
                forward_iterator<I1> AND sentinel_for<S1, I1> AND
                indirect_relation<R, projected<I0, P0>, projected<I1, P1>>)
        constexpr I0 RANGES_FUNC(find_first_of)(I0 begin0,
                                                S0 end0,
                                                I1 begin1,
                                                S1 end1,
                                                R pred = R{},
                                                P0 proj0 = P0{},
                                                P1 proj1 = P1{}) //
        {
            for(; begin0 != end0; ++begin0)
                for(auto tmp = begin1; tmp != end1; ++tmp)
                    if(invoke(pred, invoke(proj0, *begin0), invoke(proj1, *tmp)))
                        return begin0;
            return begin0;
        }

        /// \overload
        template(typename Rng0,
                     typename Rng1,
                     typename R = equal_to,
                     typename P0 = identity,
                     typename P1 = identity)(
            requires input_range<Rng0> AND forward_range<Rng1> AND
                indirect_relation<R,
                                  projected<iterator_t<Rng0>, P0>,
                                  projected<iterator_t<Rng1>, P1>>)
        constexpr borrowed_iterator_t<Rng0> RANGES_FUNC(find_first_of)(
            Rng0 && rng0, Rng1 && rng1, R pred = R{}, P0 proj0 = P0{}, P1 proj1 = P1{}) //
        {
            return (*this)(begin(rng0),
                           end(rng0),
                           begin(rng1),
                           end(rng1),
                           std::move(pred),
                           std::move(proj0),
                           std::move(proj1));
        }

    RANGES_FUNC_END(find_first_of)

    namespace cpp20
    {
        using ranges::find_first_of;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
