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
#ifndef RANGES_V3_ALGORITHM_CONTAINS_HPP
#define RANGES_V3_ALGORITHM_CONTAINS_HPP

#include <utility>

#include <concepts/concepts.hpp>

#include <range/v3/algorithm/find.hpp>
#include <range/v3/detail/config.hpp>
#include <range/v3/functional/comparisons.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-algorithms
    /// @{
    RANGES_FUNC_BEGIN(contains)

        /// \brief function template \c contains
        template(typename I, typename S, typename T, typename P = identity)(
            requires input_iterator<I> AND sentinel_for<S, I> AND
            indirect_relation<equal_to, projected<I, P>, const T *>)
        constexpr bool RANGES_FUNC(contains)(I first, S last, const T & val, P proj = {})
        {
            return find(std::move(first), last, val, std::move(proj)) != last;
        }

        /// \overload
        template(typename Rng, typename T, typename P = identity)(
            requires input_range<Rng> AND
            indirect_relation<equal_to, projected<iterator_t<Rng>, P>, const T *>)
        constexpr bool RANGES_FUNC(contains)(Rng && rng, const T & val, P proj = {})
        {
            return (*this)(begin(rng), end(rng), val, std::move(proj));
        }

    RANGES_FUNC_END(contains)
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif // RANGES_V3_ALGORITHM_CONTAINS_HPP
