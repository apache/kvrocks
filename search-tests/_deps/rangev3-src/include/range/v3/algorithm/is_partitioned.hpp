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
#ifndef RANGES_V3_ALGORITHM_IS_PARTITIONED_HPP
#define RANGES_V3_ALGORITHM_IS_PARTITIONED_HPP

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/identity.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-algorithms
    /// @{
    RANGES_FUNC_BEGIN(is_partitioned)

        /// \brief function template \c is_partitioned
        template(typename I, typename S, typename C, typename P = identity)(
            requires input_iterator<I> AND sentinel_for<S, I> AND
            indirect_unary_predicate<C, projected<I, P>>)
        constexpr bool RANGES_FUNC(is_partitioned)(I first, S last, C pred, P proj = P{}) //
        {
            for(; first != last; ++first)
                if(!invoke(pred, invoke(proj, *first)))
                    break;
            for(; first != last; ++first)
                if(invoke(pred, invoke(proj, *first)))
                    return false;
            return true;
        }

        /// \overload
        template(typename Rng, typename C, typename P = identity)(
            requires input_range<Rng> AND
            indirect_unary_predicate<C, projected<iterator_t<Rng>, P>>)
        constexpr bool RANGES_FUNC(is_partitioned)(Rng && rng, C pred, P proj = P{}) //
        {
            return (*this)(begin(rng), end(rng), std::move(pred), std::move(proj));
        }

    RANGES_FUNC_END(is_partitioned)

    namespace cpp20
    {
        using ranges::is_partitioned;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
