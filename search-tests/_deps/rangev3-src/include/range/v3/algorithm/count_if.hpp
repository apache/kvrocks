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
#ifndef RANGES_V3_ALGORITHM_COUNT_IF_HPP
#define RANGES_V3_ALGORITHM_COUNT_IF_HPP

#include <utility>

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
    RANGES_FUNC_BEGIN(count_if)

        /// \brief function template \c count_if
        template(typename I, typename S, typename R, typename P = identity)(
            requires input_iterator<I> AND sentinel_for<S, I> AND
            indirect_unary_predicate<R, projected<I, P>>)
        constexpr iter_difference_t<I> RANGES_FUNC(count_if)(I first, S last, R pred, P proj = P{})
        {
            iter_difference_t<I> n = 0;
            for(; first != last; ++first)
                if(invoke(pred, invoke(proj, *first)))
                    ++n;
            return n;
        }

        /// \overload
        template(typename Rng, typename R, typename P = identity)(
            requires input_range<Rng> AND
            indirect_unary_predicate<R, projected<iterator_t<Rng>, P>>)
        constexpr iter_difference_t<iterator_t<Rng>> //
        RANGES_FUNC(count_if)(Rng && rng, R pred, P proj = P{})
        {
            return (*this)(begin(rng), end(rng), std::move(pred), std::move(proj));
        }

    RANGES_FUNC_END(count_if)

    namespace cpp20
    {
        using ranges::count_if;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
