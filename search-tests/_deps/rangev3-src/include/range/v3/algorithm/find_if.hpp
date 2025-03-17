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
#ifndef RANGES_V3_ALGORITHM_FIND_IF_HPP
#define RANGES_V3_ALGORITHM_FIND_IF_HPP

#include <utility>

#include <range/v3/range_fwd.hpp>

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
    RANGES_FUNC_BEGIN(find_if)
        /// \brief template function \c find
        ///
        /// range-based version of the \c find std algorithm
        ///
        /// \pre `Rng` is a model of the `range` concept
        /// \pre `I` is a model of the `input_iterator` concept
        /// \pre `S` is a model of the `sentinel_for<I>` concept
        /// \pre `P` is a model of the `invocable<V>` concept, where `V` is the
        ///      value type of I.
        /// \pre `F` models `predicate<X>`, where `X` is the result type
        ///      of `invocable<P, V>`
        template(typename I, typename S, typename F, typename P = identity)(
            requires input_iterator<I> AND sentinel_for<S, I> AND
            indirect_unary_predicate<F, projected<I, P>>)
        constexpr I RANGES_FUNC(find_if)(I first, S last, F pred, P proj = P{})
        {
            for(; first != last; ++first)
                if(invoke(pred, invoke(proj, *first)))
                    break;
            return first;
        }

        /// \overload
        template(typename Rng, typename F, typename P = identity)(
            requires input_range<Rng> AND
            indirect_unary_predicate<F, projected<iterator_t<Rng>, P>>)
        constexpr borrowed_iterator_t<Rng> RANGES_FUNC(find_if)(Rng && rng, F pred, P proj = P{})
        {
            return (*this)(begin(rng), end(rng), std::move(pred), std::move(proj));
        }

    RANGES_FUNC_END(find_if)

    namespace cpp20
    {
        using ranges::find_if;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
