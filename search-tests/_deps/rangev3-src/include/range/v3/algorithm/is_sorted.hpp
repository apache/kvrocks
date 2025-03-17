/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2013-present
//  Copyright Gonzalo Brito Gadeschi 2014
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
// Implementation based on the code in libc++
//   http://http://libcxx.llvm.org/
#ifndef RANGES_V3_ALGORITHM_IS_SORTED_HPP
#define RANGES_V3_ALGORITHM_IS_SORTED_HPP

#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/is_sorted_until.hpp>
#include <range/v3/functional/comparisons.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-algorithms
    /// @{
    RANGES_FUNC_BEGIN(is_sorted)
        /// \brief template function \c is_sorted
        ///
        /// range-based version of the \c is_sorted std algorithm
        ///
        /// Works on forward_ranges
        ///
        /// \pre `Rng` is a model of the `forward_range` concept
        /// \pre `I` is a model of the `forward_iterator` concept
        /// \pre `S` and `I` model the `sentinel_for<S, I>` concept
        /// \pre `R` and `projected<I, P>` model the `indirect_strict_weak_order<R,
        /// projected<I, P>>` concept
        ///
        template(typename I, typename S, typename R = less, typename P = identity)(
            requires forward_iterator<I> AND sentinel_for<S, I> AND
            indirect_strict_weak_order<R, projected<I, P>>)
        constexpr bool RANGES_FUNC(is_sorted)(I first, S last, R rel = R{}, P proj = P{})
        {
            return is_sorted_until(
                       std::move(first), last, std::move(rel), std::move(proj)) == last;
        }

        /// \overload
        template(typename Rng, typename R = less, typename P = identity)(
            requires forward_range<Rng> AND
            indirect_strict_weak_order<R, projected<iterator_t<Rng>, P>>)
        constexpr bool RANGES_FUNC(is_sorted)(Rng && rng, R rel = R{}, P proj = P{}) //
        {
            return (*this)(begin(rng), end(rng), std::move(rel), std::move(proj));
        }

    RANGES_FUNC_END(is_sorted)

    namespace cpp20
    {
        using ranges::is_sorted;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
