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
#ifndef RANGES_V3_ALGORITHM_IS_SORTED_UNTIL_HPP
#define RANGES_V3_ALGORITHM_IS_SORTED_UNTIL_HPP

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/comparisons.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/concepts.hpp>
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
    RANGES_FUNC_BEGIN(is_sorted_until)
        /// \brief template function \c is_sorted_until
        ///
        /// range-based version of the \c is_sorted_until std algorithm
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
        constexpr I RANGES_FUNC(is_sorted_until)(I first, S last, R pred = R{}, P proj = P{})
        {
            auto i = first;
            if(first != last)
            {
                while(++i != last)
                {
                    if(invoke(pred, invoke(proj, *i), invoke(proj, *first)))
                        return i;
                    first = i;
                }
            }
            return i;
        }

        /// \overload
        template(typename Rng, typename R = less, typename P = identity)(
            requires forward_range<Rng> AND
            indirect_strict_weak_order<R, projected<iterator_t<Rng>, P>>)
        constexpr borrowed_iterator_t<Rng> //
        RANGES_FUNC(is_sorted_until)(Rng && rng, R pred = R{}, P proj = P{})
        {
            return (*this)(begin(rng), end(rng), std::move(pred), std::move(proj));
        }

    RANGES_FUNC_END(is_sorted_until)

    namespace cpp20
    {
        using ranges::is_sorted_until;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
