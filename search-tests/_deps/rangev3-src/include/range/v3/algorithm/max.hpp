/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//  Copyright Casey Carter 2015
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
#ifndef RANGES_V3_ALGORITHM_MAX_HPP
#define RANGES_V3_ALGORITHM_MAX_HPP

#include <initializer_list>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/comparisons.hpp>
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
    RANGES_FUNC_BEGIN(max)

        /// \brief function template \c max
        template(typename T, typename C = less, typename P = identity)(
            requires indirect_strict_weak_order<C, projected<T const *, P>>)
        constexpr T const & RANGES_FUNC(max)(
            T const & a, T const & b, C pred = C{}, P proj = P{}) //
        {
            return invoke(pred, invoke(proj, b), invoke(proj, a)) ? a : b;
        }

        /// \overload
        template(typename Rng, typename C = less, typename P = identity)(
            requires input_range<Rng> AND
            indirect_strict_weak_order<C, projected<iterator_t<Rng>, P>> AND
            indirectly_copyable_storable<iterator_t<Rng>, range_value_t<Rng> *>)
        constexpr range_value_t<Rng> //
        RANGES_FUNC(max)(Rng && rng, C pred = C{}, P proj = P{}) //
        {
            auto first = ranges::begin(rng);
            auto last = ranges::end(rng);
            RANGES_EXPECT(first != last);
            range_value_t<Rng> result = *first;
            while(++first != last)
            {
                auto && tmp = *first;
                if(invoke(pred, invoke(proj, result), invoke(proj, tmp)))
                    result = (decltype(tmp) &&)tmp;
            }
            return result;
        }

        /// \overload
        template(typename T, typename C = less, typename P = identity)(
            requires copyable<T> AND
                indirect_strict_weak_order<C, projected<T const *, P>>)
        constexpr T RANGES_FUNC(max)(
            std::initializer_list<T> const && rng, C pred = C{}, P proj = P{}) //
        {
            return (*this)(rng, std::move(pred), std::move(proj));
        }

    RANGES_FUNC_END(max)

    namespace cpp20
    {
        using ranges::max;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
