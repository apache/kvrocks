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
#ifndef RANGES_V3_ALGORITHM_REPLACE_HPP
#define RANGES_V3_ALGORITHM_REPLACE_HPP

#include <meta/meta.hpp>

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
    RANGES_FUNC_BEGIN(replace)

        /// \brief function template \c replace
        template(typename I, typename S, typename T1, typename T2, typename P = identity)(
            requires input_iterator<I> AND sentinel_for<S, I> AND
                indirectly_writable<I, T2 const &> AND
                indirect_relation<equal_to, projected<I, P>, T1 const *>)
        constexpr I RANGES_FUNC(replace)(
            I first, S last, T1 const & old_value, T2 const & new_value, P proj = {}) //
        {
            for(; first != last; ++first)
                if(invoke(proj, *first) == old_value)
                    *first = new_value;
            return first;
        }

        /// \overload
        template(typename Rng, typename T1, typename T2, typename P = identity)(
            requires input_range<Rng> AND
                indirectly_writable<iterator_t<Rng>, T2 const &> AND
                indirect_relation<equal_to, projected<iterator_t<Rng>, P>, T1 const *>)
        constexpr borrowed_iterator_t<Rng> RANGES_FUNC(replace)(
            Rng && rng, T1 const & old_value, T2 const & new_value, P proj = {}) //
        {
            return (*this)(begin(rng), end(rng), old_value, new_value, std::move(proj));
        }

    RANGES_FUNC_END(replace)

    namespace cpp20
    {
        using ranges::replace;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
