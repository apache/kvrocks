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
#ifndef RANGES_V3_ALGORITHM_REMOVE_HPP
#define RANGES_V3_ALGORITHM_REMOVE_HPP

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/find.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/operations.hpp>
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
    RANGES_FUNC_BEGIN(remove)

        /// \brief function template \c remove
        template(typename I, typename S, typename T, typename P = identity)(
            requires permutable<I> AND sentinel_for<S, I> AND
            indirect_relation<equal_to, projected<I, P>, T const *>)
        constexpr I RANGES_FUNC(remove)(I first, S last, T const & val, P proj = P{})
        {
            first = find(std::move(first), last, val, ranges::ref(proj));
            if(first != last)
            {
                for(I i = next(first); i != last; ++i)
                {
                    if(!(invoke(proj, *i) == val))
                    {
                        *first = iter_move(i);
                        ++first;
                    }
                }
            }
            return first;
        }

        /// \overload
        template(typename Rng, typename T, typename P = identity)(
            requires forward_range<Rng> AND permutable<iterator_t<Rng>> AND
            indirect_relation<equal_to, projected<iterator_t<Rng>, P>, T const *>)
        constexpr borrowed_iterator_t<Rng> //
        RANGES_FUNC(remove)(Rng && rng, T const & val, P proj = P{})
        {
            return (*this)(begin(rng), end(rng), val, std::move(proj));
        }

    RANGES_FUNC_END(remove)

    namespace cpp20
    {
        using ranges::remove;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
