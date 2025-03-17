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
#ifndef RANGES_V3_NUMERIC_IOTA_HPP
#define RANGES_V3_NUMERIC_IOTA_HPP

#include <range/v3/iterator/concepts.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/dangling.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-numerics
    /// @{
    struct iota_fn
    {
        template(typename O, typename S, typename T)(
            requires output_iterator<O, T const &> AND sentinel_for<S, O> AND
                weakly_incrementable<T>)
        O operator()(O first, S last, T val) const
        {
            for(; first != last; ++first, ++val)
                *first = detail::as_const(val);
            return first;
        }

        template(typename Rng, typename T)(
            requires output_range<Rng, T const &> AND weakly_incrementable<T>)
        borrowed_iterator_t<Rng> operator()(Rng && rng, T val) const //
        {
            return (*this)(begin(rng), end(rng), detail::move(val));
        }
    };

    RANGES_INLINE_VARIABLE(iota_fn, iota)
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
