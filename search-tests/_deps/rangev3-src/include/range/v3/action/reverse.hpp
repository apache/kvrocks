/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2013-present
//  Copyright Gonzalo Brito Gadeschi 2017
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_ACTION_REVERSE_HPP
#define RANGES_V3_ACTION_REVERSE_HPP

#include <range/v3/range_fwd.hpp>

#include <range/v3/action/action.hpp>
#include <range/v3/algorithm/reverse.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-actions
    /// @{
    namespace actions
    {
        /// Reversed the source range in-place.
        struct reverse_fn
        {
            template(typename Rng)(
                requires bidirectional_range<Rng> AND permutable<iterator_t<Rng>>)
            Rng operator()(Rng && rng) const
            {
                ranges::reverse(rng);
                return static_cast<Rng &&>(rng);
            }
        };

        /// \relates actions::reverse_fn
        /// \sa action_closure
        RANGES_INLINE_VARIABLE(action_closure<reverse_fn>, reverse)
    } // namespace actions
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
