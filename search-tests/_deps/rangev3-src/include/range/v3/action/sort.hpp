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

#ifndef RANGES_V3_ACTION_SORT_HPP
#define RANGES_V3_ACTION_SORT_HPP

#include <range/v3/range_fwd.hpp>

#include <range/v3/action/action.hpp>
#include <range/v3/algorithm/sort.hpp>
#include <range/v3/functional/bind_back.hpp>
#include <range/v3/functional/comparisons.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-actions
    /// @{
    namespace actions
    {
        struct sort_fn
        {
            template(typename C, typename P = identity)(
                requires (!range<C>))
            constexpr auto operator()(C pred, P proj = {}) const
            {
                return make_action_closure(
                    bind_back(sort_fn{}, std::move(pred), std::move(proj)));
            }

            template(typename Rng, typename C = less, typename P = identity)(
                requires forward_range<Rng> AND sortable<iterator_t<Rng>, C, P>)
            Rng operator()(Rng && rng, C pred = {}, P proj = {}) const
            {
                ranges::sort(rng, std::move(pred), std::move(proj));
                return static_cast<Rng &&>(rng);
            }
        };

        /// \relates actions::sort_fn
        /// \sa action_closure
        RANGES_INLINE_VARIABLE(action_closure<sort_fn>, sort)
    } // namespace actions
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
