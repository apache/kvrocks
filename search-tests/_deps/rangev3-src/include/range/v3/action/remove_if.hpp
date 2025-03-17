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

#ifndef RANGES_V3_ACTION_REMOVE_IF_HPP
#define RANGES_V3_ACTION_REMOVE_IF_HPP

#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/action/action.hpp>
#include <range/v3/action/erase.hpp>
#include <range/v3/algorithm/remove_if.hpp>
#include <range/v3/functional/bind_back.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    // TODO Look at all the special cases handled by erase_if in Library Fundamentals 2

    /// \addtogroup group-actions
    /// @{
    namespace actions
    {
        struct remove_if_fn
        {
            template(typename C, typename P = identity)(
                requires (!range<C>))
            constexpr auto operator()(C pred, P proj = P{}) const
            {
                return make_action_closure(
                    bind_back(remove_if_fn{}, std::move(pred), std::move(proj)));
            }

            template(typename Rng, typename C, typename P = identity)(
                requires forward_range<Rng> AND
                    erasable_range<Rng &, iterator_t<Rng>, iterator_t<Rng>> AND
                        permutable<iterator_t<Rng>> AND
                            indirect_unary_predicate<C, projected<iterator_t<Rng>, P>>)
            Rng operator()(Rng && rng, C pred, P proj = P{}) const
            {
                auto it = ranges::remove_if(rng, std::move(pred), std::move(proj));
                ranges::erase(rng, it, ranges::end(rng));
                return static_cast<Rng &&>(rng);
            }
        };

        /// \relates actions::remove_if_fn
        RANGES_INLINE_VARIABLE(remove_if_fn, remove_if)
    } // namespace actions
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
