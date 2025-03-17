/// \file
// Range v3 library
//
//  Copyright Andrey Diduh 2019
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
#ifndef RANGES_V3_ACTION_REMOVE_HPP
#define RANGES_V3_ACTION_REMOVE_HPP

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/action/action.hpp>
#include <range/v3/action/erase.hpp>
#include <range/v3/algorithm/remove.hpp>
#include <range/v3/functional/bind_back.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-actions
    /// @{
    namespace actions
    {
        struct remove_fn
        {
            template(typename V, typename P)(
                requires (!range<V>))
            constexpr auto operator()(V && value, P proj) const
            {
                return make_action_closure(
                    bind_back(remove_fn{}, static_cast<V &&>(value), std::move(proj)));
            }

            template<typename V>
            constexpr auto operator()(V && value) const
            {
                return make_action_closure(
                    bind_back(remove_fn{}, static_cast<V &&>(value), identity{}));
            }

            template(typename Rng, typename V, typename P = identity)(
                requires forward_range<Rng> AND permutable<iterator_t<Rng>> AND
                        erasable_range<Rng, iterator_t<Rng>, sentinel_t<Rng>> AND
                            indirect_relation<equal_to, projected<iterator_t<Rng>, P>,
                                              V const *>)
            Rng operator()(Rng && rng, V const & value, P proj = {}) const
            {
                auto it = ranges::remove(rng, value, std::move(proj));
                ranges::erase(rng, it, ranges::end(rng));
                return static_cast<Rng &&>(rng);
            }
        };

        /// \relates actions::remove_fn
        RANGES_INLINE_VARIABLE(remove_fn, remove)
    } // namespace actions
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
