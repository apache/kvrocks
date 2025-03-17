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

#ifndef RANGES_V3_ACTION_STRIDE_HPP
#define RANGES_V3_ACTION_STRIDE_HPP

#include <range/v3/range_fwd.hpp>

#include <range/v3/action/action.hpp>
#include <range/v3/action/erase.hpp>
#include <range/v3/functional/bind_back.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-actions
    /// @{
    namespace actions
    {
        struct stride_fn
        {
            template(typename D)(
                requires detail::integer_like_<D>)
            constexpr auto operator()(D step) const
            {
                return make_action_closure(bind_back(stride_fn{}, step));
            }

            template(typename Rng, typename D = range_difference_t<Rng>)(
                requires forward_range<Rng> AND
                    erasable_range<Rng &, iterator_t<Rng>, sentinel_t<Rng>> AND
                    permutable<iterator_t<Rng>>)
            Rng operator()(Rng && rng, range_difference_t<Rng> const step) const
            {
                using I = iterator_t<Rng>;
                using S = sentinel_t<Rng>;
                RANGES_EXPECT(0 < step);
                if(1 < step)
                {
                    I first = ranges::begin(rng);
                    S const last = ranges::end(rng);
                    if(first != last)
                    {
                        for(I i = ranges::next(++first, step - 1, last); i != last;
                            advance(i, step, last), ++first)
                        {
                            *first = iter_move(i);
                        }
                    }
                    ranges::actions::erase(rng, first, last);
                }
                return static_cast<Rng &&>(rng);
            }
        };

        /// \relates actions::stride_fn
        RANGES_INLINE_VARIABLE(stride_fn, stride)
    } // namespace actions
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
