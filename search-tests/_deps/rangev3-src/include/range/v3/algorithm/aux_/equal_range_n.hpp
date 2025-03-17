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
#ifndef RANGES_V3_ALGORITHM_AUX_EQUAL_RANGE_N_HPP
#define RANGES_V3_ALGORITHM_AUX_EQUAL_RANGE_N_HPP

#include <functional>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/aux_/lower_bound_n.hpp>
#include <range/v3/algorithm/aux_/upper_bound_n.hpp>
#include <range/v3/functional/comparisons.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/functional/reference_wrapper.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/subrange.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    namespace aux
    {
        struct equal_range_n_fn
        {
            template(typename I, typename V, typename R = less, typename P = identity)(
                requires forward_iterator<I> AND
                    indirect_strict_weak_order<R, V const *, projected<I, P>>)
            constexpr subrange<I> operator()(I first,
                                             iter_difference_t<I> dist,
                                             V const & val,
                                             R pred = R{},
                                             P proj = P{}) const
            {
                if(0 < dist)
                {
                    do
                    {
                        auto half = dist / 2;
                        auto middle = ranges::next(first, half);
                        auto && v = *middle;
                        auto && pv = invoke(proj, (decltype(v) &&)v);
                        if(invoke(pred, pv, val))
                        {
                            first = std::move(++middle);
                            dist -= half + 1;
                        }
                        else if(invoke(pred, val, pv))
                        {
                            dist = half;
                        }
                        else
                        {
                            return {lower_bound_n(std::move(first),
                                                  half,
                                                  val,
                                                  ranges::ref(pred),
                                                  ranges::ref(proj)),
                                    upper_bound_n(ranges::next(middle),
                                                  dist - (half + 1),
                                                  val,
                                                  ranges::ref(pred),
                                                  ranges::ref(proj))};
                        }
                    } while(0 != dist);
                }
                return {first, first};
            }
        };

        RANGES_INLINE_VARIABLE(equal_range_n_fn, equal_range_n)
    } // namespace aux
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
