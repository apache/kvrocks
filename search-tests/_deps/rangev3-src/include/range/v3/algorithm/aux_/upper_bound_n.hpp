/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//  Copyright Casey Carter 2016
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
#ifndef RANGES_V3_ALGORITHM_AUX_UPPER_BOUND_N_HPP
#define RANGES_V3_ALGORITHM_AUX_UPPER_BOUND_N_HPP

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/aux_/partition_point_n.hpp>
#include <range/v3/functional/comparisons.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
        // [&](auto&& i){ return !invoke(pred, val, i); }
        template<typename Pred, typename Val>
        struct upper_bound_predicate
        {
            Pred & pred_;
            Val & val_;

            template<typename T>
            constexpr bool operator()(T && t) const
            {
                return !invoke(pred_, val_, static_cast<T &&>(t));
            }
        };

        template<typename Pred, typename Val>
        constexpr upper_bound_predicate<Pred, Val> make_upper_bound_predicate(Pred & pred,
                                                                              Val & val)
        {
            return {pred, val};
        }
    } // namespace detail
    /// \endcond

    namespace aux
    {
        struct upper_bound_n_fn
        {
            /// \brief template function upper_bound
            ///
            /// range-based version of the `upper_bound` std algorithm
            ///
            /// \pre `Rng` is a model of the `range` concept
            template(typename I, typename V, typename C = less, typename P = identity)(
                requires forward_iterator<I> AND
                    indirect_strict_weak_order<C, V const *, projected<I, P>>)
            constexpr I operator()(I first,
                                   iter_difference_t<I> d,
                                   V const & val,
                                   C pred = C{},
                                   P proj = P{}) const
            {
                return partition_point_n(std::move(first),
                                         d,
                                         detail::make_upper_bound_predicate(pred, val),
                                         std::move(proj));
            }
        };

        RANGES_INLINE_VARIABLE(upper_bound_n_fn, upper_bound_n)
    } // namespace aux
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
