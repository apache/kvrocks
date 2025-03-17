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
#ifndef RANGES_V3_ALGORITHM_PARTITION_COPY_HPP
#define RANGES_V3_ALGORITHM_PARTITION_COPY_HPP

#include <tuple>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/result_types.hpp>
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
    template<typename I, typename O0, typename O1>
    using partition_copy_result = detail::in_out1_out2_result<I, O0, O1>;

    RANGES_FUNC_BEGIN(partition_copy)

        /// \brief function template \c partition_copy
        template(typename I,
                 typename S,
                 typename O0,
                 typename O1,
                 typename C,
                 typename P = identity)(
            requires input_iterator<I> AND sentinel_for<S, I> AND
                weakly_incrementable<O0> AND weakly_incrementable<O1> AND
                indirectly_copyable<I, O0> AND indirectly_copyable<I, O1> AND
                indirect_unary_predicate<C, projected<I, P>>)
        constexpr partition_copy_result<I, O0, O1> RANGES_FUNC(partition_copy)(
            I first, S last, O0 o0, O1 o1, C pred, P proj = P{})
        {
            for(; first != last; ++first)
            {
                auto && x = *first;
                if(invoke(pred, invoke(proj, x)))
                {
                    *o0 = (decltype(x) &&)x;
                    ++o0;
                }
                else
                {
                    *o1 = (decltype(x) &&)x;
                    ++o1;
                }
            }
            return {first, o0, o1};
        }

        /// \overload
        template(typename Rng,
                 typename O0,
                 typename O1,
                 typename C,
                 typename P = identity)(
            requires input_range<Rng> AND weakly_incrementable<O0> AND
                weakly_incrementable<O1> AND indirectly_copyable<iterator_t<Rng>, O0> AND
                indirectly_copyable<iterator_t<Rng>, O1> AND
                indirect_unary_predicate<C, projected<iterator_t<Rng>, P>>)
        constexpr partition_copy_result<borrowed_iterator_t<Rng>, O0, O1> //
        RANGES_FUNC(partition_copy)(Rng && rng, O0 o0, O1 o1, C pred, P proj = P{})
        {
            return (*this)(begin(rng),
                           end(rng),
                           std::move(o0),
                           std::move(o1),
                           std::move(pred),
                           std::move(proj));
        }

    RANGES_FUNC_END(partition_copy)

    namespace cpp20
    {
        using ranges::partition_copy;
        using ranges::partition_copy_result;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
