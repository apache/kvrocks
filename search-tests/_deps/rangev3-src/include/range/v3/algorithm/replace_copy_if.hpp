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
#ifndef RANGES_V3_ALGORITHM_REPLACE_COPY_IF_HPP
#define RANGES_V3_ALGORITHM_REPLACE_COPY_IF_HPP

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/result_types.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/concepts.hpp>
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
    template<typename I, typename O>
    using replace_copy_if_result = detail::in_out_result<I, O>;

    RANGES_FUNC_BEGIN(replace_copy_if)

        /// \brief function template \c replace_copy_if
        template(typename I,
                 typename S,
                 typename O,
                 typename C,
                 typename T,
                 typename P = identity)(
            requires input_iterator<I> AND sentinel_for<S, I> AND
                output_iterator<O, T const &> AND
                indirect_unary_predicate<C, projected<I, P>> AND
                indirectly_copyable<I, O>)
        constexpr replace_copy_if_result<I, O> RANGES_FUNC(replace_copy_if)(
            I first, S last, O out, C pred, T const & new_value, P proj = {}) //
        {
            for(; first != last; ++first, ++out)
            {
                auto && x = *first;
                if(invoke(pred, invoke(proj, x)))
                    *out = new_value;
                else
                    *out = (decltype(x) &&)x;
            }
            return {first, out};
        }

        /// \overload
        template(typename Rng, typename O, typename C, typename T, typename P = identity)(
            requires input_range<Rng> AND output_iterator<O, T const &> AND
                indirect_unary_predicate<C, projected<iterator_t<Rng>, P>> AND
                indirectly_copyable<iterator_t<Rng>, O>)
        constexpr replace_copy_if_result<borrowed_iterator_t<Rng>, O> RANGES_FUNC(replace_copy_if)(
            Rng && rng, O out, C pred, T const & new_value, P proj = {}) //
        {
            return (*this)(begin(rng),
                           end(rng),
                           std::move(out),
                           std::move(pred),
                           new_value,
                           std::move(proj));
        }

    RANGES_FUNC_END(replace_copy_if)

    namespace cpp20
    {
        using ranges::replace_copy_if;
        using ranges::replace_copy_if_result;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
