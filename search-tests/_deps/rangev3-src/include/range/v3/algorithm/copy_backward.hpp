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
#ifndef RANGES_V3_ALGORITHM_COPY_BACKWARD_HPP
#define RANGES_V3_ALGORITHM_COPY_BACKWARD_HPP

#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/result_types.hpp>
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
    template<typename I, typename O>
    using copy_backward_result = detail::in_out_result<I, O>;

    RANGES_FUNC_BEGIN(copy_backward)

        /// \brief function template \c copy_backward
        template(typename I, typename S, typename O)(
            requires bidirectional_iterator<I> AND sentinel_for<S, I> AND
            bidirectional_iterator<O> AND indirectly_copyable<I, O>)
        constexpr copy_backward_result<I, O> RANGES_FUNC(copy_backward)(I first, S end_, O out)
        {
            I i = ranges::next(first, end_), last = i;
            while(first != i)
                *--out = *--i;
            return {last, out};
        }

        /// \overload
        template(typename Rng, typename O)(
            requires bidirectional_range<Rng> AND bidirectional_iterator<O> AND
            indirectly_copyable<iterator_t<Rng>, O>)
        copy_backward_result<borrowed_iterator_t<Rng>, O> //
        constexpr RANGES_FUNC(copy_backward)(Rng && rng, O out)
        {
            return (*this)(begin(rng), end(rng), std::move(out));
        }
    RANGES_FUNC_END(copy_backward)

    namespace cpp20
    {
        using ranges::copy_backward;
        using ranges::copy_backward_result;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
