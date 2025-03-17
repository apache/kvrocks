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
#ifndef RANGES_V3_ALGORITHM_REVERSE_HPP
#define RANGES_V3_ALGORITHM_REVERSE_HPP

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/dangling.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/utility/swap.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-algorithms
    /// @{

    /// \cond
    namespace detail
    {
        template<typename I>
        constexpr void reverse_impl(I first, I last, std::bidirectional_iterator_tag)
        {
            while(first != last)
            {
                if(first == --last)
                    break;
                ranges::iter_swap(first, last);
                ++first;
            }
        }

        template<typename I>
        constexpr void reverse_impl(I first, I last, std::random_access_iterator_tag)
        {
            if(first != last)
                for(; first < --last; ++first)
                    ranges::iter_swap(first, last);
        }
    } // namespace detail
    /// \endcond

    RANGES_FUNC_BEGIN(reverse)

        /// \brief function template \c reverse
        template(typename I, typename S)(
            requires bidirectional_iterator<I> AND sentinel_for<S, I> AND permutable<I>)
        constexpr I RANGES_FUNC(reverse)(I first, S end_)
        {
            I last = ranges::next(first, end_);
            detail::reverse_impl(first, last, iterator_tag_of<I>{});
            return last;
        }

        /// \overload
        template(typename Rng, typename I = iterator_t<Rng>)(
            requires bidirectional_range<Rng> AND permutable<I>)
        constexpr borrowed_iterator_t<Rng> RANGES_FUNC(reverse)(Rng && rng) //
        {
            return (*this)(begin(rng), end(rng));
        }

    RANGES_FUNC_END(reverse)

    namespace cpp20
    {
        using ranges::reverse;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
