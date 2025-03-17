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
//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
#ifndef RANGES_V3_ALGORITHM_ROTATE_HPP
#define RANGES_V3_ALGORITHM_ROTATE_HPP

#include <type_traits>
#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/move.hpp>
#include <range/v3/algorithm/move_backward.hpp>
#include <range/v3/algorithm/swap_ranges.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/move.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/utility/swap.hpp>
#include <range/v3/view/subrange.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-algorithms
    /// @{
    /// \cond
    namespace detail
    {
        template<typename I> // Forward
        constexpr subrange<I> rotate_left(I first, I last)
        {
            iter_value_t<I> tmp = iter_move(first);
            I lm1 = ranges::move(next(first), last, first).out;
            *lm1 = std::move(tmp);
            return {lm1, last};
        }

        template<typename I> // Bidirectional
        constexpr subrange<I> rotate_right(I first, I last)
        {
            I lm1 = prev(last);
            iter_value_t<I> tmp = iter_move(lm1);
            I fp1 = move_backward(first, lm1, last).out;
            *first = std::move(tmp);
            return {fp1, last};
        }

        template<typename I, typename S> // Forward
        constexpr subrange<I> rotate_forward(I first, I middle, S last)
        {
            I i = middle;
            while(true)
            {
                ranges::iter_swap(first, i);
                ++first;
                if(++i == last)
                    break;
                if(first == middle)
                    middle = i;
            }
            I r = first;
            if(first != middle)
            {
                I j = middle;
                while(true)
                {
                    ranges::iter_swap(first, j);
                    ++first;
                    if(++j == last)
                    {
                        if(first == middle)
                            break;
                        j = middle;
                    }
                    else if(first == middle)
                        middle = j;
                }
            }
            return {r, i};
        }

        template<typename D>
        constexpr D gcd(D x, D y)
        {
            do
            {
                D t = x % y;
                x = y;
                y = t;
            } while(y);
            return x;
        }

        template<typename I> // Random
        constexpr subrange<I> rotate_gcd(I first, I middle, I last)
        {
            auto const m1 = middle - first;
            auto const m2 = last - middle;
            if(m1 == m2)
            {
                swap_ranges(first, middle, middle);
                return {middle, last};
            }
            auto const g = detail::gcd(m1, m2);
            for(I p = first + g; p != first;)
            {
                iter_value_t<I> t = iter_move(--p);
                I p1 = p;
                I p2 = p1 + m1;
                do
                {
                    *p1 = iter_move(p2);
                    p1 = p2;
                    auto const d = last - p2;
                    if(m1 < d)
                        p2 += m1;
                    else
                        p2 = first + (m1 - d);
                } while(p2 != p);
                *p1 = std::move(t);
            }
            return {first + m2, last};
        }

        template<typename I, typename S>
        constexpr subrange<I> rotate_(I first, I middle, S last, std::forward_iterator_tag)
        {
            return detail::rotate_forward(first, middle, last);
        }

        template<typename I>
        constexpr subrange<I> rotate_(I first, I middle, I last, std::forward_iterator_tag)
        {
            using value_type = iter_value_t<I>;
            if(detail::is_trivially_move_assignable_v<value_type>)
            {
                if(next(first) == middle)
                    return detail::rotate_left(first, last);
            }
            return detail::rotate_forward(first, middle, last);
        }

        template<typename I>
        constexpr subrange<I> rotate_(I first, I middle, I last, std::bidirectional_iterator_tag)
        {
            using value_type = iter_value_t<I>;
            if(detail::is_trivially_move_assignable_v<value_type>)
            {
                if(next(first) == middle)
                    return detail::rotate_left(first, last);
                if(next(middle) == last)
                    return detail::rotate_right(first, last);
            }
            return detail::rotate_forward(first, middle, last);
        }

        template<typename I>
        constexpr subrange<I> rotate_(I first, I middle, I last, std::random_access_iterator_tag)
        {
            using value_type = iter_value_t<I>;
            if(detail::is_trivially_move_assignable_v<value_type>)
            {
                if(next(first) == middle)
                    return detail::rotate_left(first, last);
                if(next(middle) == last)
                    return detail::rotate_right(first, last);
                return detail::rotate_gcd(first, middle, last);
            }
            return detail::rotate_forward(first, middle, last);
        }
    } // namespace detail
    /// \endcond

    RANGES_FUNC_BEGIN(rotate)

        /// \brief function template \c rotate
        template(typename I, typename S)(
            requires permutable<I> AND sentinel_for<S, I>)
        constexpr subrange<I> RANGES_FUNC(rotate)(I first, I middle, S last) //
        {
            if(first == middle)
            {
                first = ranges::next(std::move(first), last);
                return {first, first};
            }
            if(middle == last)
            {
                return {first, middle};
            }
            return detail::rotate_(first, middle, last, iterator_tag_of<I>{});
        }

        /// \overload
        template(typename Rng, typename I = iterator_t<Rng>)(
            requires range<Rng> AND permutable<I>)
        constexpr borrowed_subrange_t<Rng> RANGES_FUNC(rotate)(Rng && rng, I middle) //
        {
            return (*this)(begin(rng), std::move(middle), end(rng));
        }

    RANGES_FUNC_END(rotate)

    namespace cpp20
    {
        using ranges::rotate;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
