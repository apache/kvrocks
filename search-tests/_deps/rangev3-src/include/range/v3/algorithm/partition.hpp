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
//===-------------------------- algorithm ---------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
#ifndef RANGES_V3_ALGORITHM_PARTITION_HPP
#define RANGES_V3_ALGORITHM_PARTITION_HPP

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

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
#include <range/v3/utility/swap.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-algorithms
    /// @{

    /// \cond
    namespace detail
    {
        template<typename I, typename S, typename C, typename P>
        constexpr I partition_impl(I first, S last, C pred, P proj, std::forward_iterator_tag)
        {
            while(true)
            {
                if(first == last)
                    return first;
                if(!invoke(pred, invoke(proj, *first)))
                    break;
                ++first;
            }
            for(I p = first; ++p != last;)
            {
                if(invoke(pred, invoke(proj, *p)))
                {
                    ranges::iter_swap(first, p);
                    ++first;
                }
            }
            return first;
        }

        template<typename I, typename S, typename C, typename P>
        constexpr I partition_impl(I first, S end_, C pred, P proj, std::bidirectional_iterator_tag)
        {
            I last = ranges::next(first, end_);
            while(true)
            {
                while(true)
                {
                    if(first == last)
                        return first;
                    if(!invoke(pred, invoke(proj, *first)))
                        break;
                    ++first;
                }
                do
                {
                    if(first == --last)
                        return first;
                } while(!invoke(pred, invoke(proj, *last)));
                ranges::iter_swap(first, last);
                ++first;
            }
        }
    } // namespace detail
    /// \endcond

    RANGES_FUNC_BEGIN(partition)

        /// \brief function template \c partition
        template(typename I, typename S, typename C, typename P = identity)(
            requires permutable<I> AND sentinel_for<S, I> AND
            indirect_unary_predicate<C, projected<I, P>>)
        constexpr I RANGES_FUNC(partition)(I first, S last, C pred, P proj = P{})
        {
            return detail::partition_impl(std::move(first),
                                          std::move(last),
                                          std::move(pred),
                                          std::move(proj),
                                          iterator_tag_of<I>());
        }

        /// \overload
        template(typename Rng, typename C, typename P = identity)(
            requires forward_range<Rng> AND permutable<iterator_t<Rng>> AND
            indirect_unary_predicate<C, projected<iterator_t<Rng>, P>>)
        constexpr borrowed_iterator_t<Rng> RANGES_FUNC(partition)(Rng && rng, C pred, P proj = P{})
        {
            return detail::partition_impl(begin(rng),
                                          end(rng),
                                          std::move(pred),
                                          std::move(proj),
                                          iterator_tag_of<iterator_t<Rng>>());
        }

    RANGES_FUNC_END(partition)

    namespace cpp20
    {
        using ranges::partition;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
