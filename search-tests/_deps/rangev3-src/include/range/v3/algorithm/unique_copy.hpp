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
#ifndef RANGES_V3_ALGORITHM_UNIQUE_COPY_HPP
#define RANGES_V3_ALGORITHM_UNIQUE_COPY_HPP

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/result_types.hpp>
#include <range/v3/functional/comparisons.hpp>
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
    using unique_copy_result = detail::in_out_result<I, O>;

    /// \cond
    namespace detail
    {
        template<typename I, typename S, typename O, typename C, typename P>
        constexpr unique_copy_result<I, O> unique_copy_impl(I first, 
                                                            S last, 
                                                            O out, 
                                                            C pred, 
                                                            P proj,
                                                            std::input_iterator_tag,
                                                            std::false_type)
        {
            if(first != last)
            {
                // Must save a copy into a local because we will need this value
                // even after we advance the input iterator.
                iter_value_t<I> value =
                    *first; // This is guaranteed by indirectly_copyable
                *out = value;
                ++out;
                while(++first != last)
                {
                    auto && x = *first;
                    if(!invoke(pred, invoke(proj, value), invoke(proj, x)))
                    {
                        value = (decltype(x) &&)x;
                        *out = value;
                        ++out;
                    }
                }
            }
            return {first, out};
        }

        template<typename I, typename S, typename O, typename C, typename P>
        constexpr unique_copy_result<I, O> unique_copy_impl(I first, 
                                                            S last, 
                                                            O out, 
                                                            C pred, 
                                                            P proj,
                                                            std::forward_iterator_tag,
                                                            std::false_type)
        {
            if(first != last)
            {
                I tmp = first;
                *out = *tmp;
                ++out;
                while(++first != last)
                {
                    auto && x = *first;
                    if(!invoke(pred, invoke(proj, *tmp), invoke(proj, x)))
                    {
                        *out = (decltype(x) &&)x;
                        ++out;
                        tmp = first;
                    }
                }
            }
            return {first, out};
        }

        template<typename I, typename S, typename O, typename C, typename P>
        constexpr unique_copy_result<I, O> unique_copy_impl(I first, 
                                                            S last, 
                                                            O out, 
                                                            C pred, 
                                                            P proj,
                                                            std::input_iterator_tag, std::true_type)
        {
            if(first != last)
            {
                *out = *first;
                while(++first != last)
                {
                    auto && x = *first;
                    if(!invoke(pred, invoke(proj, *out), invoke(proj, x)))
                        *++out = (decltype(x) &&)x;
                }
                ++out;
            }
            return {first, out};
        }
    } // namespace detail
    /// \endcond

    RANGES_FUNC_BEGIN(unique_copy)

        /// \brief template function unique_copy
        ///
        /// range-based version of the `unique_copy` std algorithm
        ///
        /// \pre `Rng` is a model of the `input_range` concept
        /// \pre `O` is a model of the `weakly_incrementable` concept
        /// \pre `C` is a model of the `relation` concept
        template(typename I,
                 typename S,
                 typename O,
                 typename C = equal_to,
                 typename P = identity)(
            requires input_iterator<I> AND sentinel_for<S, I> AND
                indirect_relation<C, projected<I, P>> AND weakly_incrementable<O> AND
                indirectly_copyable<I, O> AND
                (forward_iterator<I> || forward_iterator<O> ||
                 indirectly_copyable_storable<I, O>)) //
        constexpr unique_copy_result<I, O> RANGES_FUNC(unique_copy)(
            I first, S last, O out, C pred = C{}, P proj = P{}) //
        {
            return detail::unique_copy_impl(std::move(first),
                                            std::move(last),
                                            std::move(out),
                                            std::move(pred),
                                            std::move(proj),
                                            iterator_tag_of<I>(),
                                            meta::bool_<forward_iterator<O>>{});
        }

        /// \overload
        template(typename Rng, typename O, typename C = equal_to, typename P = identity)(
            requires input_range<Rng> AND
                indirect_relation<C, projected<iterator_t<Rng>, P>> AND
                weakly_incrementable<O> AND indirectly_copyable<iterator_t<Rng>, O> AND
                (forward_iterator<iterator_t<Rng>> || forward_iterator<O> ||
                 indirectly_copyable_storable<iterator_t<Rng>, O>)) //
        constexpr unique_copy_result<borrowed_iterator_t<Rng>, O> //
        RANGES_FUNC(unique_copy)(Rng && rng, O out, C pred = C{}, P proj = P{}) //
        {
            return detail::unique_copy_impl(begin(rng),
                                            end(rng),
                                            std::move(out),
                                            std::move(pred),
                                            std::move(proj),
                                            iterator_tag_of<iterator_t<Rng>>(),
                                            meta::bool_<forward_iterator<O>>{});
        }

    RANGES_FUNC_END(unique_copy)

    namespace cpp20
    {
        using ranges::unique_copy;
        using ranges::unique_copy_result;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
