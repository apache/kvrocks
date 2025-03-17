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
#ifndef RANGES_V3_ALGORITHM_TRANSFORM_HPP
#define RANGES_V3_ALGORITHM_TRANSFORM_HPP

#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/result_types.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/iterator/unreachable_sentinel.hpp>
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
    using unary_transform_result = detail::in_out_result<I, O>;

    template<typename I1, typename I2, typename O>
    using binary_transform_result = detail::in1_in2_out_result<I1, I2, O>;

    RANGES_FUNC_BEGIN(transform)

        // Single-range variant
        /// \brief function template \c transform
        template(typename I, typename S, typename O, typename F, typename P = identity)(
            requires input_iterator<I> AND sentinel_for<S, I> AND
            weakly_incrementable<O> AND copy_constructible<F> AND
            indirectly_writable<O, indirect_result_t<F &, projected<I, P>>>)
        constexpr unary_transform_result<I, O> //
        RANGES_FUNC(transform)(I first, S last, O out, F fun, P proj = P{}) //
        {
            for(; first != last; ++first, ++out)
                *out = invoke(fun, invoke(proj, *first));
            return {first, out};
        }

        /// \overload
        template(typename Rng, typename O, typename F, typename P = identity)(
            requires input_range<Rng> AND weakly_incrementable<O> AND
            copy_constructible<F> AND
            indirectly_writable<O, indirect_result_t<F &, projected<iterator_t<Rng>, P>>>)
        constexpr unary_transform_result<borrowed_iterator_t<Rng>, O> //
        RANGES_FUNC(transform)(Rng && rng, O out, F fun, P proj = P{}) //
        {
            return (*this)(
                begin(rng), end(rng), std::move(out), std::move(fun), std::move(proj));
        }

        // Double-range variant, 4-iterator version
        /// \overload
        template(typename I0,
                 typename S0,
                 typename I1,
                 typename S1,
                 typename O,
                 typename F,
                 typename P0 = identity,
                 typename P1 = identity)(
            requires input_iterator<I0> AND sentinel_for<S0, I0> AND
                input_iterator<I1> AND sentinel_for<S1, I1> AND
                weakly_incrementable<O> AND copy_constructible<F> AND
                indirectly_writable<
                    O,
                    indirect_result_t<F &, projected<I0, P0>, projected<I1, P1>>>)
        constexpr binary_transform_result<I0, I1, O> //
        RANGES_FUNC(transform)(I0 begin0,
                               S0 end0,
                               I1 begin1,
                               S1 end1,
                               O out,
                               F fun,
                               P0 proj0 = P0{},
                               P1 proj1 = P1{}) //
        {
            for(; begin0 != end0 && begin1 != end1; ++begin0, ++begin1, ++out)
                *out = invoke(fun, invoke(proj0, *begin0), invoke(proj1, *begin1));
            return {begin0, begin1, out};
        }

        /// \overload
        template(typename Rng0,
                 typename Rng1,
                 typename O,
                 typename F,
                 typename P0 = identity,
                 typename P1 = identity)(
            requires input_range<Rng0> AND input_range<Rng1> AND
                weakly_incrementable<O> AND copy_constructible<F> AND
                indirectly_writable<
                    O,
                    indirect_result_t<F &,
                                      projected<iterator_t<Rng0>, P0>,
                                      projected<iterator_t<Rng1>, P1>>>)
        constexpr binary_transform_result<borrowed_iterator_t<Rng0>,
                                          borrowed_iterator_t<Rng1>,
                                          O> //
        RANGES_FUNC(transform)(
            Rng0 && rng0, Rng1 && rng1, O out, F fun, P0 proj0 = P0{}, P1 proj1 = P1{}) //
        {
            return (*this)(begin(rng0),
                           end(rng0),
                           begin(rng1),
                           end(rng1),
                           std::move(out),
                           std::move(fun),
                           std::move(proj0),
                           std::move(proj1));
        }

        // Double-range variant, 3-iterator version
        /// \overload
        template(typename I0,
                     typename S0,
                     typename I1,
                     typename O,
                     typename F,
                     typename P0 = identity,
                     typename P1 = identity)(
            requires input_iterator<I0> AND sentinel_for<S0, I0> AND
                input_iterator<I1> AND weakly_incrementable<O> AND
                copy_constructible<F> AND
                indirectly_writable<
                    O,
                    indirect_result_t<F &, projected<I0, P0>, projected<I1, P1>>>)
        RANGES_DEPRECATED(
            "Use the variant of ranges::transform that takes an upper bound "
            "for both input ranges")
        binary_transform_result<I0, I1, O> //
        RANGES_FUNC(transform)(I0 begin0,
                               S0 end0,
                               I1 begin1,
                               O out,
                               F fun,
                               P0 proj0 = P0{},
                               P1 proj1 = P1{})
        {
            return (*this)(std::move(begin0),
                           std::move(end0),
                           std::move(begin1),
                           unreachable,
                           std::move(out),
                           std::move(fun),
                           std::move(proj0),
                           std::move(proj1));
        }

        /// \overload
        template(typename Rng0,
                 typename I1Ref,
                 typename O,
                 typename F,
                 typename P0 = identity,
                 typename P1 = identity)(
            requires input_range<Rng0> AND input_iterator<uncvref_t<I1Ref>> AND
                weakly_incrementable<O> AND copy_constructible<F> AND
                indirectly_writable<
                    O,
                    indirect_result_t<F &,
                                      projected<iterator_t<Rng0>, P0>,
                                      projected<uncvref_t<I1Ref>, P1>>>)
        RANGES_DEPRECATED(
            "Use the variant of ranges::transform that takes an upper bound "
            "for both input ranges")
        binary_transform_result<borrowed_iterator_t<Rng0>, uncvref_t<I1Ref>, O> //
        RANGES_FUNC(transform)(Rng0 && rng0,
                               I1Ref && begin1,
                               O out,
                               F fun,
                               P0 proj0 = P0{},
                               P1 proj1 = P1{}) //
        {
            return (*this)(begin(rng0),
                           end(rng0),
                           static_cast<I1Ref &&>(begin1),
                           unreachable,
                           std::move(out),
                           std::move(fun),
                           std::move(proj0),
                           std::move(proj1));
        }

    RANGES_FUNC_END(transform)

    namespace cpp20
    {
        using ranges::binary_transform_result;
        using ranges::transform;
        using ranges::unary_transform_result;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
