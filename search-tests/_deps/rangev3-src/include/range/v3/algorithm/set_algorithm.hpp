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
#ifndef RANGES_V3_ALGORITHM_SET_ALGORITHM_HPP
#define RANGES_V3_ALGORITHM_SET_ALGORITHM_HPP

#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/copy.hpp>
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
    RANGES_FUNC_BEGIN(includes)

        /// \brief function template \c includes
        template(typename I1,
                 typename S1,
                 typename I2,
                 typename S2,
                 typename C = less,
                 typename P1 = identity,
                 typename P2 = identity)(
            requires input_iterator<I1> AND sentinel_for<S1, I1> AND
                input_iterator<I2> AND sentinel_for<S2, I2> AND
                indirect_strict_weak_order<C, projected<I1, P1>, projected<I2, P2>>)
        constexpr bool RANGES_FUNC(includes)(I1 begin1,
                                             S1 end1,
                                             I2 begin2,
                                             S2 end2,
                                             C pred = C{},
                                             P1 proj1 = P1{},
                                             P2 proj2 = P2{}) //
        {
            for(; begin2 != end2; ++begin1)
            {
                if(begin1 == end1 ||
                   invoke(pred, invoke(proj2, *begin2), invoke(proj1, *begin1)))
                    return false;
                if(!invoke(pred, invoke(proj1, *begin1), invoke(proj2, *begin2)))
                    ++begin2;
            }
            return true;
        }

        /// \overload
        template(typename Rng1,
                 typename Rng2,
                 typename C = less,
                 typename P1 = identity,
                 typename P2 = identity)(
            requires input_range<Rng1> AND input_range<Rng2> AND
                indirect_strict_weak_order<C,
                                           projected<iterator_t<Rng1>, P1>,
                                           projected<iterator_t<Rng2>, P2>>)
        constexpr bool RANGES_FUNC(includes)(
            Rng1 && rng1, Rng2 && rng2, C pred = C{}, P1 proj1 = P1{}, P2 proj2 = P2{}) //
        {
            return (*this)(begin(rng1),
                           end(rng1),
                           begin(rng2),
                           end(rng2),
                           std::move(pred),
                           std::move(proj1),
                           std::move(proj2));
        }

    RANGES_FUNC_END(includes)

    namespace cpp20
    {
        using ranges::includes;
    }

    template<typename I1, typename I2, typename O>
    using set_union_result = detail::in1_in2_out_result<I1, I2, O>;

    RANGES_FUNC_BEGIN(set_union)

        /// \brief function template \c set_union
        template(typename I1,
                 typename S1,
                 typename I2,
                 typename S2,
                 typename O,
                 typename C = less,
                 typename P1 = identity,
                 typename P2 = identity)(
            requires sentinel_for<S1, I1> AND sentinel_for<S2, I2> AND
                mergeable<I1, I2, O, C, P1, P2>)
        constexpr set_union_result<I1, I2, O> RANGES_FUNC(set_union)(I1 begin1,
                                                                     S1 end1,
                                                                     I2 begin2,
                                                                     S2 end2,
                                                                     O out,
                                                                     C pred = C{},
                                                                     P1 proj1 = P1{},
                                                                     P2 proj2 = P2{}) //
        {
            for(; begin1 != end1; ++out)
            {
                if(begin2 == end2)
                {
                    auto tmp = ranges::copy(begin1, end1, out);
                    return {tmp.in, begin2, tmp.out};
                }
                if(invoke(pred, invoke(proj2, *begin2), invoke(proj1, *begin1)))
                {
                    *out = *begin2;
                    ++begin2;
                }
                else
                {
                    *out = *begin1;
                    if(!invoke(pred, invoke(proj1, *begin1), invoke(proj2, *begin2)))
                        ++begin2;
                    ++begin1;
                }
            }
            auto tmp = ranges::copy(begin2, end2, out);
            return {begin1, tmp.in, tmp.out};
        }

        /// \overload
        template(typename Rng1,
                 typename Rng2,
                 typename O,
                 typename C = less,
                 typename P1 = identity,
                 typename P2 = identity)(
            requires range<Rng1> AND range<Rng2> AND
                mergeable<iterator_t<Rng1>, iterator_t<Rng2>, O, C, P1, P2>)
        constexpr set_union_result<borrowed_iterator_t<Rng1>, borrowed_iterator_t<Rng2>, O> //
        RANGES_FUNC(set_union)(Rng1 && rng1,
                               Rng2 && rng2,
                               O out,
                               C pred = C{},
                               P1 proj1 = P1{},
                               P2 proj2 = P2{}) //
        {
            return (*this)(begin(rng1),
                           end(rng1),
                           begin(rng2),
                           end(rng2),
                           std::move(out),
                           std::move(pred),
                           std::move(proj1),
                           std::move(proj2));
        }

    RANGES_FUNC_END(set_union)

    namespace cpp20
    {
        using ranges::set_union;
        using ranges::set_union_result;
    } // namespace cpp20

    RANGES_FUNC_BEGIN(set_intersection)

        /// \brief function template \c set_intersection
        template(typename I1,
                 typename S1,
                 typename I2,
                 typename S2,
                 typename O,
                 typename C = less,
                 typename P1 = identity,
                 typename P2 = identity)(
            requires sentinel_for<S1, I1> AND sentinel_for<S2, I2> AND
                mergeable<I1, I2, O, C, P1, P2>)
        constexpr O RANGES_FUNC(set_intersection)(I1 begin1,
                                                  S1 end1,
                                                  I2 begin2,
                                                  S2 end2,
                                                  O out,
                                                  C pred = C{},
                                                  P1 proj1 = P1{},
                                                  P2 proj2 = P2{}) //
        {
            while(begin1 != end1 && begin2 != end2)
            {
                if(invoke(pred, invoke(proj1, *begin1), invoke(proj2, *begin2)))
                    ++begin1;
                else
                {
                    if(!invoke(pred, invoke(proj2, *begin2), invoke(proj1, *begin1)))
                    {
                        *out = *begin1;
                        ++out;
                        ++begin1;
                    }
                    ++begin2;
                }
            }
            return out;
        }

        /// \overload
        template(typename Rng1,
                 typename Rng2,
                 typename O,
                 typename C = less,
                 typename P1 = identity,
                 typename P2 = identity)(
            requires range<Rng1> AND range<Rng2> AND
                mergeable<iterator_t<Rng1>, iterator_t<Rng2>, O, C, P1, P2>)
        constexpr O RANGES_FUNC(set_intersection)(Rng1 && rng1,
                                                  Rng2 && rng2,
                                                  O out,
                                                  C pred = C{},
                                                  P1 proj1 = P1{},
                                                  P2 proj2 = P2{}) //
        {
            return (*this)(begin(rng1),
                           end(rng1),
                           begin(rng2),
                           end(rng2),
                           std::move(out),
                           std::move(pred),
                           std::move(proj1),
                           std::move(proj2));
        }

    RANGES_FUNC_END(set_intersection)

    namespace cpp20
    {
        using ranges::set_intersection;
    }

    template<typename I, typename O>
    using set_difference_result = detail::in1_out_result<I, O>;

    RANGES_FUNC_BEGIN(set_difference)

        /// \brief function template \c set_difference
        template(typename I1,
                 typename S1,
                 typename I2,
                 typename S2,
                 typename O,
                 typename C = less,
                 typename P1 = identity,
                 typename P2 = identity)(
            requires sentinel_for<S1, I1> AND sentinel_for<S2, I2> AND
                mergeable<I1, I2, O, C, P1, P2>)
        constexpr set_difference_result<I1, O> RANGES_FUNC(set_difference)(I1 begin1,
                                                                           S1 end1,
                                                                           I2 begin2,
                                                                           S2 end2,
                                                                           O out,
                                                                           C pred = C{},
                                                                           P1 proj1 = P1{},
                                                                           P2 proj2 = P2{}) //
        {
            while(begin1 != end1)
            {
                if(begin2 == end2)
                {
                    auto tmp = ranges::copy(begin1, end1, out);
                    return {tmp.in, tmp.out};
                }
                if(invoke(pred, invoke(proj1, *begin1), invoke(proj2, *begin2)))
                {
                    *out = *begin1;
                    ++out;
                    ++begin1;
                }
                else
                {
                    if(!invoke(pred, invoke(proj2, *begin2), invoke(proj1, *begin1)))
                        ++begin1;
                    ++begin2;
                }
            }
            return {begin1, out};
        }

        /// \overload
        template(typename Rng1,
                 typename Rng2,
                 typename O,
                 typename C = less,
                 typename P1 = identity,
                 typename P2 = identity)(
            requires range<Rng1> AND range<Rng2> AND
                mergeable<iterator_t<Rng1>, iterator_t<Rng2>, O, C, P1, P2>)
        constexpr set_difference_result<borrowed_iterator_t<Rng1>, O> //
        RANGES_FUNC(set_difference)(Rng1 && rng1,
                                    Rng2 && rng2,
                                    O out,
                                    C pred = C{},
                                    P1 proj1 = P1{},
                                    P2 proj2 = P2{}) //
        {
            return (*this)(begin(rng1),
                           end(rng1),
                           begin(rng2),
                           end(rng2),
                           std::move(out),
                           std::move(pred),
                           std::move(proj1),
                           std::move(proj2));
        }

    RANGES_FUNC_END(set_difference)

    namespace cpp20
    {
        using ranges::set_difference;
        using ranges::set_difference_result;
    } // namespace cpp20

    template<typename I1, typename I2, typename O>
    using set_symmetric_difference_result = detail::in1_in2_out_result<I1, I2, O>;

    RANGES_FUNC_BEGIN(set_symmetric_difference)

        /// \brief function template \c set_symmetric_difference
        template(typename I1,
                 typename S1,
                 typename I2,
                 typename S2,
                 typename O,
                 typename C = less,
                 typename P1 = identity,
                 typename P2 = identity)(
            requires sentinel_for<S1, I1> AND sentinel_for<S2, I2> AND
                mergeable<I1, I2, O, C, P1, P2>)
        constexpr set_symmetric_difference_result<I1, I2, O> //
        RANGES_FUNC(set_symmetric_difference)(I1 begin1,
                                              S1 end1,
                                              I2 begin2,
                                              S2 end2,
                                              O out,
                                              C pred = C{},
                                              P1 proj1 = P1{},
                                              P2 proj2 = P2{}) //
        {
            while(begin1 != end1)
            {
                if(begin2 == end2)
                {
                    auto tmp = ranges::copy(begin1, end1, out);
                    return {tmp.in, begin2, tmp.out};
                }
                if(invoke(pred, invoke(proj1, *begin1), invoke(proj2, *begin2)))
                {
                    *out = *begin1;
                    ++out;
                    ++begin1;
                }
                else
                {
                    if(invoke(pred, invoke(proj2, *begin2), invoke(proj1, *begin1)))
                    {
                        *out = *begin2;
                        ++out;
                    }
                    else
                        ++begin1;
                    ++begin2;
                }
            }
            auto tmp = ranges::copy(begin2, end2, out);
            return {begin1, tmp.in, tmp.out};
        }

        /// \overload
        template(typename Rng1,
                 typename Rng2,
                 typename O,
                 typename C = less,
                 typename P1 = identity,
                 typename P2 = identity)(
            requires range<Rng1> AND range<Rng2> AND
                mergeable<iterator_t<Rng1>, iterator_t<Rng2>, O, C, P1, P2>)
        constexpr set_symmetric_difference_result<borrowed_iterator_t<Rng1>,
                                                  borrowed_iterator_t<Rng2>,
                                                  O>
        RANGES_FUNC(set_symmetric_difference)(Rng1 && rng1,
                                              Rng2 && rng2,
                                              O out,
                                              C pred = C{},
                                              P1 proj1 = P1{},
                                              P2 proj2 = P2{}) //
        {
            return (*this)(begin(rng1),
                           end(rng1),
                           begin(rng2),
                           end(rng2),
                           std::move(out),
                           std::move(pred),
                           std::move(proj1),
                           std::move(proj2));
        }

    RANGES_FUNC_END(set_symmetric_difference)

    namespace cpp20
    {
        using ranges::set_symmetric_difference;
        using ranges::set_symmetric_difference_result;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
