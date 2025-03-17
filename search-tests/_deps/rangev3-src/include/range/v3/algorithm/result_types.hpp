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

#ifndef RANGES_V3_ALGORITHM_RESULT_TYPES_HPP
#define RANGES_V3_ALGORITHM_RESULT_TYPES_HPP

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
        // Extensions: the dangling story actually works, and result structs
        // are conditionally equality_comparable
#define RANGES_ALGO_RESULT_AUX_2(C, T1, M1, T2, M2)                                   \
    template(typename X, typename Y)(                                                 \
        requires convertible_to<T1 const &, X> AND convertible_to<T2 const &, Y>)     \
    operator C<X, Y>() const &                                                        \
    {                                                                                 \
        return {M1, M2};                                                              \
    }                                                                                 \
    template(typename X, typename Y)(                                                 \
        requires convertible_to<T1, X> AND convertible_to<T2, Y>)                     \
    operator C<X, Y>() &&                                                             \
    {                                                                                 \
        return {static_cast<T1 &&>(M1), static_cast<T2 &&>(M2)};                      \
    }                                                                                 \
    CPP_broken_friend_member                                                          \
    friend constexpr auto operator==(C<T1, T2> const & x, C<T1, T2> const & y)        \
        -> CPP_broken_friend_ret(bool)(                                               \
            requires equality_comparable<T1> && equality_comparable<T2>)              \
    {                                                                                 \
        return x.M1 == y.M1 && x.M2 == y.M2;                                          \
    }                                                                                 \
    CPP_broken_friend_member                                                          \
    friend constexpr auto operator!=(C<T1, T2> const & x, C<T1, T2> const & y)        \
        -> CPP_broken_friend_ret(bool)(                                               \
            requires equality_comparable<T1> && equality_comparable<T2>)              \
    {                                                                                 \
        return !(x == y);                                                             \
    }                                                                                 \
    /**/

#define RANGES_ALGO_RESULT_AUX_3(C, T1, M1, T2, M2, T3, M3)                              \
    template(typename X, typename Y, typename Z)(                                        \
        requires convertible_to<T1 const &, X> AND convertible_to<T2 const &, Y> AND     \
            convertible_to<T3 const &, Z>)                                               \
    operator C<X, Y, Z>() const &                                                        \
    {                                                                                    \
        return {M1, M2, M3};                                                             \
    }                                                                                    \
    template(typename X, typename Y, typename Z)(                                        \
        requires convertible_to<T1, X> AND convertible_to<T2, Y> AND                     \
            convertible_to<T3, Z>)                                                       \
    operator C<X, Y, Z>() &&                                                             \
    {                                                                                    \
        return {static_cast<T1 &&>(M1), static_cast<T2 &&>(M2), static_cast<T3 &&>(M3)}; \
    }                                                                                    \
    CPP_broken_friend_member                                                             \
    friend constexpr auto operator==(C<T1, T2, T3> const & x, C<T1, T2, T3> const & y)   \
        -> CPP_broken_friend_ret(bool)(                                                  \
            requires equality_comparable<T1> && equality_comparable<T2> &&               \
                equality_comparable<T3>)                                                 \
    {                                                                                    \
        return x.M1 == y.M1 && x.M2 == y.M2 && x.M3 == y.M3;                             \
    }                                                                                    \
    CPP_broken_friend_member                                                             \
    friend constexpr auto operator!=(C<T1, T2, T3> const & x, C<T1, T2, T3> const & y)   \
        -> CPP_broken_friend_ret(bool)(                                                  \
            requires equality_comparable<T1> && equality_comparable<T2> &&               \
                equality_comparable<T3>)                                                 \
    {                                                                                    \
        return !(x == y);                                                                \
    }                                                                                    \
    /**/

        template<typename I, typename O>
        struct in_out_result
        {
            I in;
            O out;

            RANGES_ALGO_RESULT_AUX_2(in_out_result, I, in, O, out)
        };

        template<typename I1, typename O>
        struct in1_out_result
        {
            I1 in1;
            O out;

            RANGES_ALGO_RESULT_AUX_2(in1_out_result, I1, in1, O, out)
        };

        template<typename I1, typename I2>
        struct in1_in2_result
        {
            I1 in1;
            I2 in2;

            RANGES_ALGO_RESULT_AUX_2(in1_in2_result, I1, in1, I2, in2)
        };

        template<typename I, typename Fun>
        struct in_fun_result
        {
            I in;
            Fun fun;

            RANGES_ALGO_RESULT_AUX_2(in_fun_result, I, in, Fun, fun)
        };

        template<typename O, typename Fun>
        struct out_fun_result
        {
            O out;
            Fun fun;

            RANGES_ALGO_RESULT_AUX_2(out_fun_result, O, out, Fun, fun)
        };

        template<typename T, typename U>
        struct min_max_result
        {
            T min;
            U max;

            RANGES_ALGO_RESULT_AUX_2(min_max_result, T, min, U, max)
        };

        template<typename I1, typename I2, typename O>
        struct in1_in2_out_result
        {
            I1 in1;
            I2 in2;
            O out;

            RANGES_ALGO_RESULT_AUX_3(in1_in2_out_result, I1, in1, I2, in2, O, out)
        };

        template<typename I, typename O1, typename O2>
        struct in_out1_out2_result
        {
            I in;
            O1 out1;
            O2 out2;

            RANGES_ALGO_RESULT_AUX_3(in_out1_out2_result, I, in, O1, out1, O2, out2)
        };
    } // namespace detail
    /// \endcond
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
