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
#ifndef RANGES_V3_FUNCTIONAL_ARITHMETIC_HPP
#define RANGES_V3_FUNCTIONAL_ARITHMETIC_HPP

#include <concepts/concepts.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-functional
    /// @{
    struct plus
    {
        template<typename T, typename U>
        constexpr auto operator()(T && t, U && u) const -> decltype((T &&) t + (U &&) u)
        {
            return (T &&) t + (U &&) u;
        }
        using is_transparent = void;
    };

    struct minus
    {
        template<typename T, typename U>
        constexpr auto operator()(T && t, U && u) const -> decltype((T &&) t - (U &&) u)
        {
            return (T &&) t - (U &&) u;
        }
        using is_transparent = void;
    };

    struct multiplies
    {
        template<typename T, typename U>
        constexpr auto operator()(T && t, U && u) const -> decltype((T &&) t * (U &&) u)
        {
            return (T &&) t * (U &&) u;
        }
        using is_transparent = void;
    };

    struct bitwise_or
    {
        template<typename T, typename U>
        constexpr auto operator()(T && t, U && u) const -> decltype((T &&) t | (U &&) u)
        {
            return (T &&) t | (U &&) u;
        }
        using is_transparent = void;
    };

    template<typename T>
    struct convert_to
    {
        // clang-format off
        template<typename U>
        constexpr auto CPP_auto_fun(operator())(U &&u)(const)
        (
            return static_cast<T>((U &&) u)
        )
        // clang-format on
    };

    template<typename T>
    struct coerce
    {
        constexpr T & operator()(T & t) const
        {
            return t;
        }
        /// \overload
        constexpr T const & operator()(T const & t) const
        {
            return t;
        }
        /// \overload
        constexpr T operator()(T && t) const
        {
            return (T &&) t;
        }
        T operator()(T const &&) const = delete;
    };

    template<typename T>
    struct coerce<T const> : coerce<T>
    {};

    template<typename T>
    struct coerce<T &> : coerce<T>
    {};

    template<typename T>
    struct coerce<T &&> : coerce<T>
    {};
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
