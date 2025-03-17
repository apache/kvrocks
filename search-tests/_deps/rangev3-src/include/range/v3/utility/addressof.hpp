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
#ifndef RANGES_V3_UTILITY_ADDRESSOF_HPP
#define RANGES_V3_UTILITY_ADDRESSOF_HPP

#include <memory>
#include <type_traits>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/detail/config.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
#ifdef __cpp_lib_addressof_constexpr
        using std::addressof;
#else
        namespace check_addressof
        {
            inline ignore_t operator&(ignore_t)
            {
                return {};
            }
            template<typename T>
            auto addressof(T & t)
            {
                return &t;
            }
        } // namespace check_addressof

        template<typename T>
        constexpr bool has_bad_addressof()
        {
            return !std::is_scalar<T>::value &&
                   !RANGES_IS_SAME(decltype(check_addressof::addressof(*(T *)nullptr)),
                                   ignore_t);
        }

        template(typename T)(
            requires(has_bad_addressof<T>()))
        T * addressof(T & arg) noexcept
        {
            return std::addressof(arg);
        }

        template(typename T)(
            requires (!has_bad_addressof<T>()))
        constexpr T * addressof(T & arg) noexcept
        {
            return &arg;
        }

        template<typename T>
        T const * addressof(T const &&) = delete;
#endif
    } // namespace detail
    /// \endcond
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
