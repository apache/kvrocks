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

#ifndef RANGES_V3_UTILITY_INFINITY_HPP
#define RANGES_V3_UTILITY_INFINITY_HPP

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>
RANGES_DEPRECATED_HEADER(
    "This header is deprecated and will be removed from a future version of range-v3.")

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    struct infinity
    {
        friend constexpr bool operator==(infinity, infinity)
        {
            return true;
        }
        friend constexpr bool operator!=(infinity, infinity)
        {
            return false;
        }
        template<typename Integer>
        friend constexpr auto operator==(Integer, infinity) noexcept
            -> CPP_broken_friend_ret(bool)(
                requires integral<Integer>)
        {
            return false;
        }
        template<typename Integer>
        friend constexpr auto operator==(infinity, Integer) noexcept
            -> CPP_broken_friend_ret(bool)(
                requires integral<Integer>)
        {
            return false;
        }
        template<typename Integer>
        friend constexpr auto operator!=(Integer, infinity) noexcept
            -> CPP_broken_friend_ret(bool)(
                requires integral<Integer>)
        {
            return true;
        }
        template<typename Integer>
        friend constexpr auto operator!=(infinity, Integer) noexcept
            -> CPP_broken_friend_ret(bool)(
                requires integral<Integer>)
        {
            return true;
        }
    };
    /// \endcond
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
