// Range v3 library
//
//  Copyright Eric Niebler 2013,2014.
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_UTILITY_NULLPTR_V_HPP
#define RANGES_V3_UTILITY_NULLPTR_V_HPP

#include <range/v3/detail/config.hpp>
RANGES_DEPRECATED_HEADER(
    "This header is deprecated and will be removed from a future version of range-v3.")

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \ingroup group-utility
    template<typename T>
    constexpr T * _nullptr_v()
    {
        return nullptr;
    }

#if RANGES_CXX_VARIABLE_TEMPLATES
    /// \ingroup group-utility
    template<typename T>
    constexpr T * nullptr_v = nullptr;
#endif
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
