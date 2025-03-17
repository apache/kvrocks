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

#ifndef RANGES_V3_UTILITY_STATIC_CONST_HPP
#define RANGES_V3_UTILITY_STATIC_CONST_HPP

namespace ranges
{
    /// \ingroup group-utility

    template<typename T>
    struct static_const
    {
        static constexpr T value{};
    };

    /// \ingroup group-utility
    /// \sa `static_const`
    template<typename T>
    constexpr T static_const<T>::value;
} // namespace ranges

#endif
