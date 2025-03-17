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
#ifndef RANGES_V3_FUNCTIONAL_IDENTITY_HPP
#define RANGES_V3_FUNCTIONAL_IDENTITY_HPP

#include <range/v3/detail/config.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-functional
    /// @{
    struct identity
    {
        template<typename T>
        constexpr T && operator()(T && t) const noexcept
        {
            return (T &&) t;
        }
        using is_transparent = void;
    };

    /// \cond
    using ident RANGES_DEPRECATED("Replace uses of ranges::ident with ranges::identity") =
        identity;
    /// \endcond

    namespace cpp20
    {
        using ranges::identity;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
