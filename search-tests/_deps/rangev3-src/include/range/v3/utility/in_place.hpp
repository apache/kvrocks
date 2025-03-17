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

#ifndef RANGES_V3_UTILITY_IN_PLACE_HPP
#define RANGES_V3_UTILITY_IN_PLACE_HPP

#include <range/v3/range_fwd.hpp>

#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \ingroup group-utility
    struct in_place_t
    {};
    RANGES_INLINE_VARIABLE(in_place_t, in_place)
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
