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
// The implementation of swap (see below) has been adapted from libc++
// (http://libcxx.llvm.org).

#ifndef RANGES_V3_UTILITY_SWAP_HPP
#define RANGES_V3_UTILITY_SWAP_HPP

#include <concepts/swap.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    template<typename T>
    using is_swappable = concepts::is_swappable<T>;

    template<typename T>
    using is_nothrow_swappable = concepts::is_nothrow_swappable<T>;

    template<typename T, typename U>
    using is_swappable_with = concepts::is_swappable_with<T, U>;

    template<typename T, typename U>
    using is_nothrow_swappable_with = concepts::is_nothrow_swappable_with<T, U>;

    using concepts::exchange;

    /// \ingroup group-utility
    /// \relates concepts::adl_swap_detail::swap_fn
    RANGES_DEFINE_CPO(uncvref_t<decltype(concepts::swap)>, swap)

    namespace cpp20
    {
        using ranges::swap;
    }
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
