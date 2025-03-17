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

#ifndef RANGES_V3_UTILITY_MOVE_HPP
#define RANGES_V3_UTILITY_MOVE_HPP

#include <type_traits>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    namespace aux
    {
        /// \ingroup group-utility
        struct move_fn : move_tag
        {
            template<typename T>
            constexpr meta::_t<std::remove_reference<T>> && operator()(T && t) const //
                noexcept
            {
                return static_cast<meta::_t<std::remove_reference<T>> &&>(t);
            }

            /// \ingroup group-utility
            /// \sa `move_fn`
            template<typename T>
            friend constexpr decltype(auto) operator|(T && t, move_fn move) noexcept
            {
                return move(t);
            }
        };

        /// \ingroup group-utility
        /// \sa `move_fn`
        RANGES_INLINE_VARIABLE(move_fn, move)

        /// \ingroup group-utility
        /// \sa `move_fn`
        template<typename R>
        using move_t =
            meta::if_c<std::is_reference<R>::value, meta::_t<std::remove_reference<R>> &&,
                       detail::decay_t<R>>;
    } // namespace aux
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
