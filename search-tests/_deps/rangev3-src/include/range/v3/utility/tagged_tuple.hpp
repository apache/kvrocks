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

#ifndef RANGES_V3_UTILITY_TAGGED_TUPLE_HPP
#define RANGES_V3_UTILITY_TAGGED_TUPLE_HPP

#include <tuple>

#include <range/v3/range_fwd.hpp>

#include <range/v3/utility/tagged_pair.hpp>

#include <range/v3/detail/prologue.hpp>

RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_DEPRECATED_DECLARATIONS

namespace ranges
{
    template<typename... Ts>
    using tagged_tuple RANGES_DEPRECATED("ranges::tagged_tuple is deprecated.") =
        tagged<std::tuple<detail::tag_elem<Ts>...>, detail::tag_spec<Ts>...>;

    template<typename... Tags, typename... Ts>
    RANGES_DEPRECATED("ranges::make_tagged_tuple is deprecated.")
    constexpr tagged_tuple<Tags(bind_element_t<Ts>)...> make_tagged_tuple(Ts &&... ts)
    {
        return tagged_tuple<Tags(bind_element_t<Ts>)...>{static_cast<Ts &&>(ts)...};
    }
} // namespace ranges

RANGES_DIAGNOSTIC_POP

#include <range/v3/detail/epilogue.hpp>

#endif
