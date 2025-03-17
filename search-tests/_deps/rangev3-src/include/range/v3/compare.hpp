/// \file
//  CPP, the Concepts PreProcessor library
//
//  Copyright Eric Niebler 2018-present
//  Copyright (c) 2020-present, Google LLC.
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.
//
// Project home: https://github.com/ericniebler/range-v3
//
#ifndef RANGES_V3_COMPARE_HPP
#define RANGES_V3_COMPARE_HPP

#if __cplusplus > 201703L && __has_include(<compare>) && \
    defined(__cpp_concepts) && defined(__cpp_impl_three_way_comparison)

#include <compare>
#include <type_traits>

namespace ranges
{
   template<typename... Ts>
   struct common_comparison_category
   {
       using type = void;
   };

    template<typename... Ts>
    requires ((std::is_same_v<Ts, std::partial_ordering> ||
               std::is_same_v<Ts, std::weak_ordering> ||
               std::is_same_v<Ts, std::strong_ordering>) && ...)
    struct common_comparison_category<Ts...> : std::common_type<Ts...>
    {};

    template<typename... Ts>
    using common_comparison_category_t = typename common_comparison_category<Ts...>::type;
} // namespace ranges

#endif // __cplusplus
#endif // RANGES_V3_COMPARE_HPP
