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
#ifndef CPP_COMPARE_HPP
#define CPP_COMPARE_HPP

#if __cplusplus > 201703L && __has_include(<compare>) && \
    defined(__cpp_concepts) && defined(__cpp_impl_three_way_comparison)

#include <compare>
#include <concepts/concepts.hpp>
#include <range/v3/compare.hpp>

// clang-format off

namespace concepts
{
    // Note: concepts in this file can use C++20 concepts, since operator<=> isn't available in
    // compilers that don't support core concepts.
    namespace detail
    {
        template<typename T, typename Cat>
        concept compares_as = same_as<ranges::common_comparison_category_t<T, Cat>, Cat>;
    } // namespace detail

    inline namespace defs
    {
        template<typename T, typename Cat = std::partial_ordering>
        concept three_way_comparable =
            detail::weakly_equality_comparable_with_<T, T> &&
            detail::partially_ordered_with_<T ,T> &&
            requires(detail::as_cref_t<T>& a, detail::as_cref_t<T>& b) {
                { a <=> b } -> detail::compares_as<Cat>;
            };

        template<typename T, typename U, typename Cat = std::partial_ordering>
        concept three_way_comparable_with =
            three_way_comparable<T, Cat> &&
            three_way_comparable<U, Cat> &&
            common_reference_with<detail::as_cref_t<T>&, detail::as_cref_t<U>&> &&
            three_way_comparable<common_reference_t<detail::as_cref_t<T>&, detail::as_cref_t<U>&>> &&
            detail::partially_ordered_with_<T, U> &&
            requires(detail::as_cref_t<T>& t, detail::as_cref_t<U>& u) {
                { t <=> u } -> detail::compares_as<Cat>;
                { u <=> t } -> detail::compares_as<Cat>;
            };
    } // inline namespace defs
} // namespace concepts

// clang-format on

#endif // __cplusplus
#endif // CPP_COMPARE_HPP
