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
#if __cplusplus > 201703L && __has_include(<compare>) && \
    defined(__cpp_concepts) && defined(__cpp_impl_three_way_comparison)

#include <compare>
#include <range/v3/compare.hpp>
#include <range/v3/range_fwd.hpp>

using ranges::same_as;
using ranges::common_comparison_category_t;

static_assert(same_as<common_comparison_category_t<std::partial_ordering>, std::partial_ordering>);
static_assert(same_as<common_comparison_category_t<std::weak_ordering>, std::weak_ordering>);
static_assert(same_as<common_comparison_category_t<std::strong_ordering>, std::strong_ordering>);

static_assert(same_as<common_comparison_category_t<std::partial_ordering, std::strong_ordering>, std::partial_ordering>);
static_assert(same_as<common_comparison_category_t<std::weak_ordering, std::strong_ordering>, std::weak_ordering>);
static_assert(same_as<common_comparison_category_t<std::strong_ordering, std::strong_ordering>, std::strong_ordering>);
static_assert(same_as<common_comparison_category_t<std::weak_ordering, std::strong_ordering, std::partial_ordering>, std::partial_ordering>);

static_assert(same_as<common_comparison_category_t<ranges::less, std::partial_ordering>, void>);
static_assert(same_as<common_comparison_category_t<ranges::less*, std::strong_ordering>, void>);
static_assert(same_as<common_comparison_category_t<ranges::less&, std::strong_ordering, std::partial_ordering>, void>);
static_assert(same_as<common_comparison_category_t<ranges::less(*)(), std::strong_ordering, std::partial_ordering>, void>);
#endif // __cplusplus

int main() {}
