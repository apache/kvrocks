// Range v3 library
//
//  Copyright Eric Niebler 2013-present
//  Copyright Casey Carter 2016
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
#ifndef RANGES_V3_UTILITY_FUNCTIONAL_HPP
#define RANGES_V3_UTILITY_FUNCTIONAL_HPP

#include <range/v3/detail/config.hpp>
RANGES_DEPRECATED_HEADER(
    "This header has been deprecated. Please find what you are looking for in the "
    "range/v3/functional/ directory.")

#include <range/v3/detail/with_braced_init_args.hpp>
#include <range/v3/functional/arithmetic.hpp>
#include <range/v3/functional/bind.hpp>
#include <range/v3/functional/bind_back.hpp>
#include <range/v3/functional/comparisons.hpp>
#include <range/v3/functional/compose.hpp>
#include <range/v3/functional/concepts.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/functional/indirect.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/functional/not_fn.hpp>
#include <range/v3/functional/on.hpp>
#include <range/v3/functional/overload.hpp>
#include <range/v3/functional/pipeable.hpp>
#include <range/v3/functional/reference_wrapper.hpp>

namespace ranges
{
    using detail::with_braced_init_args;
} // namespace ranges

#endif
