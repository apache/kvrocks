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

#ifndef RANGES_V3_DETAIL_PROLOGUE_HPP
#define RANGES_V3_DETAIL_PROLOGUE_HPP
#include <range/v3/detail/config.hpp>
#endif

#ifdef RANGES_PROLOGUE_INCLUDED
#error "Prologue already included!"
#endif
#define RANGES_PROLOGUE_INCLUDED

RANGES_DIAGNOSTIC_PUSH

#ifdef RANGES_FEWER_WARNINGS
RANGES_DIAGNOSTIC_IGNORE_UNDEFINED_INTERNAL
RANGES_DIAGNOSTIC_IGNORE_INDENTATION
RANGES_DIAGNOSTIC_IGNORE_CXX17_COMPAT
#endif

RANGES_DIAGNOSTIC_KEYWORD_MACRO

#define template(...)                                                           \
    CPP_PP_IGNORE_CXX2A_COMPAT_BEGIN                                            \
    template<__VA_ARGS__ CPP_TEMPLATE_AUX_                                      \

#define AND CPP_and

/// \cond
#if defined(__cpp_noexcept_function_type) && __cpp_noexcept_function_type >= 201510
// noexcept is part of the type system
#define declval(...) static_cast<__VA_ARGS__(*)() noexcept>(nullptr)()
#else
// noexcept is not part of the type system
#define declval(...) static_cast<__VA_ARGS__(*)()>(nullptr)()
#endif
/// \cond
