// Copyright 2018-2019 Martin Moene
//
// https://github.com/martinmoene/span-lite
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#ifndef TEST_SPAN_LITE_H_INCLUDED
#define TEST_SPAN_LITE_H_INCLUDED

// Optionally provide byte-lite:

#ifdef    span_BYTE_LITE_HEADER
# include span_BYTE_LITE_HEADER
#endif

#include "nonstd/span.hpp"

#if span_USES_STD_SPAN
# include <array>
# include <tuple>

# define span_BETWEEN( v, lo, hi )  ( (lo) <= (v) && (v) < (hi) )
#endif

// Compiler warning suppression for usage of lest:

#ifdef __clang__
# pragma clang diagnostic ignored "-Wstring-conversion"
# pragma clang diagnostic ignored "-Wunused-parameter"
# pragma clang diagnostic ignored "-Wunused-template"
# pragma clang diagnostic ignored "-Wunused-function"
# pragma clang diagnostic ignored "-Wunused-member-function"
#elif defined __GNUC__
# pragma GCC   diagnostic ignored "-Wunused-parameter"
# pragma GCC   diagnostic ignored "-Wunused-function"
#endif

// provide std::byte or nonstd::byte as xstd::byte:

namespace xstd {
#if span_HAVE( BYTE )
using std::byte;
using std::to_integer;
#elif span_HAVE( NONSTD_BYTE )
using nonstd::byte;
using nonstd::to_integer;
#endif
}

#if span_HAVE( BYTE ) || span_HAVE( NONSTD_BYTE )
#include <iosfwd>
namespace lest { std::ostream & operator<<( std::ostream & os, xstd::byte b ); }
#endif

#include "lest_cpp03.hpp"

extern lest::tests & specification();

#define CASE( name ) lest_CASE( specification(), name )

namespace nonstd { namespace span_lite {

// use oparator<< instead of to_string() overload;
// see  http://stackoverflow.com/a/10651752/437272

template< typename T >
inline std::ostream & operator<<( std::ostream & os, span<T> const & v )
{
    using lest::to_string;
    return os << "[span:" << (v.empty() ? "[empty]" : to_string( v[0] ) ) << "]";
}

}}

namespace lest {

using ::nonstd::span_lite::operator<<;

#if span_HAVE( BYTE ) ||span_HAVE( NONSTD_BYTE )
inline std::ostream & operator<<( std::ostream & os, xstd::byte b )
{
    return os << "[byte:" << std::hex << std::showbase << xstd::to_integer<int>(b) << "]";
}
#endif

} // namespace lest

#endif // TEST_SPAN_LITE_H_INCLUDED

// end of file
