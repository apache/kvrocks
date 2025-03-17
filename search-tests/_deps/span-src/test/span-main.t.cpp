// Copyright 2018-2019 Martin Moene
//
// https://github.com/martinmoene/span-lite
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "span-main.t.hpp"

#ifndef  span_HAVE
# define span_HAVE( feature ) ( span_HAVE_##feature )
#endif

#define span_PRESENT( x ) \
    std::cout << #x << ": " << x << "\n"

#define span_ABSENT( x ) \
    std::cout << #x << ": (undefined)\n"

lest::tests & specification()
{
    static lest::tests tests;
    return tests;
}

CASE( "span-lite version" "[.span][.version]" )
{
    span_PRESENT( span_lite_MAJOR );
    span_PRESENT( span_lite_MINOR );
    span_PRESENT( span_lite_PATCH );
    span_PRESENT( span_lite_VERSION );
}

CASE( "span configuration" "[.span][.config]" )
{
#if span_USES_STD_SPAN
    std::cout << "(Note: Configuration has no effect: using std::span)\n";
#endif
    span_PRESENT( span_HAVE_STD_SPAN );
    span_PRESENT( span_USES_STD_SPAN );
    span_PRESENT( span_SPAN_DEFAULT );
    span_PRESENT( span_SPAN_NONSTD );
    span_PRESENT( span_SPAN_STD );
    span_PRESENT( span_CONFIG_SELECT_SPAN );
//  span_PRESENT( span_CONFIG_EXTENT_TYPE );
//  span_PRESENT( span_CONFIG_SIZE_TYPE );
    span_PRESENT( span_CONFIG_NO_EXCEPTIONS );
}

CASE( "Presence of span library features" "[.span][.config]" )
{
#if span_USES_STD_SPAN
    std::cout << "(Note: library feature configuration has no effect: using std::span)\n";
#endif
    span_PRESENT( span_FEATURE_BYTE_SPAN  );
    span_PRESENT( span_FEATURE_MAKE_SPAN );
#ifdef span_FEATURE_MAKE_SPAN_TO_STD
    span_PRESENT( span_FEATURE_MAKE_SPAN_TO_STD );
#else
    span_ABSENT(  span_FEATURE_MAKE_SPAN_TO_STD );
#endif
    span_PRESENT( span_FEATURE_WITH_CONTAINER );
#ifdef span_FEATURE_WITH_CONTAINER_TO_STD
    span_PRESENT( span_FEATURE_WITH_CONTAINER_TO_STD );
#else
    span_ABSENT(  span_FEATURE_WITH_CONTAINER_TO_STD );
#endif
    span_PRESENT( span_FEATURE_CONSTRUCTION_FROM_STDARRAY_ELEMENT_TYPE );
    span_PRESENT( span_FEATURE_MEMBER_CALL_OPERATOR );
    span_PRESENT( span_FEATURE_MEMBER_AT );
    span_PRESENT( span_FEATURE_MEMBER_BACK_FRONT );
    span_PRESENT( span_FEATURE_MEMBER_SWAP );
    span_PRESENT( span_FEATURE_NON_MEMBER_FIRST_LAST_SUB );
    span_PRESENT( span_FEATURE_COMPARISON );
    span_PRESENT( span_FEATURE_SAME );
}

CASE( "span configuration, contract level" "[.span][.config][.contract]" )
{
#if span_USES_STD_SPAN
    std::cout << "(Note: contract level configuration has no effect: using std::span)\n";
#endif

#ifdef span_CONFIG_CONTRACT_LEVEL_ON
    span_PRESENT( span_CONFIG_CONTRACT_LEVEL_ON );
#else
    span_ABSENT( span_CONFIG_CONTRACT_LEVEL_ON );
#endif

#ifdef span_CONFIG_CONTRACT_LEVEL_OFF
    span_PRESENT( span_CONFIG_CONTRACT_LEVEL_OFF );
#else
    span_ABSENT( span_CONFIG_CONTRACT_LEVEL_OFF );
#endif

#ifdef span_CONFIG_CONTRACT_LEVEL_EXPECTS_ONLY
    span_PRESENT( span_CONFIG_CONTRACT_LEVEL_EXPECTS_ONLY );
#else
    span_ABSENT( span_CONFIG_CONTRACT_LEVEL_EXPECTS_ONLY );
#endif

#ifdef span_CONFIG_CONTRACT_LEVEL_ENSURES_ONLY
    span_PRESENT( span_CONFIG_CONTRACT_LEVEL_ENSURES_ONLY );
#else
    span_ABSENT( span_CONFIG_CONTRACT_LEVEL_ENSURES_ONLY );
#endif

#ifdef span_CONFIG_CONTRACT_VIOLATION_THROWS
    span_PRESENT( span_CONFIG_CONTRACT_VIOLATION_THROWS );
#else
    span_ABSENT( span_CONFIG_CONTRACT_VIOLATION_THROWS );
#endif

#ifdef span_CONFIG_CONTRACT_VIOLATION_TERMINATES
    span_PRESENT( span_CONFIG_CONTRACT_VIOLATION_TERMINATES );
#else
    span_ABSENT( span_CONFIG_CONTRACT_VIOLATION_TERMINATES );
#endif
}

CASE( "__cplusplus" "[.stdc++]" )
{
    span_PRESENT( __cplusplus );

#ifdef _MSVC_LANG
    span_PRESENT( _MSVC_LANG );
#else
    span_ABSENT(  _MSVC_LANG );
#endif

    span_PRESENT( span_CPLUSPLUS );
    span_PRESENT( span_CPLUSPLUS_V );
}

CASE( "Compiler version" "[.compiler]" )
{
#if span_USES_STD_SPAN
    std::cout << "(Compiler version not available: using std::span)\n";
#else
    span_PRESENT( span_COMPILER_CLANG_VERSION );
    span_PRESENT( span_COMPILER_GNUC_VERSION );
    span_PRESENT( span_COMPILER_MSVC_VERSION );
#endif
}

CASE( "Presence of C++ language features" "[.stdlanguage]" )
{
#if defined(__cpp_exceptions)
    span_PRESENT( __cpp_exceptions );
#else
    span_ABSENT(  __cpp_exceptions );
#endif

#if defined(__EXCEPTIONS)
    span_PRESENT( __EXCEPTIONS );
#else
    span_ABSENT(  __EXCEPTIONS );
#endif

#if defined(_CPPUNWIND)
    span_PRESENT( _CPPUNWIND );
#else
    span_ABSENT(  _CPPUNWIND );
#endif

#if defined(_DEBUG)
    span_PRESENT( _DEBUG );
#else
    span_ABSENT(  _DEBUG );
#endif

#if defined(NDEBUG)
    span_PRESENT( NDEBUG );
#else
    span_ABSENT(  NDEBUG );
#endif

#if span_USES_STD_SPAN
    std::cout << "(Presence of C++ language features not available: using std::span)\n";
#else
    span_PRESENT( span_HAVE_ALIAS_TEMPLATE );
    span_PRESENT( span_HAVE_AUTO );
    span_PRESENT( span_HAVE_CONSTEXPR_11 );
    span_PRESENT( span_HAVE_CONSTEXPR_14 );
    span_PRESENT( span_HAVE_CONSTRAINED_SPAN_CONTAINER_CTOR );
    span_PRESENT( span_HAVE_DEDUCTION_GUIDES );
    span_PRESENT( span_HAVE_DEFAULT_FUNCTION_TEMPLATE_ARG );
    span_PRESENT( span_HAVE_DEPRECATED );
    span_PRESENT( span_HAVE_EXPLICIT_CONVERSION );
    span_PRESENT( span_HAVE_IS_DEFAULT );
    span_PRESENT( span_HAVE_IS_DELETE );
    span_PRESENT( span_HAVE_NODISCARD );
    span_PRESENT( span_HAVE_NOEXCEPT );
    span_PRESENT( span_HAVE_NORETURN );
    span_PRESENT( span_HAVE_NULLPTR );
    span_PRESENT( span_HAVE_STATIC_ASSERT );
#endif
}

CASE( "Presence of C++ library features" "[.stdlibrary]" )
{
#if span_USES_STD_SPAN
    std::cout << "(Presence of C++ library features not available: using std::span)\n";
#else
    span_PRESENT( span_HAS_CPP0X );
    span_PRESENT( span_HAVE_ADDRESSOF );
    span_PRESENT( span_HAVE_ARRAY );
    span_PRESENT( span_HAVE_BYTE );
    span_PRESENT( span_HAVE_CONDITIONAL );
    span_PRESENT( span_HAVE_CONTAINER_DATA_METHOD );
    span_PRESENT( span_HAVE_DATA );
    span_PRESENT( span_HAVE_NONSTD_BYTE );
    span_PRESENT( span_HAVE_REMOVE_CONST );
    span_PRESENT( span_HAVE_SNPRINTF );
    span_PRESENT( span_HAVE_STRUCT_BINDING );
    span_PRESENT( span_HAVE_TYPE_TRAITS );
#endif
}

int main( int argc, char * argv[] )
{
    return lest::run( specification(), argc, argv );
}

#if 0
g++            -I../include -o span-lite.t.exe span-lite.t.cpp && span-lite.t.exe --pass
g++ -std=c++98 -I../include -o span-lite.t.exe span-lite.t.cpp && span-lite.t.exe --pass
g++ -std=c++03 -I../include -o span-lite.t.exe span-lite.t.cpp && span-lite.t.exe --pass
g++ -std=c++0x -I../include -o span-lite.t.exe span-lite.t.cpp && span-lite.t.exe --pass
g++ -std=c++11 -I../include -o span-lite.t.exe span-lite.t.cpp && span-lite.t.exe --pass
g++ -std=c++14 -I../include -o span-lite.t.exe span-lite.t.cpp && span-lite.t.exe --pass
g++ -std=c++17 -I../include -o span-lite.t.exe span-lite.t.cpp && span-lite.t.exe --pass

cl -EHsc -I../include span-lite.t.cpp && span-lite.t.exe --pass
#endif

// end of file
