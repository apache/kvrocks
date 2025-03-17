//
// span for C++98 and later.
// Based on http://wg21.link/p0122r7
// For more information see https://github.com/martinmoene/span-lite
//
// Copyright 2015, 2018-2020 Martin Moene
// Copyright 2015 Microsoft Corporation. All rights reserved.
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include <ciso646>
#include <memory>   // std::unique_ptr
#include "span-main.t.hpp"

#define DIMENSION_OF( a ) ( sizeof(a) / sizeof(0[a]) )

#define span_STD_OR(     expr )  ( span_USES_STD_SPAN) || ( expr )
#define span_NONSTD_AND( expr )  (!span_USES_STD_SPAN) && ( expr )

using namespace nonstd;

typedef span<int>::size_type size_type;
typedef std::ptrdiff_t       ssize_type;

CASE( "span<>: Terminates construction from a nullptr and a non-zero size (C++11)" )
{
#if span_NONSTD_AND( span_HAVE( NULLPTR ) )
    struct F { static void blow() { span<int> v( nullptr, 42 ); } };

    EXPECT_THROWS( F::blow() );
#else
    EXPECT( !!"nullptr is not available (no C++11), or no exception, using std::span)" );
#endif
}

CASE( "span<>: Terminates construction from two pointers in the wrong order" )
{
#if !span_USES_STD_SPAN
    struct F { static void blow() { int a[2]; span<int> v( &a[1], &a[0] ); } };

    EXPECT_THROWS( F::blow() );
#else
    EXPECT( !!"No exception, using std::span" );
#endif
}

CASE( "span<>: Terminates construction from a null pointer and a non-zero size" )
{
#if !span_USES_STD_SPAN
    struct F { static void blow() { int * p = span_nullptr; span<int> v( p, 42 ); } };

    EXPECT_THROWS( F::blow() );
#else
    EXPECT( !!"No exception, using std::span" );
#endif
}

CASE( "span<>: Terminates creation of a sub span of the first n elements for n exceeding the span" )
{
#if !span_USES_STD_SPAN
    struct F {
    static void blow()
    {
        int arr[] = { 1, 2, 3, };
        span<int> v( arr );

        (void) v.first( 4 );
    }};

    EXPECT_THROWS( F::blow() );
#else
    EXPECT( !!"No exception, using std::span" );
#endif
}

CASE( "span<>: Terminates creation of a sub span of the last n elements for n exceeding the span" )
{
#if !span_USES_STD_SPAN
    struct F {
    static void blow()
    {
        int arr[] = { 1, 2, 3, };
        span<int> v( arr );

        (void) v.last( 4 );
    }};

    EXPECT_THROWS( F::blow() );
#else
    EXPECT( !!"No exception, using std::span" );
#endif
}

CASE( "span<>: Terminates creation of a sub span outside the span" )
{
#if !span_USES_STD_SPAN
    struct F {
        static void blow_offset() { int arr[] = { 1, 2, 3, }; span<int> v( arr ); (void) v.subspan( 4 ); }
        static void blow_count()  { int arr[] = { 1, 2, 3, }; span<int> v( arr ); (void) v.subspan( 1, 3 ); }
    };

    EXPECT_THROWS( F::blow_offset() );
    EXPECT_THROWS( F::blow_count()  );
#else
    EXPECT( !!"No exception, using std::span" );
#endif
}

CASE( "span<>: Terminates access outside the span" )
{
#if !span_USES_STD_SPAN
    struct F {
        static void blow_ix(size_type i) { int arr[] = { 1, 2, 3, }; span<int> v( arr ); (void) v[i]; }
#if span_NONSTD_AND( span_FEATURE( MEMBER_CALL_OPERATOR ) )
        static void blow_iv(size_type i) { int arr[] = { 1, 2, 3, }; span<int> v( arr ); (void) v(i); }
#endif
#if span_NONSTD_AND( span_FEATURE( MEMBER_AT ) )
        static void blow_at(size_type i) { int arr[] = { 1, 2, 3, }; span<int> v( arr ); (void) v.at(i); }
#endif
    };

    EXPECT_NO_THROW( F::blow_ix(2) );
    EXPECT_THROWS(   F::blow_ix(3) );

#if span_NONSTD_AND( span_FEATURE( MEMBER_CALL_OPERATOR ) )
    EXPECT_NO_THROW( F::blow_iv(2) );
    EXPECT_THROWS(   F::blow_iv(3) );
#endif
#if span_NONSTD_AND( span_FEATURE( MEMBER_AT ) )
    EXPECT_NO_THROW( F::blow_at(2) );
    EXPECT_THROWS(   F::blow_at(3) );
#endif
#else
    EXPECT( !!"No exception, using std::span" );
#endif
}

CASE( "span<>: Throws  on access outside the span via at(): std::out_of_range [span_FEATURE_MEMBER_AT>0][span_CONFIG_NO_EXCEPTIONS=0]" )
{
#if span_NONSTD_AND( span_FEATURE( MEMBER_AT ) )
    int arr[] = { 1, 2, 3, };
    span<int>       v( arr );
    span<int> const w( arr );

    EXPECT_THROWS_AS( v.at(42), std::out_of_range );
    EXPECT_THROWS_AS( w.at(42), std::out_of_range );

    struct F {
        static void fail(lest::env & lest_env) {
            int arr[] = { 1, 2, 3, }; span<int> v( arr ); EXPECT( (v.at(42), true) );
    }};

    lest::test fail[] = { lest::test( "F", F::fail ) };

    std::ostringstream os;

    EXPECT( 1 == run( fail, os ) );
#if span_FEATURE( MEMBER_AT ) > 1
    EXPECT( std::string::npos != os.str().find("42") );
    EXPECT( std::string::npos != os.str().find("3)") );
#else
    EXPECT( std::string::npos != os.str().find("index outside span") );
#endif
#else
    EXPECT( !!"member at() is not available (span_FEATURE_MEMBER_AT=0, or using std::span)" );
#endif
}

CASE( "span<>: Termination throws std::logic_error-derived exception [span_CONFIG_CONTRACT_VIOLATION_THROWS=1]" )
{
#if span_NONSTD_AND( span_CONFIG( CONTRACT_VIOLATION_THROWS_V ) )
    struct F {
        static void blow() { int arr[] = { 1, }; span<int> v( arr ); (void) v[1]; }
    };

    EXPECT_THROWS_AS( F::blow(), nonstd::span_lite::detail::contract_violation );
#else
    EXPECT( !!"exception contract_violation is not available (non-throwing contract violation, or using std::span)" );
#endif
}

CASE( "span<>: Allows to default-construct" )
{
    span<int> v;
    span<int, 0> w;

    EXPECT( v.size() == size_type( 0 ) );
    EXPECT( w.size() == size_type( 0 ) );
}

CASE( "span<>: Allows to construct from a nullptr and a zero size (C++11)" )
{
#if span_NONSTD_AND( span_HAVE(  NULLPTR ) )
    span<      int> v( nullptr, size_type(0) );
    span<const int> w( nullptr, size_type(0) );

    EXPECT( v.size() == size_type( 0 ) );
    EXPECT( w.size() == size_type( 0 ) );
#else
    EXPECT( !!"nullptr is not available (no C++11)" );
#endif
}

CASE( "span<>: Allows to construct from two pointers" )
{
    int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span<      int> v( &arr[0], &arr[0] + DIMENSION_OF( arr ) );
    span<const int> w( &arr[0], &arr[0] + DIMENSION_OF( arr ) );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
    EXPECT( std::equal( w.begin(), w.end(), &arr[0] ) );
}

CASE( "span<>: Allows to construct from two iterators" )
{
#if span_HAVE_ITERATOR_CTOR
    std::vector<int> v( 5, 123 );

    span<int> s( v.begin(), v.end() );

    EXPECT( std::equal( s.begin(), s.end(), v.begin() ) );
#else
    EXPECT( !!"construction from iterator is not available (no C++11)" );
#endif
}

CASE( "span<>: Allows to construct from two iterators - empty range" )
{
#if span_HAVE_ITERATOR_CTOR
#if ! (span_COMPILER_MSVC_VER && defined(_DEBUG) )
    std::vector<int> v;

   span<int> s( v.begin(), v.end() );

   EXPECT( std::equal( s.begin(), s.end(), v.begin() ) );
#else
    EXPECT( !!"construction from empty range not available (MSVC Debug)" );
#endif
#else
    EXPECT( !!"construction from iterator is not available (no C++11)" );
#endif
}

CASE( "span<>: Allows to construct from two iterators - move-only element" )
{
#if span_HAVE_ITERATOR_CTOR
    std::vector<std::unique_ptr<int> > v(5);

    nonstd::span<std::unique_ptr<int> > s{ v.begin(), v.end() };

    EXPECT( s.size() == 5 );
#else
    EXPECT( !!"construction from iterator is not available (no C++11)" );
#endif
}

CASE( "span<>: Allows to construct from an iterator and a size" )
{
#if span_HAVE_ITERATOR_CTOR
    std::vector<int> v( 5, 123 );

    span<int> s( v.begin(), v.size() );

    EXPECT( std::equal( s.begin(), s.end(), v.begin() ) );
#else
    EXPECT( !!"construction from iterator is not available (no C++11)" );
#endif
}

CASE( "span<>: Allows to construct from an iterator and a size - empty range" )
{
#if span_HAVE_ITERATOR_CTOR
#if ! (span_COMPILER_MSVC_VER && defined(_DEBUG) )
    std::vector<int> v;

    span<int> s( v.begin(), v.size() );

    EXPECT( std::equal( s.begin(), s.end(), v.begin() ) );
#else
    EXPECT( !!"construction from empty range not available (MSVC Debug)" );
#endif
#else
    EXPECT( !!"construction from iterator is not available (no C++11)" );
#endif
}

CASE( "span<>: Allows to construct from an iterator and a size - move-only element" )
{
#if span_HAVE_ITERATOR_CTOR
    std::vector<std::unique_ptr<int> > v(5);

    nonstd::span<std::unique_ptr<int> > s{ v.begin(), v.size() };

    EXPECT( s.size() == v.size() );
#else
    EXPECT( !!"construction from iterator is not available (no C++11)" );
#endif
}

CASE( "span<>: Allows to construct from two pointers to const" )
{
    const int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span<const int> v( &arr[0], &arr[0] + DIMENSION_OF( arr ) );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
}

CASE( "span<>: Allows to construct from a non-null pointer and a size" )
{
    int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span<      int> v( &arr[0], DIMENSION_OF( arr ) );
    span<const int> w( &arr[0], DIMENSION_OF( arr ) );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
    EXPECT( std::equal( w.begin(), w.end(), &arr[0] ) );
}

CASE( "span<>: Allows to construct from a non-null pointer to const and a size" )
{
    const int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span<const int> v( &arr[0], DIMENSION_OF( arr ) );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
}

CASE( "span<>: Allows to construct from a temporary pointer and a size" )
{
    int x = 42;

    span<      int> v( &x, 1 );
    span<const int> w( &x, 1 );

    EXPECT( std::equal( v.begin(), v.end(), &x ) );
    EXPECT( std::equal( w.begin(), w.end(), &x ) );
}

CASE( "span<>: Allows to construct from a temporary pointer to const and a size" )
{
    const int x = 42;

    span<const int> v( &x, 1 );

    EXPECT( std::equal( v.begin(), v.end(), &x ) );
}

CASE( "span<>: Allows to construct from any pointer and a zero size (C++98)" )
{
#if !span_HAVE_ITERATOR_CTOR
    struct F {
        static void null() {
            int * p = span_nullptr; span<int> v( p, size_type( 0 ) );
        }
        static void nonnull() {
            int i = 7; int * p = &i; span<int> v( p, size_type( 0 ) );
        }
    };

    EXPECT_NO_THROW( F::null() );
    EXPECT_NO_THROW( F::nonnull() );
#else
    EXPECT( !!"Only with C++98" );
#endif
}

CASE( "span<>: Allows to construct from a pointer and a size via a deduction guide (C++17)" )
{
#if span_STD_OR( span_HAVE( DEDUCTION_GUIDES ) )
    char const * argv[] = { "prog", "arg1", "arg2" };
    int  const   argc   = DIMENSION_OF( argv );

    span args( &argv[0], argc );

    EXPECT( args.size() == DIMENSION_OF( argv ) );
#else
    EXPECT( !!"deduction guide is not available (no C++17)" );
#endif
}

CASE( "span<>: Allows to construct from an iterator and a size via a deduction guide (C++17)" )
{
#if span_STD_OR( span_HAVE( DEDUCTION_GUIDES ) )
    std::vector<int> v( 5, 123 );

    span spn( v.begin(), v.size() );

    EXPECT( spn.size() == v.size() );
#else
    EXPECT( !!"deduction guide is not available (no C++17)" );
#endif
}

CASE( "span<>: Allows to construct from two iterators via a deduction guide (C++17)" )
{
#if span_STD_OR( span_HAVE( DEDUCTION_GUIDES ) )
    std::vector<int> v( 5, 123 );

   span spn( v.begin(), v.end() );

   EXPECT( spn.size() == v.size() );
#else
    EXPECT( !!"deduction guide is not available (no C++17)" );
#endif
}

CASE( "span<>: Allows to construct from a C-array" )
{
    int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span<      int> v( arr );
    span<const int> w( arr );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
    EXPECT( std::equal( w.begin(), w.end(), &arr[0] ) );
}

CASE( "span<>: Allows to construct from a C-array via a deduction guide (C++17)" )
{
#if span_STD_OR( span_HAVE( DEDUCTION_GUIDES ) ) && !span_BETWEEN( _MSC_VER, 1, 1920 )
    int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span v( arr );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
#else
    EXPECT( !!"deduction guide is not available (no C++17)" );
#endif
}

CASE( "span<>: Allows to construct from a const C-array" )
{
    const int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span<const int> v( arr );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
}

CASE( "span<>: Allows to construct from a C-array with size via decay to pointer (potentially dangerous)" )
{
    int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    {
    span<      int> v( &arr[0], DIMENSION_OF(arr) );
    span<const int> w( &arr[0], DIMENSION_OF(arr) );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
    EXPECT( std::equal( w.begin(), w.end(), &arr[0] ) );
    }

#if span_CPP14_OR_GREATER
    {
    span<      int> v( &arr[0], 3 );
    span<const int> w( &arr[0], 3 );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0], &arr[0] + 3 ) );
    EXPECT( std::equal( w.begin(), w.end(), &arr[0], &arr[0] + 3 ) );
    }
#endif
}

CASE( "span<>: Allows to construct from a const C-array with size via decay to pointer (potentially dangerous)" )
{
    const int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    {
    span<const int> v( &arr[0], DIMENSION_OF(arr) );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
    }

#if span_CPP14_OR_GREATER
    {
    span<const int> w( &arr[0], 3 );

    EXPECT( std::equal( w.begin(), w.end(), &arr[0], &arr[0] + 3 ) );
    }
#endif
}

CASE( "span<>: Allows to construct from a std::initializer_list<> (C++11)" )
{
#if span_STD_OR( span_HAVE( INITIALIZER_LIST ) )
#if span_STD_OR( span_HAVE( CONSTRAINED_SPAN_CONTAINER_CTOR ) )
    SETUP("")  {
    SECTION("empty initializer list") {
        std::initializer_list<int const> il = {};

        span<int const> v( il );

#if span_BETWEEN( span_COMPILER_MSVC_VERSION, 120, 130 )
        EXPECT( v.size() == 0u );
#else
        EXPECT( std::equal( v.begin(), v.end(), il.begin() ) );
#endif
    }
    SECTION("non-empty initializer list") {
        auto il = { 1, 2, 3, 4, 5, };

        span<int const> v( il );

        EXPECT( std::equal( v.begin(), v.end(), il.begin() ) );
    }}
#else
    EXPECT( !!"constrained construction from container is not available" );
#endif
#else
    EXPECT( !!"std::initializer_list<> is not available (no C++11)" );
#endif
}

CASE( "span<>: Allows to construct from a std::initializer_list<> as a constant set of values (C++11, p2447)" )
{
#if span_HAVE( INITIALIZER_LIST )
#if span_NONSTD_AND( span_FEATURE( WITH_INITIALIZER_LIST_P2447 ) )
    SETUP("")  {
    SECTION("empty initializer list") {
        std::initializer_list<int const> il = {};

        auto f = [&]( span<const int> spn ) {
#if span_BETWEEN( span_COMPILER_MSVC_VERSION, 120, 130 )
            EXPECT( spn.size() == 0u );
#else
            EXPECT( std::equal( spn.begin(), spn.end(), il.begin() ) );
#endif
        };

        f({});
    }
    SECTION("non-empty initializer list") {
        auto il = { 1, 2, 3, 4, 5, };

        auto f = [&]( span<const int> spn ) {
            EXPECT( std::equal( spn.begin(), spn.end(), il.begin() ) );
        };

        f({ 1, 2, 3, 4, 5, });
    }}
#else
    EXPECT( !!"construction from std::initializer_list is not available (span_FEATURE_WITH_INITIALIZER_LIST_P2447, or using std::span)" );
#endif
#else
    EXPECT( !!"std::initializer_list<> is not available, or std::span<> (no C++11)" );
#endif
}

CASE( "span<>: Allows to construct from a std::array<> (C++11)" )
{
#if span_STD_OR( span_HAVE( ARRAY ) )
    std::array<int,9> arr = {{ 1, 2, 3, 4, 5, 6, 7, 8, 9, }};

    span<      int> v( arr );
    span<const int> w( arr );

    EXPECT( std::equal( v.begin(), v.end(), arr.begin() ) );
    EXPECT( std::equal( w.begin(), w.end(), arr.begin() ) );
#else
    EXPECT( !!"std::array<> is not available (no C++11)" );
#endif
}

CASE( "span<>: Allows to construct from a std::array via a deduction guide (C++17)" )
{
#if span_STD_OR( span_HAVE( DEDUCTION_GUIDES ) ) && !span_BETWEEN( _MSC_VER, 1, 1920 )
    std::array<int,9> arr = {{ 1, 2, 3, 4, 5, 6, 7, 8, 9, }};

    span v( arr );

    EXPECT( std::equal( v.begin(), v.end(), arr.begin() ) );
#else
    EXPECT( !!"deduction guide is not available (no C++17)" );
#endif
}

CASE( "span<>: Allows to construct from a std::array<> with const data (C++11, span_FEATURE_CONSTR..._ELEMENT_TYPE=1)" )
{
#if span_FEATURE( CONSTRUCTION_FROM_STDARRAY_ELEMENT_TYPE )
# if span_HAVE( ARRAY )
    std::array<const int,9> arr = {{ 1, 2, 3, 4, 5, 6, 7, 8, 9, }};

    span<const int> v( arr );
    span<const int> const w( arr );

    EXPECT( std::equal( v.begin(), v.end(), arr.begin() ) );
    EXPECT( std::equal( w.begin(), w.end(), arr.begin() ) );
# else
    EXPECT( !!"std::array<> is not available (no C++11)" );
# endif
#else
    EXPECT( !!"construction is not available (span_FEATURE_CONSTRUCTION_FROM_STDARRAY_ELEMENT_TYPE=0)" );
#endif
}

CASE( "span<>: Allows to construct from an empty std::array<> (C++11)" )
{
#if span_STD_OR( span_HAVE( ARRAY ) )
    std::array<int,0> arr;

    span<      int> v( arr );
    span<const int> w( arr );

    EXPECT( std::equal( v.begin(), v.end(), arr.begin() ) );
    EXPECT( std::equal( w.begin(), w.end(), arr.begin() ) );
#else
    EXPECT( !!"std::array<> is not available (no C++11)" );
#endif
}

CASE( "span<>: Allows to construct from a container (std::vector<>)" )
{
#if span_STD_OR( span_HAVE( INITIALIZER_LIST ) )
    std::vector<int> vec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };
#else
    int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };
    std::vector<int> vec( arr, &arr[0] + DIMENSION_OF(arr) );
#endif

#if span_STD_OR( span_HAVE( CONSTRAINED_SPAN_CONTAINER_CTOR ) )
    span<      int> v( vec );
    span<const int> w( vec );

    EXPECT( std::equal( v.begin(), v.end(), vec.begin() ) );
    EXPECT( std::equal( w.begin(), w.end(), vec.begin() ) );
#else
    EXPECT( !!"constrained construction from container is not available" );
#endif
}

CASE( "span<>: Allows to construct from a container via a deduction guide (std::vector<>, C++17)" )
{
#if span_STD_OR( span_HAVE( DEDUCTION_GUIDES ) ) && !span_BETWEEN( _MSC_VER, 1, 1920 )
    std::vector<int> vec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span v( vec );

    EXPECT( std::equal( v.begin(), v.end(), vec.begin() ) );
#else
    EXPECT( !!"deduction guide is not available (no C++17)" );
#endif
}

CASE( "span<>: Allows to tag-construct from a container (std::vector<>)" )
{
#if span_NONSTD_AND( span_FEATURE_TO_STD( WITH_CONTAINER ) )
# if span_HAVE( INITIALIZER_LIST )
    std::vector<int> vec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };
# else
    int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };
    std::vector<int> vec( arr, &arr[0] + DIMENSION_OF(arr) );
# endif
    span<      int> v( with_container, vec );
    span<const int> w( with_container, vec );

    EXPECT( std::equal( v.begin(), v.end(), vec.begin() ) );
    EXPECT( std::equal( w.begin(), w.end(), vec.begin() ) );
#else
    EXPECT( !!"with_container is not available (span_FEATURE_WITH_CONTAINER_TO_STD, or using std::span)" );
#endif
}

CASE( "span<>: Allows to tag-construct from a const container (std::vector<>)" )
{
#if span_NONSTD_AND( span_FEATURE_TO_STD( WITH_CONTAINER ) )
# if span_HAVE( INITIALIZER_LIST )
    const std::vector<int> vec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };
# else
    const int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };
    const std::vector<int> vec( arr, &arr[0] + DIMENSION_OF(arr) );
# endif
    span<      int> v( with_container, vec );
    span<const int> w( with_container, vec );

    EXPECT( std::equal( v.begin(), v.end(), vec.begin() ) );
    EXPECT( std::equal( w.begin(), w.end(), vec.begin() ) );
#else
    EXPECT( !!"with_container is not available (span_FEATURE_WITH_CONTAINER_TO_STD, or using std::span)" );
#endif
}

CASE( "span<>: Allows to copy-construct from another span of the same type" )
{
    int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };
    span<      int> v( arr );
    span<const int> w( arr );

    span<      int> x( v );
    span<const int> y( w );

    EXPECT( std::equal( x.begin(), x.end(), &arr[0] ) );
    EXPECT( std::equal( y.begin(), y.end(), &arr[0] ) );
}

CASE( "span<>: Allows to copy-construct from another span of a compatible type" )
{
    int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };
    span<      int> v( arr );
    span<const int> w( arr );

    span<const volatile int> x( v );
    span<const volatile int> y( v );
#ifndef _LIBCPP_VERSION
    EXPECT( std::equal( x.begin(), x.end(), &arr[0] ) );
    EXPECT( std::equal( y.begin(), y.end(), &arr[0] ) );
#else
    for(size_t i = 0; i < x.size(); ++i)
        EXPECT(x[i] == arr[i]);
    for(size_t i = 0; i < y.size(); ++i)
        EXPECT(y[i] == arr[i]);
#endif
}

CASE( "span<>: Allows to copy-construct from a temporary span of the same type (C++11)" )
{
    int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span<      int> v(( span<      int>( arr ) ));
//  span<      int> w(( span<const int>( arr ) ));
    span<const int> x(( span<      int>( arr ) ));
    span<const int> y(( span<const int>( arr ) ));

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
    EXPECT( std::equal( x.begin(), x.end(), &arr[0] ) );
    EXPECT( std::equal( y.begin(), y.end(), &arr[0] ) );
}

CASE( "span<>: Allows to copy-assign from another span of the same type" )
{
    int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };
    span<      int> v( arr );
    span<      int> s;
    span<const int> t;

    s = v;
    t = v;

    EXPECT( std::equal( s.begin(), s.end(), &arr[0] ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] ) );
}

CASE( "span<>: Allows to copy-assign from a temporary span of the same type (C++11)" )
{
    int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };
    span<      int> v;
    span<const int> w;

    v = span<int>( arr );
    w = span<int>( arr );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
    EXPECT( std::equal( w.begin(), w.end(), &arr[0] ) );
}

CASE( "span<>: Allows to create a sub span of the first n elements" )
{
    int arr[] = { 1, 2, 3, 4, 5, };
    span<int> v( arr );
    size_type count = 3;

    span<      int> s = v.first( count );
    span<const int> t = v.first( count );

    EXPECT( s.size() == count );
    EXPECT( t.size() == count );
    EXPECT( std::equal( s.begin(), s.end(), &arr[0] ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] ) );
}

CASE( "span<>: Allows to create a sub span of the last n elements" )
{
    int arr[] = { 1, 2, 3, 4, 5, };
    span<int> v( arr );
    size_type count = 3;

    span<      int> s = v.last( count );
    span<const int> t = v.last( count );

    EXPECT( s.size() == count );
    EXPECT( t.size() == count );
    EXPECT( std::equal( s.begin(), s.end(), &arr[0] + v.size() - count ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] + v.size() - count ) );
}

CASE( "span<>: Allows to create a sub span starting at a given offset" )
{
    int arr[] = { 1, 2, 3, };
    span<int> v( arr );
    size_type offset = 1;

    span<      int> s = v.subspan( offset );
    span<const int> t = v.subspan( offset );

    EXPECT( s.size() == v.size() - offset );
    EXPECT( t.size() == v.size() - offset );
    EXPECT( std::equal( s.begin(), s.end(), &arr[0] + offset ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] + offset ) );
}

CASE( "span<>: Allows to create a sub span starting at a given offset with a given length" )
{
    int arr[] = { 1, 2, 3, };
    span<int> v( arr );
    size_type offset = 1;
    size_type length = 1;

    span<      int> s = v.subspan( offset, length );
    span<const int> t = v.subspan( offset, length );

    EXPECT( s.size() == length );
    EXPECT( t.size() == length );
    EXPECT( std::equal( s.begin(), s.end(), &arr[0] + offset ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] + offset ) );
}

//CASE( "span<>: Allows to create an empty sub span at full offset" )
//{
//    int arr[] = { 1, 2, 3, };
//    span<int> v( arr );
//    size_type offset = v.size() - 1;
//
//    span<      int> s = v.subspan( offset );
//    span<const int> t = v.subspan( offset );
//
//    EXPECT( s.empty() );
//    EXPECT( t.empty() );
//}

//CASE( "span<>: Allows to create an empty sub span at full offset with zero length" )
//{
//    int arr[] = { 1, 2, 3, };
//    span<int> v( arr );
//    size_type offset = v.size();
//    size_type length = 0;
//
//    span<      int> s = v.subspan( offset, length );
//    span<const int> t = v.subspan( offset, length );
//
//    EXPECT( s.empty() );
//    EXPECT( t.empty() );
//}

CASE( "span<>: Allows to observe an element via array indexing" )
{
    int arr[] = { 1, 2, 3, };
    span<int>       v( arr );
    span<int> const w( arr );

    for ( size_type i = 0; i < v.size(); ++i )
    {
        EXPECT( v[i] == arr[i] );
        EXPECT( w[i] == arr[i] );
    }
}

CASE( "span<>: Allows to observe an element via call indexing" )
{
#if span_NONSTD_AND( span_FEATURE( MEMBER_CALL_OPERATOR ) )
    int arr[] = { 1, 2, 3, };
    span<int>       v( arr );
    span<int> const w( arr );

    for ( size_type i = 0; i < v.size(); ++i )
    {
        EXPECT( v(i) == arr[i] );
        EXPECT( w(i) == arr[i] );
    }
#else
    EXPECT( !!"member () is not available (span_FEATURE_MEMBER_CALL_OPERATOR=0, or using std::span)" );
#endif
}

CASE( "span<>: Allows to observe an element via at() [span_FEATURE_MEMBER_AT>0]" )
{
#if span_NONSTD_AND( span_FEATURE( MEMBER_AT ) )
    int arr[] = { 1, 2, 3, };
    span<int>       v( arr );
    span<int> const w( arr );

    for ( size_type i = 0; i < v.size(); ++i )
    {
        EXPECT( v.at(i) == arr[i] );
        EXPECT( w.at(i) == arr[i] );
    }
#else
    EXPECT( !!"member at() is not available (span_FEATURE_MEMBER_AT=0, or using std::span)" );
#endif
}

CASE( "span<>: Allows to observe an element via data()" )
{
    int arr[] = { 1, 2, 3, };
    span<int>       v( arr );
    span<int> const w( arr );

    EXPECT( *v.data() == *v.begin() );
    EXPECT( *w.data() == *v.begin() );

    for ( size_type i = 0; i < v.size(); ++i )
    {
        EXPECT( v.data()[i] == arr[i] );
        EXPECT( w.data()[i] == arr[i] );
    }
}

CASE( "span<>: Allows to observe the first element via front() [span_FEATURE_MEMBER_BACK_FRONT=1]" )
{
#if span_NONSTD_AND( span_FEATURE( MEMBER_BACK_FRONT ) )
    int arr[] = { 1, 2, 3, };
    span<int> v( arr );

    EXPECT( v.front() == 1 );
#else
    EXPECT( !!"front() is not available (span_FEATURE_MEMBER_BACK_FRONT undefined or 0)" );
#endif
}

CASE( "span<>: Allows to observe the last element via back() [span_FEATURE_MEMBER_BACK_FRONT=1]" )
{
#if span_FEATURE( MEMBER_BACK_FRONT )
    int arr[] = { 1, 2, 3, };
    span<int> v( arr );

    EXPECT( v.back() == 3 );
#else
    EXPECT( !!"back()is not available (span_FEATURE_MEMBER_BACK_FRONT undefined or 0)" );
#endif
}

CASE( "span<>: Allows to change an element via array indexing" )
{
    int arr[] = { 1, 2, 3, };
    span<int>       v( arr );
    span<int> const w( arr );

    v[1] = 22;
    w[2] = 33;

    EXPECT( 22 == arr[1] );
    EXPECT( 33 == arr[2] );
}

CASE( "span<>: Allows to change an element via call indexing" )
{
#if span_NONSTD_AND( span_FEATURE( MEMBER_CALL_OPERATOR ) )
    int arr[] = { 1, 2, 3, };
    span<int>       v( arr );
    span<int> const w( arr );

    v(1) = 22;
    w(2) = 33;

    EXPECT( 22 == arr[1] );
    EXPECT( 33 == arr[2] );
#else
    EXPECT( !!"member () is not available (span_FEATURE_MEMBER_CALL_OPERATOR=0, or using std::span)" );
#endif
}

CASE( "span<>: Allows to change an element via at() [span_FEATURE_MEMBER_AT>0]" )
{
#if span_NONSTD_AND( span_FEATURE( MEMBER_AT ) )
    int arr[] = { 1, 2, 3, };
    span<int>       v( arr );
    span<int> const w( arr );

    v.at(1) = 22;
    w.at(2) = 33;

    EXPECT( 22 == arr[1] );
    EXPECT( 33 == arr[2] );
#else
    EXPECT( !!"member at() is not available (span_FEATURE_MEMBER_AT=0, or using std::span)" );
#endif
}

CASE( "span<>: Allows to change an element via data()" )
{
    int arr[] = { 1, 2, 3, };

    span<int> v( arr );
    span<int> const w( arr );

    *v.data() = 22;
    EXPECT( 22 == *v.data() );

    *w.data() = 33;
    EXPECT( 33 == *w.data() );
}

CASE( "span<>: Allows to change the first element via front() [span_FEATURE_MEMBER_BACK_FRONT=1]" )
{
#if span_FEATURE( MEMBER_BACK_FRONT )
    int arr[] = { 1, 2, 3, };
    span<int> v( arr );

    v.front() = 42;

    EXPECT( v.front() == 42 );
#else
    EXPECT( !!"front() is not available (span_FEATURE_MEMBER_BACK_FRONT undefined or 0)" );
#endif
}

CASE( "span<>: Allows to change the last element via back() [span_FEATURE_MEMBER_BACK_FRONT=1]" )
{
#if span_FEATURE( MEMBER_BACK_FRONT )
    int arr[] = { 1, 2, 3, };
    span<int> v( arr );

    v.back() = 42;

    EXPECT( v.back() == 42 );
#else
    EXPECT( !!"back()is not available (span_FEATURE_MEMBER_BACK_FRONT undefined or 0)" );
#endif
}

CASE( "span<>: Allows to swap with another span [span_FEATURE_MEMBER_SWAP=1]" )
{
#if span_NONSTD_AND( span_FEATURE( MEMBER_SWAP ) )
    int arr[] = { 1, 2, 3, };
    span<int> a( arr );
    span<int> b = a.subspan( 1 );

    a.swap( b );

    EXPECT( a.size() == size_type(2) );
    EXPECT( b.size() == size_type(3) );
    EXPECT( a[0]     == 2 );
    EXPECT( b[0]     == 1 );
#else
    EXPECT( !!"swap()is not available (span_FEATURE_MEMBER_SWAP undefined or 0, or using std::span)" );
#endif
}

CASE( "span<>: Allows forward iteration" )
{
    int arr[] = { 1, 2, 3, };
    span<int> v( arr );

    for ( span<int>::iterator pos = v.begin(); pos != v.end(); ++pos )
    {
        EXPECT( *pos == arr[ std::distance( v.begin(), pos )] );
    }
}

CASE( "span<>: Allows const forward iteration" )
{
    int arr[] = { 1, 2, 3, };
    span<int> v( arr );

#if span_USES_STD_SPAN
    for ( auto pos = std::cbegin(v); pos != std::cend(v); ++pos )
    {
        EXPECT( *pos == arr[ std::distance( std::cbegin(v), pos )] );
    }
#else
    for ( span<int>::const_iterator pos = v.cbegin(); pos != v.cend(); ++pos )
    {
        EXPECT( *pos == arr[ std::distance( v.cbegin(), pos )] );
    }
#endif
}

CASE( "span<>: Allows reverse iteration" )
{
    int arr[] = { 1, 2, 3, };
    span<int> v( arr );

    for ( span<int>::reverse_iterator pos = v.rbegin(); pos != v.rend(); ++pos )
    {
//        size_t dist = narrow<size_t>( std::distance(v.rbegin(), pos) );
        size_type dist = static_cast<size_type>( std::distance(v.rbegin(), pos) );
        EXPECT( *pos == arr[ v.size() - 1 - dist ] );
    }
}

CASE( "span<>: Allows const reverse iteration" )
{
    int arr[] = { 1, 2, 3, };
    const span<int> v( arr );

#if span_USES_STD_SPAN
    for ( auto pos = std::crbegin(v); pos != std::crend(v); ++pos )
    {
//        size_t dist = narrow<size_t>( std::distance(v.crbegin(), pos) );
        size_type dist = static_cast<size_type>( std::distance(std::crbegin(v), pos) );
        EXPECT( *pos == arr[ v.size() - 1 - dist ] );
    }
#else
    for ( span<int>::const_reverse_iterator pos = v.crbegin(); pos != v.crend(); ++pos )
    {
//        size_t dist = narrow<size_t>( std::distance(v.crbegin(), pos) );
        size_type dist = static_cast<size_type>( std::distance(v.crbegin(), pos) );
        EXPECT( *pos == arr[ v.size() - 1 - dist ] );
    }
#endif
}

CASE( "span<>: Allows to identify if a span is the same as another span [span_FEATURE_SAME=1]" )
{
#if span_NONSTD_AND( span_FEATURE( COMPARISON ) )
#if span_FEATURE( SAME )
    int  a[] = { 1 }, b[] = { 1 }, c[] = { 1, 2 };
    char x[] = { '\x1' };

    span<int > va( a );
    span<int > vb( b );
    span<int > vc( c );
    span<char> vx( x );
    span<unsigned char> vu( reinterpret_cast<unsigned char*>( &x[0] ), 1 );

    EXPECT(     same( va, va ) );
    EXPECT_NOT( same( vb, va ) );
    EXPECT_NOT( same( vc, va ) );
    EXPECT_NOT( same( vx, va ) );
    EXPECT_NOT( same( vx, vu ) );

    EXPECT(         va == va );
    EXPECT(         vb == va );
    EXPECT_NOT(     vc == va );
    EXPECT(         vx == va );
    EXPECT(         vx == vu );
#else
    EXPECT( !!"same() is not available (span_FEATURE_SAME=0)" );
#endif
#else
    EXPECT( !!"comparison is not available (span_FEATURE_COMPARISON=0, or using std::span)" );
#endif
}

CASE( "span<>: Allows to compare equal to another span of the same type [span_FEATURE_COMPARISON=1]" )
{
#if span_NONSTD_AND( span_FEATURE( COMPARISON ) )
    int a[] = { 1 }, b[] = { 1 }, c[] = { 2 }, d[] = { 1, 2 };
    span<int> va( a );
    span<int> vb( b );
    span<int> vc( c );
    span<int> vd( d );

    EXPECT(     va == va );
    EXPECT(     vb == va );
    EXPECT_NOT( vc == va );
    EXPECT_NOT( vd == va );
#else
    EXPECT( !!"comparison is not available (span_FEATURE_COMPARISON=0)" );
#endif
}

CASE( "span<>: Allows to compare unequal to another span of the same type [span_FEATURE_COMPARISON=1]" )
{
#if span_NONSTD_AND( span_FEATURE( COMPARISON ))
    int a[] = { 1 }, b[] = { 1 }, c[] = { 2 }, d[] = { 1, 2 };
    span<int> va( a );
    span<int> vb( b );
    span<int> vc( c );
    span<int> vd( d );

    EXPECT_NOT( va != va );
    EXPECT_NOT( vb != va );
    EXPECT(     vc != va );
    EXPECT(     vd != va );
#else
    EXPECT( !!"comparison is not available (span_FEATURE_COMPARISON=0)" );
#endif
}

CASE( "span<>: Allows to compare less than another span of the same type [span_FEATURE_COMPARISON=1]" )
{
#if span_NONSTD_AND( span_FEATURE( COMPARISON ))
    int a[] = { 1 }, b[] = { 2 }, c[] = { 1, 2 };
    span<int> va( a );
    span<int> vb( b );
    span<int> vc( c );

    EXPECT_NOT( va < va );
    EXPECT(     va < vb );
    EXPECT(     va < vc );
#else
    EXPECT( !!"comparison is not available (span_FEATURE_COMPARISON=0)" );
#endif
}

CASE( "span<>: Allows to compare less than or equal to another span of the same type [span_FEATURE_COMPARISON=1]" )
{
#if span_NONSTD_AND( span_FEATURE( COMPARISON ))
    int a[] = { 1 }, b[] = { 2 }, c[] = { 1, 2 };
    span<int> va( a );
    span<int> vb( b );
    span<int> vc( c );

    EXPECT_NOT( vb <= va );
    EXPECT(     va <= vb );
    EXPECT(     va <= vc );
#else
    EXPECT( !!"comparison is not available (span_FEATURE_COMPARISON=0)" );
#endif
}

CASE( "span<>: Allows to compare greater than another span of the same type [span_FEATURE_COMPARISON=1]" )
{
#if span_NONSTD_AND( span_FEATURE( COMPARISON ))
    int a[] = { 1 }, b[] = { 2 }, c[] = { 1, 2 };
    span<int> va( a );
    span<int> vb( b );
    span<int> vc( c );

    EXPECT_NOT( va > va );
    EXPECT(     vb > va );
    EXPECT(     vc > va );
#else
    EXPECT( !!"comparison is not available (span_FEATURE_COMPARISON=0)" );
#endif
}

CASE( "span<>: Allows to compare greater than or equal to another span of the same type [span_FEATURE_COMPARISON=1]" )
{
#if span_NONSTD_AND( span_FEATURE( COMPARISON ))
    int a[] = { 1 }, b[] = { 2 }, c[] = { 1, 2 };
    span<int> va( a );
    span<int> vb( b );
    span<int> vc( c );

    EXPECT_NOT( va >= vb );
    EXPECT(     vb >= va );
    EXPECT(     vc >= va );
#else
    EXPECT( !!"comparison is not available (span_FEATURE_COMPARISON=0)" );
#endif
}

CASE( "span<>: Allows to compare to another span of the same type and different cv-ness [span_FEATURE_SAME=0]" )
{
#if span_NONSTD_AND( span_FEATURE( COMPARISON ))
#if span_FEATURE( SAME )
    EXPECT( !!"skipped as same() is provided via span_FEATURE_SAME=1" );
#else
    int aa[] = { 1 }, bb[] = { 2 };
    span<         int>  a( aa );
    span<   const int> ca( aa );
    span<volatile int> va( aa );
    span<         int>  b( bb );
    span<   const int> cb( bb );

    EXPECT( va == ca );
    EXPECT(  a == va );

    EXPECT(  a == ca );
    EXPECT(  a != cb );
    EXPECT(  a <= cb );
    EXPECT(  a <  cb );
    EXPECT(  b >= ca );
    EXPECT(  b >  ca );
#endif
#else
    EXPECT( !!"comparison is not available (span_FEATURE_COMPARISON=0)" );
#endif
}

CASE( "span<>: Allows to compare empty spans as equal [span_FEATURE_COMPARISON=1]" )
{
#if span_NONSTD_AND( span_FEATURE( COMPARISON ))
    int a;

    span<int> p;
    span<int> q;
    span<int> r( &a, size_type( 0 ) );

    EXPECT( p == q );
    EXPECT( p == r );

#if span_STD_OR( span_HAVE( NULLPTR ) )
    span<int> s( nullptr, size_type( 0 ) );
    span<int> t( nullptr, size_type( 0 ) );

    EXPECT( s == p );
    EXPECT( s == r );
    EXPECT( s == t );
#endif
#else
    EXPECT( !!"comparison is not available (span_FEATURE_COMPARISON=0)" );
#endif
}

CASE( "span<>: Allows to test for empty span via empty(), empty case" )
{
    span<int> v;

    EXPECT( v.empty() );
}

CASE( "span<>: Allows to test for empty span via empty(), non-empty case" )
{
    int a[] = { 1 };
    span<int> v( a );

    EXPECT_NOT( v.empty() );
}

CASE( "span<>: Allows to obtain the number of elements via size()" )
{
    int a[] = { 1, 2, 3, };
    int b[] = { 1, 2, 3, 4, 5, };

    span<int> z;
    span<int> va( a );
    span<int> vb( b );

    EXPECT( va.size() == size_type( DIMENSION_OF( a ) ) );
    EXPECT( vb.size() == size_type( DIMENSION_OF( b ) ) );
    EXPECT(  z.size() == size_type( 0 ) );
}

CASE( "span<>: Allows to obtain the number of elements via ssize()" )
{
#if !span_USES_STD_SPAN
    int a[] = { 1, 2, 3, };
    int b[] = { 1, 2, 3, 4, 5, };

    span<int> z;
    span<int> va( a );
    span<int> vb( b );

    EXPECT( va.ssize() == ssize_type( DIMENSION_OF( a ) ) );
    EXPECT( vb.ssize() == ssize_type( DIMENSION_OF( b ) ) );
    EXPECT(  z.ssize() == 0 );
#else
    EXPECT( !!"member ssize() is not available (using std::span)" );
#endif
}

CASE( "span<>: Allows to obtain the number of bytes via size_bytes()" )
{
    int a[] = { 1, 2, 3, };
    int b[] = { 1, 2, 3, 4, 5, };

    span<int> z;
    span<int> va( a );
    span<int> vb( b );

    EXPECT( va.size_bytes() == size_type( DIMENSION_OF( a ) * sizeof(int) ) );
    EXPECT( vb.size_bytes() == size_type( DIMENSION_OF( b ) * sizeof(int) ) );
    EXPECT(  z.size_bytes() == size_type( 0 * sizeof(int) ) );
}

//CASE( "span<>: Allows to swap with another span of the same type" )
//{
//    int a[] = { 1, 2, 3, };
//    int b[] = { 1, 2, 3, 4, 5, };
//
//    span<int> va0( a );
//    span<int> vb0( b );
//    span<int> va( a );
//    span<int> vb( b );
//
//    va.swap( vb );
//
//    EXPECT( va == vb0 );
//    EXPECT( vb == va0 );
//}

#if span_STD_OR( span_HAVE( BYTE ) ) || span_HAVE( NONSTD_BYTE )

static bool is_little_endian()
{
    union U
    {
        U() : i(1) {}
        int i;
        char c[ sizeof(int) ];
    };
    return 1 != U().c[ sizeof(int) - 1 ];
}
#endif

CASE( "span<>: Allows to view the elements as read-only bytes" )
{
#if span_CPP11_OR_GREATER && ( span_HAVE( BYTE ) || span_HAVE( NONSTD_BYTE ) )
    using byte = xstd::byte;
    using type = std::int32_t;

    EXPECT( sizeof( type ) == size_t( 4 ) );

    type  a[] = { 0x12345678, };
    byte be[] = { byte{0x12}, byte{0x34}, byte{0x56}, byte{0x78}, };
    byte le[] = { byte{0x78}, byte{0x56}, byte{0x34}, byte{0x12}, };

    xstd::byte * b = is_little_endian() ? &le[0] : &be[0];

    span<type> va( a );
    span<const xstd::byte> vb( as_bytes( va ) );

    EXPECT( vb[0] == b[0] );
    EXPECT( vb[1] == b[1] );
    EXPECT( vb[2] == b[2] );
    EXPECT( vb[3] == b[3] );
#else
    EXPECT( !!"(non)std::byte is not available (no C++17, no byte-lite); test requires C++11" );
#endif
}

CASE( "span<>: Allows to view and change the elements as writable bytes" )
{
#if span_CPP11_OR_GREATER && ( span_HAVE( BYTE ) || span_HAVE( NONSTD_BYTE ) )
    using byte = xstd::byte;
    using type = std::int32_t;

    EXPECT( sizeof(type) == size_t( 4 ) );

    type  a[] = { 0x0, };
    span<type> va( a );
    span<byte> vb( as_writable_bytes(va) );

    for ( size_type i = 0; i < size_type( sizeof(type) ); ++i )
    {
        EXPECT( vb[i] == byte{0} );
    }

    vb[0] = byte{0x42};

    EXPECT( vb[0] == byte{0x42} );
    for ( size_type i = 1; i < size_type( sizeof(type) ); ++i )
    {
        EXPECT( vb[i] == byte{0} );
    }
#else
    EXPECT( !!"(non)std::byte is not available (no C++17, no byte-lite); test requires C++11" );
#endif
}

//CASE( "span<>: Allows to view the elements as a span of another type" )
//{
//#if span_STD_OR( span_HAVE( SIZED_TYPES ) )
//    typedef int32_t type1;
//    typedef int16_t type2;
//#else
//    typedef int   type1;
//    typedef short type2;
//#endif
//    EXPECT( sizeof( type1 ) == size_t( 4 ) );
//    EXPECT( sizeof( type2 ) == size_t( 2 ) );
//
//    type1  a[] = { 0x12345678, };
//    type2 be[] = { type2(0x1234), type2(0x5678), };
//    type2 le[] = { type2(0x5678), type2(0x1234), };
//
//    type2 * b = is_little_endian() ? le : be;
//
//    span<type1> va( a );
//    span<type2> vb( va );
//
//    EXPECT( vb[0] == b[0] );
//    EXPECT( vb[1] == b[1] );
//}

//CASE( "span<>: Allows to change the elements from a span of another type" )
//{
//#if span_STD_OR( span_HAVE( SIZED_TYPES ) )
//    typedef int32_t type1;
//    typedef int16_t type2;
//#else
//    typedef int   type1;
//    typedef short type2;
//#endif
//    EXPECT( sizeof( type1 ) == size_t( 4 ) );
//    EXPECT( sizeof( type2 ) == size_t( 2 ) );
//
//    type1  a[] = { 0x0, };
//
//    span<type1> va( a );
//#if span_COMPILER_MSVC_VERSION == 60
//    span<type2> vb( va.as_span( type2() ) );
//#else
//    span<type2> vb( va.as_span<type2>() );
//#endif
//
//    {for ( size_t i = 0; i < sizeof(type2); ++i ) EXPECT( vb[i] == type2(0) ); }
//
//    vb[0] = 0x42;
//
//    EXPECT( vb[0] == type2(0x42) );
//    {for ( size_t i = 1; i < sizeof(type2); ++i ) EXPECT( vb[i] == type2(0) ); }
//}

#if span_FEATURE_TO_STD( MAKE_SPAN )

CASE( "make_span() [span_FEATURE_MAKE_SPAN_TO_STD=99]" )
{
    EXPECT( !!"(avoid warning)" );  // suppress: unused parameter 'lest_env' [-Wunused-parameter]
}

CASE( "make_span(): Allows building from two pointers" )
{
    int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span<int> v = make_span( &arr[0], &arr[0] + DIMENSION_OF( arr ) );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
}

CASE( "make_span(): Allows building from two const pointers" )
{
    const int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span<const int> v = make_span( &arr[0], &arr[0] + DIMENSION_OF( arr ) );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
}

CASE( "make_span(): Allows building from a non-null pointer and a size" )
{
    int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span<int> v = make_span( &arr[0], DIMENSION_OF( arr ) );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
}

CASE( "make_span(): Allows building from a non-null const pointer and a size" )
{
    const int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span<const int> v = make_span( &arr[0], DIMENSION_OF( arr ) );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
}

CASE( "make_span(): Allows building from a C-array" )
{
    int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span<int> v = make_span( arr );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
}

CASE( "make_span(): Allows building from a const C-array" )
{
    const int arr[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span<const int> v = make_span( arr );

    EXPECT( std::equal( v.begin(), v.end(), &arr[0] ) );
}

CASE( "make_span(): Allows building from a std::initializer_list<> (C++11)" )
{
#if span_STD_OR( span_HAVE( INITIALIZER_LIST ) )
#if span_STD_OR( span_HAVE( CONSTRAINED_SPAN_CONTAINER_CTOR ) )
    auto il = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span<int const> v = make_span( il );

    EXPECT( std::equal( v.begin(), v.end(), il.begin() ) );
#else
    EXPECT( !!"constrained construction from container is not available" );
#endif
#else
    EXPECT( !!"std::initializer_list<> is not available (no C++11)" );
#endif
}

CASE( "make_span(): Allows building from a std::initializer_list<> as a constant set of values (C++11)" )
{
#if span_STD_OR( span_HAVE( INITIALIZER_LIST ) )
    SETUP("")  {
    SECTION("empty initializer list") {
        std::initializer_list<int const> il = {};

        auto f = [&]( span<const int> v ){
#if span_BETWEEN( span_COMPILER_MSVC_VERSION, 120, 130 )
            EXPECT( v.size() == 0u );
#else
            EXPECT( std::equal( v.begin(), v.end(), il.begin() ) );
#endif
        };

    //  f( make_span({}) );         // element type unknown
        f( make_span<int>({}) );
    }
    SECTION("non-empty initializer list") {
        auto il = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

        auto f = [&]( span<const int> v ){
            EXPECT( std::equal( v.begin(), v.end(), il.begin() ) );
        };

        f( make_span({ 1, 2, 3, 4, 5, 6, 7, 8, 9, }) );
    }}
#else
    EXPECT( !!"std::initializer_list<> is not available (no C++11)" );
#endif
}

CASE( "make_span(): Allows building from a std::array<> (C++11)" )
{
#if span_STD_OR( span_HAVE( ARRAY ) )
    std::array<int,9> arr = {{ 1, 2, 3, 4, 5, 6, 7, 8, 9, }};

    span<int> v = make_span( arr );

    EXPECT( std::equal( v.begin(), v.end(), arr.begin() ) );
#else
    EXPECT( !!"std::array<> is not available (no C++11)" );
#endif
}

CASE( "make_span(): Allows building from a const std::array<> (C++11)" )
{
#if span_STD_OR( span_HAVE( ARRAY ) )
    const std::array<int,9> arr = {{ 1, 2, 3, 4, 5, 6, 7, 8, 9, }};

    span<const int> v = make_span( arr );

    EXPECT( std::equal( v.begin(), v.end(), arr.begin() ) );
#else
    EXPECT( !!"std::array<> is not available (no C++11)" );
#endif
}

CASE( "make_span(): Allows building from a container (std::vector<>)" )
{
#if span_STD_OR( span_HAVE( INITIALIZER_LIST ) )
    std::vector<int> vec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };
#else
    std::vector<int> vec; {for ( int i = 1; i < 10; ++i ) vec.push_back(i); }
#endif
    span<int> v = make_span( vec );

    EXPECT( std::equal( v.begin(), v.end(), vec.begin() ) );
}

CASE( "make_span(): Allows building from a const container (std::vector<>)" )
{
#if span_STD_OR( span_HAVE( INITIALIZER_LIST ) )
    const std::vector<int> vec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };
#else
    const std::vector<int> vec( 10, 42 );
#endif
    span<const int> v = make_span( vec );

    EXPECT( std::equal( v.begin(), v.end(), vec.begin() ) );
}

CASE( "make_span(): Allows building from a container (with_container_t, std::vector<>)" )
{
#if span_NONSTD_AND( span_FEATURE_TO_STD( WITH_CONTAINER ) )
#if span_STD_OR( span_HAVE( INITIALIZER_LIST ) )
    std::vector<int> vec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };
#else
    std::vector<int> vec; {for ( int i = 1; i < 10; ++i ) vec.push_back(i); }
#endif
    span<int> v = make_span( with_container, vec );

    EXPECT( std::equal( v.begin(), v.end(), vec.begin() ) );
#else
    EXPECT( !!"make_span(with_container,...) is not available (span_PROVIDE_WITH_CONTAINER_TO_STD=0, or using std::span)" );
#endif
}

CASE( "make_span(): Allows building from a const container (with_container_t, std::vector<>)" )
{
#if span_NONSTD_AND( span_FEATURE_TO_STD( WITH_CONTAINER ) )
#if span_STD_OR( span_HAVE( INITIALIZER_LIST ) )
    const std::vector<int> vec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };
#else
    const std::vector<int> vec( 10, 42 );
#endif
    span<const int> v = make_span( with_container, vec );

    EXPECT( std::equal( v.begin(), v.end(), vec.begin() ) );
#else
    EXPECT( !!"make_span(with_container,...) is not available (span_PROVIDE_WITH_CONTAINER_TO_STD=0, or using std::span)" );
#endif
}

#endif // span_FEATURE_MAKE_SPAN

#if span_FEATURE( BYTE_SPAN )

CASE( "byte_span() [span_FEATURE_BYTE_SPAN=1]" )
{
    EXPECT( !!"(avoid warning)" );  // suppress: unused parameter 'lest_env' [-Wunused-parameter]
}

CASE( "byte_span(): Allows building a span of std::byte from a single object (C++17, byte-lite)" )
{
#if span_CPP11_OR_GREATER && ( span_HAVE( BYTE ) || span_HAVE( NONSTD_BYTE ) )
    int x = ~0;

    span<xstd::byte> spn = byte_span( x );

    EXPECT( spn.size() == size_type( sizeof x ) );
#if span_STD_OR( span_HAVE( NONSTD_BYTE ) )
    EXPECT( spn[0]     == to_byte( 0xff ) );
#else
    EXPECT( spn[0]     == xstd::byte( 0xff ) );
#endif
#else
    EXPECT( !!"(non)std::byte is not available (no C++17, no byte-lite); test requires C++11" );
#endif
}

CASE( "byte_span(): Allows building a span of const std::byte from a single const object (C++17, byte-lite)" )
{
#if span_CPP11_OR_GREATER && ( span_HAVE( BYTE ) || span_HAVE( NONSTD_BYTE ) )
    const int x = ~0;

    span<const xstd::byte> spn = byte_span( x );

    EXPECT( spn.size() == size_type( sizeof x ) );
#if span_STD_OR( span_HAVE( NONSTD_BYTE ) )
    EXPECT( spn[0]     == to_byte( 0xff ) );
#else
    EXPECT( spn[0]     == xstd::byte( 0xff ) );
#endif
#else
    EXPECT( !!"(non)std::byte is not available (no C++17, no byte-lite); test requires C++11" );
#endif
}

#endif // span_FEATURE( BYTE_SPAN )

CASE( "first(), last(), subspan() [span_FEATURE_NON_MEMBER_FIRST_LAST_SUB=1]" )
{
    EXPECT( !!"(avoid warning)" );  // suppress: unused parameter 'lest_env' [-Wunused-parameter]
}

CASE( "first(): Allows to create a sub span of the first n elements (span, template parameter)" )
{
#if span_NONSTD_AND( span_FEATURE( NON_MEMBER_FIRST_LAST_SUB_SPAN ) )
    int arr[] = { 1, 2, 3, 4, 5, };
    span<int> spn( arr );
    const size_type count = 3;

    span<      int, count> s = first<count>( spn );
    span<const int, count> t = first<count>( spn );

    EXPECT( s.size() == count );
    EXPECT( t.size() == count );
    EXPECT( std::equal( s.begin(), s.end(), &arr[0] ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] ) );
#else
    EXPECT( !!"first() is not available (NON_MEMBER_FIRST_LAST_SUB_SPAN=0, or using std::span)" );
#endif
}

CASE( "first(): Allows to create a sub span of the first n elements (span, function parameter)" )
{
#if span_NONSTD_AND( span_FEATURE( NON_MEMBER_FIRST_LAST_SUB_SPAN ) )
    int arr[] = { 1, 2, 3, 4, 5, };
    span<int> spn( arr );
    size_type count = 3;

    span<      int> s = first( spn, count );
    span<const int> t = first( spn, count );

    EXPECT( s.size() == count );
    EXPECT( t.size() == count );
    EXPECT( std::equal( s.begin(), s.end(), &arr[0] ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] ) );
#else
    EXPECT( !!"first() is not available (NON_MEMBER_FIRST_LAST_SUB_SPAN=0, or using std::span)" );
#endif
}

CASE( "first(): Allows to create a sub span of the first n elements (compatible container, template parameter)" )
{
#if span_NONSTD_AND( span_FEATURE( NON_MEMBER_FIRST_LAST_SUB_CONTAINER ) )
#if span_CPP11_120
    int arr[] = { 1, 2, 3, 4, 5, };
    std::vector<int> v( &arr[0], &arr[0] + DIMENSION_OF(arr) );
    const size_type count = 3;

    span<      int, count> s = first<count>( v );
    span<const int, count> t = first<count>( v );

    EXPECT( s.size() == count );
    EXPECT( t.size() == count );
    EXPECT( std::equal( s.begin(), s.end(), &arr[0] ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] ) );
#else
    EXPECT( !!"first() is not available (no C++11)" );
#endif
#else
    EXPECT( !!"first() is not available (NON_MEMBER_FIRST_LAST_SUB_CONTAINER=0, or using std::span)" );
#endif
}

CASE( "first(): Allows to create a sub span of the first n elements (compatible container, function parameter)" )
{
#if span_NONSTD_AND( span_FEATURE( NON_MEMBER_FIRST_LAST_SUB_CONTAINER ) )
#if span_CPP11_120
    int arr[] = { 1, 2, 3, 4, 5, };
    std::vector<int> v( &arr[0], &arr[0] + DIMENSION_OF(arr) );
    size_type count = 3;

    span<      int> s = first( v, count );
    span<const int> t = first( v, count );

    EXPECT( s.size() == count );
    EXPECT( t.size() == count );
    EXPECT( std::equal( s.begin(), s.end(), &arr[0] ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] ) );
#else
    EXPECT( !!"first() is not available (no C++11)" );
#endif
#else
    EXPECT( !!"first() is not available (NON_MEMBER_FIRST_LAST_SUB_CONTAINER=0, or using std::span)" );
#endif
}

CASE( "last(): Allows to create a sub span of the last n elements (span, template parameter)" )
{
#if span_NONSTD_AND( span_FEATURE( NON_MEMBER_FIRST_LAST_SUB_SPAN ) )
    int arr[] = { 1, 2, 3, 4, 5, };
    span<int> spn( arr );
    const size_type count = 3;

    span<      int, count> s = last<count>( spn );
    span<const int, count> t = last<count>( spn );

    EXPECT( s.size() == count );
    EXPECT( t.size() == count );
    EXPECT( std::equal( s.begin(), s.end(), &arr[0] + spn.size() - count ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] + spn.size() - count ) );
#else
    EXPECT( !!"last() is not available (NON_MEMBER_FIRST_LAST_SUB_SPAN=0, or using std::span)" );
#endif
}

CASE( "last(): Allows to create a sub span of the last n elements (span, function parameter)" )
{
#if span_NONSTD_AND( span_FEATURE( NON_MEMBER_FIRST_LAST_SUB_SPAN ) )
    int arr[] = { 1, 2, 3, 4, 5, };
    span<int> spn( arr );
    size_type count = 3;

    span<      int> s = last( spn, count );
    span<const int> t = last( spn, count );

    EXPECT( s.size() == count );
    EXPECT( t.size() == count );
    EXPECT( std::equal( s.begin(), s.end(), &arr[0] + spn.size() - count ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] + spn.size() - count ) );
#else
    EXPECT( !!"last() is not available (NON_MEMBER_FIRST_LAST_SUB_SPAN=0, or using std::span)" );
#endif
}

CASE( "last(): Allows to create a sub span of the last n elements (compatible container, template parameter)" )
{
#if span_NONSTD_AND( span_FEATURE( NON_MEMBER_FIRST_LAST_SUB_CONTAINER ) )
#if span_CPP11_120
    int arr[] = { 1, 2, 3, 4, 5, };
    std::vector<int> v( &arr[0], &arr[0] + DIMENSION_OF(arr) );
    const size_type count = 3;

    span<      int, count> s = last<count>( v );
    span<const int, count> t = last<count>( v );

    EXPECT( s.size() == count );
    EXPECT( t.size() == count );
    EXPECT( std::equal( s.begin(), s.end(), &arr[0] + v.size() - count ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] + v.size() - count ) );
#else
    EXPECT( !!"last() is not available (no C++11)" );
#endif
#else
    EXPECT( !!"last() is not available (NON_MEMBER_FIRST_LAST_SUB_CONTAINER=0, or using std::span)" );
#endif
}

CASE( "last(): Allows to create a sub span of the last n elements (compatible container, function parameter)" )
{
#if span_NONSTD_AND( span_FEATURE( NON_MEMBER_FIRST_LAST_SUB_CONTAINER ) )
#if span_CPP11_120
    int arr[] = { 1, 2, 3, 4, 5, };
    std::vector<int> v( &arr[0], &arr[0] + DIMENSION_OF(arr) );
    size_type count = 3;

    span<      int> s = last( v, count );
    span<const int> t = last( v, count );

    EXPECT( s.size() == count );
    EXPECT( t.size() == count );
    EXPECT( std::equal( s.begin(), s.end(), &arr[0] + v.size() - count ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] + v.size() - count ) );
#else
    EXPECT( !!"last() is not available (no C++11)" );
#endif
#else
    EXPECT( !!"last() is not available (NON_MEMBER_FIRST_LAST_SUB_CONTAINER=0, or using std::span)" );
#endif
}

CASE( "subspan(): Allows to create a sub span starting at a given offset (span, template parameter)" )
{
#if span_NONSTD_AND( span_FEATURE( NON_MEMBER_FIRST_LAST_SUB_SPAN ) )
    int arr[] = { 1, 2, 3, };
    span<int> spn( arr );
    const size_type offset = 1;
    const size_type count  = DIMENSION_OF(arr) - offset;

    span<      int, count> s = subspan<offset, count>( spn );
    span<const int, count> t = subspan<offset, count>( spn );

    EXPECT( s.size() == count );
    EXPECT( t.size() == count );
    EXPECT( std::equal( s.begin(), s.end(), &arr[0] + offset ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] + offset ) );
#else
    EXPECT( !!"subspan() is not available (NON_MEMBER_FIRST_LAST_SUB_SPAN=0, or using std::span)" );
#endif
}

CASE( "subspan(): Allows to create a sub span starting at a given offset (span, function parameter)" )
{
#if span_NONSTD_AND( span_FEATURE( NON_MEMBER_FIRST_LAST_SUB_SPAN ) )
    int arr[] = { 1, 2, 3, };
    span<int> spn( arr );
    size_type offset = 1;

    span<      int> s = subspan( spn, offset );
    span<const int> t = subspan( spn, offset );

    EXPECT( s.size() == spn.size() - offset );
    EXPECT( t.size() == spn.size() - offset );
    EXPECT( std::equal( s.begin(), s.end(), &arr[0] + offset ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] + offset ) );
#else
    EXPECT( !!"subspan() is not available (NON_MEMBER_FIRST_LAST_SUB_SPAN=0, or using std::span)" );
#endif
}

CASE( "subspan(): Allows to create a sub span starting at a given offset (compatible container, template parameter)" )
{
#if span_NONSTD_AND( span_FEATURE( NON_MEMBER_FIRST_LAST_SUB_CONTAINER ) )
#if span_CPP11_120
    int arr[] = { 1, 2, 3, };
    std::vector<int> v( &arr[0], &arr[0] + DIMENSION_OF(arr) );
    const size_type offset = 1;
    const size_type count  = DIMENSION_OF(arr) - offset;

    span<      int, count> s = subspan<offset, count>( v );
    span<const int, count> t = subspan<offset, count>( v );

    EXPECT( s.size() == count );
    EXPECT( t.size() == count );
    EXPECT( std::equal( s.begin(), s.end(), &arr[0] + offset ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] + offset ) );
#else
    EXPECT( !!"subspan() is not available (no C++11)" );
#endif
#else
    EXPECT( !!"subspan() is not available (NON_MEMBER_FIRST_LAST_SUB_CONTAINER=0, or using std::span)" );
#endif
}

CASE( "subspan(): Allows to create a sub span starting at a given offset (compatible container, function parameter)" )
{
#if span_NONSTD_AND( span_FEATURE( NON_MEMBER_FIRST_LAST_SUB_CONTAINER ) )
#if span_CPP11_120
    int arr[] = { 1, 2, 3, };
    std::vector<int> v( &arr[0], &arr[0] + DIMENSION_OF(arr) );
    size_type offset = 1;

    span<      int> s = subspan( v, offset );
    span<const int> t = subspan( v, offset );

    EXPECT( s.size() == v.size() - offset );
    EXPECT( t.size() == v.size() - offset );
    EXPECT( std::equal( s.begin(), s.end(), &arr[0] + offset ) );
    EXPECT( std::equal( t.begin(), t.end(), &arr[0] + offset ) );
#else
    EXPECT( !!"subspan() is not available (no C++11)" );
#endif
#else
    EXPECT( !!"subspan() is not available (NON_MEMBER_FIRST_LAST_SUB_CONTAINER=0, or using std::span)" );
#endif
}

CASE( "size(): Allows to obtain the number of elements via size()" )
{
    int a[] = { 1, 2, 3, };
    int b[] = { 1, 2, 3, 4, 5, };

    span<int> z;
    span<int> va( a );
    span<int> vb( b );

    EXPECT( size( va ) == DIMENSION_OF( a ) );
    EXPECT( size( vb ) == DIMENSION_OF( b ) );
    EXPECT( size( z  ) == size_t( 0 ) );
}

CASE( "ssize(): Allows to obtain the number of elements via ssize()" )
{
    int a[] = { 1, 2, 3, };
    int b[] = { 1, 2, 3, 4, 5, };

    span<int> z;
    span<int> va( a );
    span<int> vb( b );

    EXPECT( ssize( va ) == ssize_type( DIMENSION_OF( a ) ) );
    EXPECT( ssize( vb ) == ssize_type( DIMENSION_OF( b ) ) );
    EXPECT( ssize( z  ) == 0 );
}

CASE( "tuple_size<>: Allows to obtain the number of elements via std::tuple_size<> (C++11)" )
{
#if span_NONSTD_AND( span_HAVE( STRUCT_BINDING ) )
    const auto N = 3u;
    using T = span<int, N>;

    int a[N] = { 1, 2, 3, };

    T v( a );

    EXPECT(           ssize( v )        == N );
    EXPECT( std::tuple_size< T >::value == N );

    static_assert( std::tuple_size< T >::value == N, "std::tuple_size< span<> > fails" );
#else
    EXPECT( !!"std::tuple_size<> is not available (no C++11)" );
#endif
}

CASE( "tuple_element<>: Allows to obtain an element via std::tuple_element<> (C++11)" )
{
#if span_NONSTD_AND( span_HAVE( STRUCT_BINDING ) )
    using S = span<int,3>;
    using T = std::tuple_element<0, S>::type;

    EXPECT( (std::is_same<T, int>::value) );

   static_assert( std::is_same<T, int>::value, "std::tuple_element<0, S>::type fails" );
#else
    EXPECT( !!"std::tuple_element<> is not available (no C++11)" );
#endif
}

CASE( "tuple_element<>: Allows to obtain an element via std::tuple_element_t<> (C++11)" )
{
#if span_NONSTD_AND( span_HAVE( STRUCT_BINDING ) ) && span_CPP11_140
    using S = span<int,3>;
    using T = std::tuple_element_t<0, S>;

    EXPECT( (std::is_same<T, int>::value) );

   static_assert( std::is_same<T, int>::value, "std::tuple_element_t<0, S> fails" );
#else
    EXPECT( !!"std::tuple_element<> is not available (no C++11)" );
#endif
}

CASE( "get<I>(spn): Allows to access an element via std::get<>()" )
{
#if span_NONSTD_AND( span_HAVE( STRUCT_BINDING ) )
    SETUP("") {
        const auto N = 3u;

        using T = span<int, N>;
        using U = span<const int, N>;

              int a[N] = { 1, 2, 3, };
        const int b[N] = { 1, 2, 3, };

              T vna( a );
              U vnb( b );
        const T vca( a );
        const U vcb( b );

    SECTION("lvalue")
    {

        EXPECT( std::get< 1 >( vna ) == a[1] );
        EXPECT( std::get< 1 >( vnb ) == a[1] );
        EXPECT( std::get< 1 >( vca ) == a[1] );
        EXPECT( std::get< 1 >( vcb ) == a[1] );

        std::get< 1 >( vna ) = 7;
//      std::get< 1 >( vca ) = 7; // read-only

        EXPECT( std::get< 1 >( vna ) == 7 );

        // static_assert( std::get< 1 >( vc ) == 2, "std::tuple_element<I>(spn) fails" );
    }

    SECTION("rvalue")
    {
        EXPECT( std::get< 1 >( std::move(vna) ) == 2 );
        EXPECT( std::get< 1 >( std::move(vnb) ) == 2 );
        EXPECT( std::get< 1 >( std::move(vca) ) == 2 );
        EXPECT( std::get< 1 >( std::move(vcb) ) == 2 );
    }}
#else
    EXPECT( !!"std::get<>(spn) is not available (no C++11)" );
#endif
}

// Issues

#include <cassert>

CASE( "[hide][issue-3: heterogeneous comparison]" )
{
#if span_NONSTD_AND( span_FEATURE_TO_STD( MAKE_SPAN ) )
#if span_FEATURE( COMPARISON )
    static const int data[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, };

    span< const int > spn( data );

    EXPECT( !!"(avoid warning)" );  // suppress: unused parameter 'lest_env' [-Wunused-parameter]

    assert( make_span( data ) == make_span(data) ); // Ok, non-heterogeneous comparison
    assert( make_span( data ) == spn             ); // Compile error: comparing fixed with dynamic extension
#else
    EXPECT( !!"test is unavailable as comparison of span is not provided via span_FEATURE_COMPARISON=1" );
#endif // span_FEATURE( COMPARISON )
#else
    EXPECT( !!"test is unavailable as make_span() is not provided via span_FEATURE_MAKE_SPAN_TO_STD=99" );
#endif // span_FEATURE_TO_STD( MAKE_SPAN )
}

CASE( "[hide][issue-3: same()]" )
{
#if span_NONSTD_AND( span_FEATURE_TO_STD( MAKE_SPAN ) )
#if span_FEATURE( SAME )
    EXPECT( !!"(avoid warning)" );  // suppress: unused parameter 'lest_env' [-Wunused-parameter]

    typedef unsigned char uint8_type;

    static uint8_type const data[]    = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    static float      const farray[4] = { 0, 1, 2, 3 };

    span<float const> fspan1 = make_span( farray );

    assert( fspan1.data() == farray );
    assert( fspan1.size() == static_cast<size_type>( DIMENSION_OF( farray ) ) );

#if span_STD_OR( span_HAVE( BYTE ) )
    span<std::byte const> fspan2 = byte_span( farray[0] );

    assert( static_cast<void const *>( fspan1.data() ) == fspan2.data() );
    assert(                            fspan1.size()   == fspan2.size() );
    assert(                    ! same( fspan1          ,  fspan2 )      );
# endif

    span<uint8_type const> bspan4 = make_span( &data[0], 4 );

    assert(        bspan4 == fspan1   );
    assert(        fspan1 == bspan4   );
    assert( !same( fspan1 ,  bspan4 ) );

#if span_STD_OR( span_HAVE( BYTE ) )
    assert(        as_bytes( fspan1 ) != as_bytes( bspan4 )   );
    assert( !same( as_bytes( fspan1 ) ,  as_bytes( bspan4 ) ) );
#endif

    union
    {
        int i;
        float f;
        char c;
    } u = { 0x12345678 };

    span<int  > uspan1 = make_span( &u.i, 1 );
    span<float> uspan2 = make_span( &u.f, 1 );
    span<char > uspan3 = make_span( &u.c, 1 );

    assert( static_cast<void const *>( uspan1.data() ) == uspan2.data() );
    assert(                            uspan1.size()   == uspan2.size() );
    assert( static_cast<void const *>( uspan1.data() ) == uspan3.data() );
    assert(                            uspan1.size()   == uspan3.size() );

    assert( !same( uspan1, uspan2 ) );
    assert( !same( uspan1, uspan3 ) );
    assert( !same( uspan2, uspan3 ) );

    assert( uspan1 != uspan2 );
    assert( uspan1 != uspan3 );
    assert( uspan2 != uspan3 );

#if span_STD_OR( span_HAVE( BYTE ) )
    assert(  same( as_bytes( uspan1 ), as_bytes( uspan2 ) ) );
    assert( !same( as_bytes( uspan1 ), as_bytes( uspan3 ) ) );
    assert( !same( as_bytes( uspan2 ), as_bytes( uspan3 ) ) );
#endif
#else
    EXPECT( !!"same() is not provided via span_FEATURE_SAME=1" );
#endif // span_FEATURE( SAME )
#else
    EXPECT( !!"test is unavailable as make_span is not provided via span_FEATURE_MAKE_SPAN_TO_STD=99" );
#endif // span_FEATURE_TO_STD( MAKE_SPAN )
}

// issue #64

#if span_HAVE_ITERATOR_CTOR

namespace issue64 {

    void fun(nonstd::span<unsigned>) {}
    void fun(nonstd::span<nonstd::span<unsigned> >) {}
}
#endif

CASE( "[hide][issue-64: overly permissive constructor]" )
{
#if span_HAVE_ITERATOR_CTOR
    using issue64::fun;
    unsigned u;

    fun( {&u, 1u} );
#else
    EXPECT( !!"construction from iterator is not available (no C++11)" );
#endif
}

CASE( "[hide][issue-69: constructor from iterators is not properly constrained]" )
{
#if span_HAVE_ITERATOR_CTOR
    static_assert(
        std::is_constructible<nonstd::span<float>, int*, int*>::value == false,
        "span<float> should not be constructible from int"
    );

    static_assert(
        std::is_constructible<nonstd::span<float const>, int*, int*>::value == false,
        "span<const float> should not be constructible from int"
    );
#else
    EXPECT( !!"construction from iterator is not available (no C++11)" );
#endif
}

CASE( "tweak header: reads tweak header if supported " "[tweak]" )
{
#if span_HAVE( TWEAK_HEADER )
    EXPECT( SPAN_TWEAK_VALUE == 42 );
#else
    EXPECT( !!"Tweak header is not available (span_HAVE_TWEAK_HEADER: 0)." );
#endif
}

// end of file
