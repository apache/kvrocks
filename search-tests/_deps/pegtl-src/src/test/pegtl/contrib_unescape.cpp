// Copyright (c) 2015-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include <tao/pegtl/contrib/unescape.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   // clang-format off
   struct escaped_c : one< '"', '\\', 't' > {};
   struct escaped_u : seq< one< 'u' >, rep< 4, xdigit > > {};
   struct escaped_U : seq< one< 'U' >, rep< 8, xdigit > > {};
   struct escaped_j : list< seq< one< 'j' >, rep< 4, xdigit > >, one< '\\' > > {};
   struct escaped_x : seq< one< 'x' >, rep< 2, xdigit > > {};
   struct escaped : sor< escaped_c, escaped_u, escaped_U, escaped_j, escaped_x > {};
   struct character : if_then_else< one< '\\' >, escaped, utf8::any > {};
   struct unstring : until< eof, character > {};

   template< typename Rule > struct unaction {};

   template<> struct unaction< escaped_c > : unescape::unescape_c< escaped_c, '"', '\\', '\t' > {};
   template<> struct unaction< escaped_u > : unescape::unescape_u {};
   template<> struct unaction< escaped_U > : unescape::unescape_u {};
   template<> struct unaction< escaped_j > : unescape::unescape_j {};
   template<> struct unaction< escaped_x > : unescape::unescape_x {};
   template<> struct unaction< utf8::any > : unescape::append_all {};
   // clang-format on

   template< unsigned M, unsigned N >
   bool verify_data( const char ( &m )[ M ], const char ( &n )[ N ] )
   {
      std::string s;
      memory_input in( m, M - 1, __FUNCTION__ );
      if( !parse< unstring, unaction >( in, s ) ) {
         return false;  // LCOV_EXCL_LINE
      }
      return s == std::string( n, N - 1 );
   }

   bool verify_fail( const std::string& m )
   {
      std::string s;
      memory_input in( m, __FUNCTION__ );
#if defined( __cpp_exceptions )
      try {
         return !parse< unstring, unaction >( in, s );
      }
      catch( const parse_error& ) {
      }
      return true;
#else
      return !parse< unstring, unaction >( in, s );
#endif
   }

   void unit_test()
   {
      TAO_PEGTL_TEST_ASSERT( verify_data( "\\t", "\t" ) );
      TAO_PEGTL_TEST_ASSERT( verify_data( "\\\\", "\\" ) );
      TAO_PEGTL_TEST_ASSERT( verify_data( "abc", "abc" ) );
      TAO_PEGTL_TEST_ASSERT( verify_data( "\\\"foo\\\"", "\"foo\"" ) );
      TAO_PEGTL_TEST_ASSERT( verify_data( "\\x20", " " ) );
      TAO_PEGTL_TEST_ASSERT( verify_data( "\\x30", "0" ) );
      TAO_PEGTL_TEST_ASSERT( verify_data( "\\x2000", " 00" ) );
      TAO_PEGTL_TEST_ASSERT( verify_data( "\\u0020", " " ) );
      TAO_PEGTL_TEST_ASSERT( verify_data( "\\u0020\\u0020", "  " ) );
      TAO_PEGTL_TEST_ASSERT( verify_data( "\\u00e4", "\xc3\xa4" ) );
      TAO_PEGTL_TEST_ASSERT( verify_data( "\\u00E4", "\xC3\xA4" ) );
      TAO_PEGTL_TEST_ASSERT( verify_data( "\\u20ac", "\xe2\x82\xac" ) );

      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\ud800" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\ud800X" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\ud800\\u0020" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\ud800\\udc00" ) );  // unescape_u does not support surrogate pairs.
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\udc00\\ud800" ) );

      TAO_PEGTL_TEST_ASSERT( verify_data( "\\j0020", " " ) );
      TAO_PEGTL_TEST_ASSERT( verify_data( "\\j0020\\j0020", "  " ) );
      TAO_PEGTL_TEST_ASSERT( verify_data( "\\j20ac", "\xe2\x82\xac" ) );

      TAO_PEGTL_TEST_ASSERT( verify_data( "\\jd800\\jdc00", "\xf0\x90\x80\x80" ) );  // unescape_j does support proper surrogate pairs.

      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\jd800" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\jd800X" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\jd800\\j0020" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\jdc00\\jd800" ) );

      TAO_PEGTL_TEST_ASSERT( verify_data( "\\j0000\\u0000\x00", "\x00\x00\x00" ) );

      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\\\\\" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\x" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\xx" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\xa" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\x1" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\x1h" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "a\\" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "a\\x" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "a\\xx" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "a\\xa" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "a\\x1" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "a\\x1h" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\a" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\_" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\z" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\1" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\a00" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\_1111" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\z22222222" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\13333333333333333" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\u" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\uu" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\uuuu" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\u123" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\u999" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\u444h" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\j" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\ju" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\juuu" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\j123" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\j999" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\j444h" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\U00110000" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\U80000000" ) );
      TAO_PEGTL_TEST_ASSERT( verify_fail( "\\Uffffffff" ) );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
