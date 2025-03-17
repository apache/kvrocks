// Copyright (c) 2019-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <string>

#include "test.hpp"

#include <tao/pegtl/internal/cstring_reader.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   template< typename Rule, template< typename... > class Action >
   bool parse_cstring( const char* string, const char* source, const std::size_t maximum )
   {
      buffer_input< internal::cstring_reader, eol::lf_crlf, const char*, 1 > in( source, maximum, string );
      return parse< Rule, Action >( in );
   }

   // clang-format off
   struct n : one< 'n' > {};
   struct a : one< 'a' > {};
   struct f : one< 'f' > {};
   struct s : one< 's' > {};

   template< typename Rule > struct my_action {};
   template<> struct my_action< a > : discard_input {};
   template<> struct my_action< f > : discard_input_on_failure {};
   template<> struct my_action< s > : discard_input_on_success {};
   // clang-format on

   void unit_test()
   {
#if defined( __cpp_exceptions )
      TAO_PEGTL_TEST_THROWS( parse_cstring< rep< 4, sor< n, n > >, my_action >( "nnnn", TAO_TEST_LINE, 2 ) );
#endif
      TAO_PEGTL_TEST_ASSERT( parse_cstring< rep< 4, sor< a, n > >, my_action >( "nnnn", TAO_TEST_LINE, 2 ) );
      TAO_PEGTL_TEST_ASSERT( parse_cstring< rep< 4, sor< f, n > >, my_action >( "nnnn", TAO_TEST_LINE, 2 ) );
#if defined( __cpp_exceptions )
      TAO_PEGTL_TEST_THROWS( parse_cstring< rep< 4, sor< s, n > >, my_action >( "nnnn", TAO_TEST_LINE, 2 ) );
#endif

      TAO_PEGTL_TEST_ASSERT( parse_cstring< rep< 4, sor< n, a > >, my_action >( "aaaa", TAO_TEST_LINE, 2 ) );
      TAO_PEGTL_TEST_ASSERT( parse_cstring< rep< 4, sor< a, a > >, my_action >( "aaaa", TAO_TEST_LINE, 2 ) );
      TAO_PEGTL_TEST_ASSERT( parse_cstring< rep< 4, sor< f, a > >, my_action >( "aaaa", TAO_TEST_LINE, 2 ) );
      TAO_PEGTL_TEST_ASSERT( parse_cstring< rep< 4, sor< s, a > >, my_action >( "aaaa", TAO_TEST_LINE, 2 ) );

#if defined( __cpp_exceptions )
      TAO_PEGTL_TEST_THROWS( parse_cstring< rep< 4, sor< n, f > >, my_action >( "ffff", TAO_TEST_LINE, 2 ) );
#endif
      TAO_PEGTL_TEST_ASSERT( parse_cstring< rep< 4, sor< a, f > >, my_action >( "ffff", TAO_TEST_LINE, 2 ) );
#if defined( __cpp_exceptions )
      TAO_PEGTL_TEST_THROWS( parse_cstring< rep< 4, sor< f, f > >, my_action >( "ffff", TAO_TEST_LINE, 2 ) );
      TAO_PEGTL_TEST_THROWS( parse_cstring< rep< 4, sor< s, f > >, my_action >( "ffff", TAO_TEST_LINE, 2 ) );
#endif

      TAO_PEGTL_TEST_ASSERT( parse_cstring< rep< 4, sor< n, s > >, my_action >( "ssss", TAO_TEST_LINE, 2 ) );
      TAO_PEGTL_TEST_ASSERT( parse_cstring< rep< 4, sor< a, s > >, my_action >( "ssss", TAO_TEST_LINE, 2 ) );
      TAO_PEGTL_TEST_ASSERT( parse_cstring< rep< 4, sor< f, s > >, my_action >( "ssss", TAO_TEST_LINE, 2 ) );
      TAO_PEGTL_TEST_ASSERT( parse_cstring< rep< 4, sor< s, s > >, my_action >( "ssss", TAO_TEST_LINE, 2 ) );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
