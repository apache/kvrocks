// Copyright (c) 2016-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include <tao/pegtl/internal/cstring_reader.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   template< typename Rule,
             template< typename... > class Action = nothing,
             template< typename... > class Control = normal >
   bool parse_cstring( const char* string, const char* source, const std::size_t maximum )
   {
      buffer_input< internal::cstring_reader > in( source, maximum, string );
      return parse< Rule, Action, Control >( in );
   }

   struct test_grammar
      : seq< string< 'a', 'b', 'c', 'd', 'e', 'f' >, not_at< any >, eof >
   {};

   void unit_test()
   {
      TAO_PEGTL_TEST_ASSERT( parse_cstring< test_grammar >( "abcdef", "test data", 10 ) );
      TAO_PEGTL_TEST_ASSERT( parse_cstring< test_grammar >( "abcdef\0g", "test data", 10 ) );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
