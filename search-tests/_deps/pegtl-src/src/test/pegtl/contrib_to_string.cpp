// Copyright (c) 2017-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include <tao/pegtl.hpp>
#include <tao/pegtl/contrib/to_string.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      TAO_PEGTL_TEST_ASSERT( to_string< string<> >().empty() );
      TAO_PEGTL_TEST_ASSERT( ( to_string< string< 'a', 'b', 'c' > >() == "abc" ) );

      TAO_PEGTL_TEST_ASSERT( to_string< istring<> >().empty() );
      TAO_PEGTL_TEST_ASSERT( ( to_string< istring< 'a', 'b', 'c' > >() == "abc" ) );

      TAO_PEGTL_TEST_ASSERT( to_string< TAO_PEGTL_STRING( "" ) >().empty() );
      TAO_PEGTL_TEST_ASSERT( to_string< TAO_PEGTL_STRING( "abc" ) >() == "abc" );
      TAO_PEGTL_TEST_ASSERT( to_string< TAO_PEGTL_STRING( "AbC" ) >() == "AbC" );
      TAO_PEGTL_TEST_ASSERT( to_string< TAO_PEGTL_STRING( "abc" ) >() != "AbC" );
      TAO_PEGTL_TEST_ASSERT( to_string< TAO_PEGTL_ISTRING( "abc" ) >() == "abc" );
      TAO_PEGTL_TEST_ASSERT( to_string< TAO_PEGTL_ISTRING( "AbC" ) >() == "AbC" );
      TAO_PEGTL_TEST_ASSERT( to_string< TAO_PEGTL_ISTRING( "abc" ) >() != "AbC" );

      // to_string does *not* care about the outer class template
      TAO_PEGTL_TEST_ASSERT( ( to_string< one< 'a', 'b', 'c' > >() == "abc" ) );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
