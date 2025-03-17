// Copyright (c) 2021-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <tao/pegtl/contrib/limit_bytes.hpp>

#include "test.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   struct test_rule
      : star< alpha >
   {};

   struct test_grammar
      : seq< test_rule, eof >
   {};

   template< typename Rule >
   struct test_action
      : nothing< Rule >
   {};

   template<>
   struct test_action< test_rule >
      : limit_bytes< 5 >
   {};

   void unit_test()
   {
      memory_input<> i1( "aaa", __FUNCTION__ );
      const auto r1 = pegtl::parse< test_grammar >( i1 );
      TAO_PEGTL_TEST_ASSERT( r1 );

      memory_input<> i2( "aaaaaaaaaaa", __FUNCTION__ );
      const auto r2 = pegtl::parse< test_grammar >( i2 );
      TAO_PEGTL_TEST_ASSERT( r2 );

      memory_input<> i3( "aaa", __FUNCTION__ );
      const auto r3 = pegtl::parse< test_grammar, test_action >( i3 );
      TAO_PEGTL_TEST_ASSERT( r3 );

#if defined( __cpp_exceptions )
      memory_input<> i4( "aaaaaaaaaaa", __FUNCTION__ );
      TAO_PEGTL_TEST_THROWS( pegtl::parse< test_grammar, test_action >( i4 ) );
#endif
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
