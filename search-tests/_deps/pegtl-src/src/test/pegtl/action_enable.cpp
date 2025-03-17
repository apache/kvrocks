// Copyright (c) 2019-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   // clang-format off
   struct A : one< 'a' > {};
   struct B : one< 'b' > {};
   struct C : one< 'c' > {};
   struct BCB : seq< B, C, B > {};
   struct ABCBA : seq< A, BCB, A > {};
   // clang-format on

   template< typename >
   struct my_action
   {};

   template<>
   struct my_action< A >
   {
      static void apply0( int& a, int& /*b*/, int& /*c*/ )
      {
         ++a;
      }
   };

   template<>
   struct my_action< B >
      : disable_action
   {
      static void apply0( int& /*a*/, int& b, int& /*c*/ )
      {
         ++b;
      }
   };

   template<>
   struct my_action< C >
      : enable_action
   {
      static void apply0( int& /*a*/, int& /*b*/, int& c )
      {
         ++c;
      }
   };

   void unit_test()
   {
      memory_input<> in( "abcba", "" );
      int a = 0;
      int b = 0;
      int c = 0;
      const auto result = parse< ABCBA, my_action >( in, a, b, c );
      TAO_PEGTL_TEST_ASSERT( result );
      TAO_PEGTL_TEST_ASSERT( a == 2 );
      TAO_PEGTL_TEST_ASSERT( b == 0 );
      TAO_PEGTL_TEST_ASSERT( c == 1 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
