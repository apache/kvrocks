// Copyright (c) 2019-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   // clang-format off
   struct A : one< 'a' > {};
   struct B : one< 'b' > {};
   struct AB : seq< A, B > {};
   // clang-format on

   template< typename >
   struct my_action_1
   {};

   template< typename >
   struct my_action_2
   {};

   template<>
   struct my_action_1< A >
   {
      static void apply0( int& c )
      {
         TAO_PEGTL_TEST_ASSERT( c == 0 );
         c = 1;
      }
   };

   template<>
   struct my_action_1< B >
      : change_action_and_states< my_action_2, int >
   {
      // not called because my_action_2 is active
      static void apply0( int& /*v*/ )
      {
         TAO_PEGTL_TEST_UNREACHABLE;
      }

      template< typename ParseInput >
      static void success( const ParseInput& /*unused*/, int& v, int& c )
      {
         TAO_PEGTL_TEST_ASSERT( v == 2 );
         TAO_PEGTL_TEST_ASSERT( c == 1 );
         c = 3;
      }
   };

   template<>
   struct my_action_2< A >
   {
      static void apply0( int& /*c*/ )
      {
         TAO_PEGTL_TEST_UNREACHABLE;
      }
   };

   template<>
   struct my_action_2< B >
   {
      static void apply0( int& v )
      {
         TAO_PEGTL_TEST_ASSERT( v == 0 );
         v = 2;
      }
   };

   void unit_test()
   {
      {
         memory_input in( "ab", "" );
         int c = 0;
         const auto result = parse< AB, my_action_1 >( in, c );
         TAO_PEGTL_TEST_ASSERT( result );
         TAO_PEGTL_TEST_ASSERT( c == 3 );
      }
      {
         memory_input in( "a", "" );
         int c = 0;
         const auto result = parse< AB, my_action_1 >( in, c );
         TAO_PEGTL_TEST_ASSERT( !result );
         TAO_PEGTL_TEST_ASSERT( c == 1 );
      }
      {
         memory_input in( "b", "" );
         int c = 0;
         const auto result = parse< AB, my_action_1 >( in, c );
         TAO_PEGTL_TEST_ASSERT( !result );
         TAO_PEGTL_TEST_ASSERT( c == 0 );
      }
      {
         memory_input in( "ab", "" );
         int c = 5;
         const auto result = parse< disable< AB >, my_action_1 >( in, c );
         TAO_PEGTL_TEST_ASSERT( result );
         TAO_PEGTL_TEST_ASSERT( c == 5 );
      }
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
