// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include <tao/pegtl/contrib/trace.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   using GRAMMAR1 = sor< failure, one< 'a' > >;
   using GRAMMAR2 = seq< one< 'a' >, any, any, any, any, one< 'b' >, eof >;
   using GRAMMAR3 = sor< one< 'a' >, one< 'b' > >;
#if defined( __cpp_exceptions )
   using GRAMMAR4 = try_catch< sor< one< 'a' >, must< one< 'b' > > > >;
#endif

   template< typename Rule >
   struct trace_action
   {};

   unsigned a0 = 0;
   unsigned a = 0;

   template<>
   struct trace_action< one< 'a' > >
   {
      template< typename... Ts >
      static void apply0( Ts&&... /*unused*/ )
      {
         ++a0;
      }
   };

   template<>
   struct trace_action< GRAMMAR1 >
   {
      template< typename... Ts >
      static void apply( Ts&&... /*unused*/ )
      {
         ++a;
      }
   };

   void unit_test()
   {
      {
         memory_input in( "ab", "trace test please ignore" );
         const auto result = standard_trace< GRAMMAR1 >( in );
         TAO_PEGTL_TEST_ASSERT( result );
         TAO_PEGTL_TEST_ASSERT( a0 == 0 );
         TAO_PEGTL_TEST_ASSERT( a == 0 );
      }
      {
         memory_input in( "ab", "trace test please ignore" );
         const auto result = standard_trace< GRAMMAR1, trace_action >( in );
         TAO_PEGTL_TEST_ASSERT( result );
         TAO_PEGTL_TEST_ASSERT( a0 == 1 );
         TAO_PEGTL_TEST_ASSERT( a == 1 );
      }
      {
         memory_input in( "a\r\n\t\0b", 6, "trace test please ignore" );
         const auto result = standard_trace< GRAMMAR2 >( in );
         TAO_PEGTL_TEST_ASSERT( result );
         TAO_PEGTL_TEST_ASSERT( a0 == 1 );
         TAO_PEGTL_TEST_ASSERT( a == 1 );
      }
      {
         memory_input in( "a\r\n\t\0b", 6, "trace test please ignore" );
         const auto result = standard_trace< GRAMMAR2, trace_action >( in );
         TAO_PEGTL_TEST_ASSERT( result );
         TAO_PEGTL_TEST_ASSERT( a0 == 2 );
         TAO_PEGTL_TEST_ASSERT( a == 1 );
      }
      {
         memory_input in( "c", "trace test please ignore" );
         const auto result = standard_trace< GRAMMAR3 >( in );
         TAO_PEGTL_TEST_ASSERT( !result );
      }
#if defined( __cpp_exceptions )
      {
         memory_input in( "c", "trace test please ignore" );
         const auto result = standard_trace< GRAMMAR4 >( in );
         TAO_PEGTL_TEST_ASSERT( !result );
      }
#endif
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
