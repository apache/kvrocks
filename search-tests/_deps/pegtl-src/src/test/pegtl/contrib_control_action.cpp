// Copyright (c) 2020-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <string>

#include "test.hpp"

#include <tao/pegtl/contrib/control_action.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   template< typename Rule >
   struct test_action
      : nothing< Rule >
   {};

   struct first_rule
      : sor< alpha, digit >
   {};

#if defined( __cpp_exceptions )
   struct second_rule
      : must< alnum >
   {};
#endif

   std::string story;

   template<>
   struct test_action< first_rule >
      : control_action
   {
      template< typename ParseInput >
      static void start( const ParseInput& /*unused*/, int /*unused*/ )
      {
         story += 'a';
      }

      template< typename ParseInput >
      static void success( const ParseInput& /*unused*/, int /*unused*/ )
      {
         story += 'b';
      }

      // LCOV_EXCL_START
      template< typename ParseInput >
      static void failure( const ParseInput& /*unused*/, int /*unused*/ )
      {
         TAO_PEGTL_TEST_UNREACHABLE;
      }
      // LCOV_EXCL_STOP
   };

   template<>
   struct test_action< alpha >
      : control_action
   {
      template< typename ParseInput >
      static void start( const ParseInput& /*unused*/, int /*unused*/ )
      {
         story += 'd';
      }

      // LCOV_EXCL_START
      template< typename ParseInput >
      static void success( const ParseInput& /*unused*/, int /*unused*/ )
      {
         TAO_PEGTL_TEST_UNREACHABLE;
      }
      // LCOV_EXCL_STOP

      template< typename ParseInput >
      static void failure( const ParseInput& /*unused*/, int /*unused*/ )
      {
         story += 'f';
      }
   };

   template<>
   struct test_action< digit >
      : control_action
   {
      template< typename ParseInput >
      static void start( const ParseInput& /*unused*/, int /*unused*/ )
      {
         story += 'g';
      }

      template< typename ParseInput >
      static void success( const ParseInput& /*unused*/, int /*unused*/ )
      {
         story += 'h';
      }

      // LCOV_EXCL_START
      template< typename ParseInput >
      static void failure( const ParseInput& /*unused*/, int /*unused*/ )
      {
         TAO_PEGTL_TEST_UNREACHABLE;
      }
      // LCOV_EXCL_STOP
   };

#if defined( __cpp_exceptions )
   template<>
   struct test_action< second_rule >
      : control_action
   {
      template< typename ParseInput >
      static void start( const ParseInput& /*unused*/, int /*unused*/ )
      {
         story += 'j';
      }

      // LCOV_EXCL_START
      template< typename ParseInput >
      static void success( const ParseInput& /*unused*/, int /*unused*/ )
      {
         TAO_PEGTL_TEST_UNREACHABLE;
      }

      template< typename ParseInput >
      static void failure( const ParseInput& /*unused*/, int /*unused*/ )
      {
         TAO_PEGTL_TEST_UNREACHABLE;
      }
      // LCOV_EXCL_STOP

      template< typename ParseInput >
      static void unwind( const ParseInput& /*unused*/, int /*unused*/ )
      {
         story += 'm';
      }
   };
#endif

   void unit_test()
   {
      {
         memory_input in( "0", __FUNCTION__ );
         const auto b = parse< first_rule, test_action >( in, 42 );
         TAO_PEGTL_TEST_ASSERT( b );
         TAO_PEGTL_TEST_ASSERT( story == "adfghb" );
      }
#if defined( __cpp_exceptions )
      story.clear();
      {
         memory_input in( "*", __FUNCTION__ );
         TAO_PEGTL_TEST_THROWS( parse< second_rule, test_action >( in, 42 ) );
         TAO_PEGTL_TEST_ASSERT( story == "jm" );
      }
#endif
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
