// Copyright (c) 2017-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   template< unsigned Size, apply_mode B, rewind_mode N, typename... Rules >
   struct test_rule
   {
      template< apply_mode A,
                rewind_mode M,
                template< typename... >
                class Action,
                template< typename... >
                class Control,
                typename ParseInput,
                typename... States >
      static bool match( ParseInput& in, States&&... st )
      {
         static_assert( A == B, "unexpected apply mode" );
         static_assert( M == N, "unexpected rewind mode" );

         TAO_PEGTL_TEST_ASSERT( in.size() == Size );

         return seq< Rules... >::template match< A, M, Action, Control >( in, st... );
      }
   };

   namespace test1
   {
      bool apply_result;

      struct grammar
         : test_rule< 2, apply_mode::action, rewind_mode::active, any >
      {};

      template< typename Rule >
      struct apply_bool_action
      {};

      template<>
      struct apply_bool_action< grammar >
      {
         template< typename ActionInput >
         static bool apply( const ActionInput& /*unused*/ )
         {
            return apply_result;
         }
      };

      void apply_bool_true()
      {
         apply_result = true;
         memory_input in( "ab", __FUNCTION__ );
         const auto result = parse< grammar, apply_bool_action >( in );
         TAO_PEGTL_TEST_ASSERT( result );
         TAO_PEGTL_TEST_ASSERT( in.size() == 1 );
         TAO_PEGTL_TEST_ASSERT( in.peek_char() == 'b' );
      }

      void apply_bool_false()
      {
         apply_result = false;
         memory_input in( "ab", __FUNCTION__ );
         const auto result = parse< grammar, apply_bool_action >( in );
         TAO_PEGTL_TEST_ASSERT( !result );
         TAO_PEGTL_TEST_ASSERT( in.size() == 2 );
         TAO_PEGTL_TEST_ASSERT( in.peek_char() == 'a' );
      }

      template< typename Rule >
      struct apply0_bool_action
      {};

      template<>
      struct apply0_bool_action< grammar >
      {
         static bool apply0()
         {
            return apply_result;
         }
      };

      void apply0_bool_true()
      {
         apply_result = true;
         memory_input in( "ab", __FUNCTION__ );
         const auto result = parse< grammar, apply0_bool_action >( in );
         TAO_PEGTL_TEST_ASSERT( result );
         TAO_PEGTL_TEST_ASSERT( in.size() == 1 );
         TAO_PEGTL_TEST_ASSERT( in.peek_char() == 'b' );
      }

      void apply0_bool_false()
      {
         apply_result = false;
         memory_input in( "ab", __FUNCTION__ );
         const auto result = parse< grammar, apply0_bool_action >( in );
         TAO_PEGTL_TEST_ASSERT( !result );
         TAO_PEGTL_TEST_ASSERT( in.size() == 2 );
         TAO_PEGTL_TEST_ASSERT( in.peek_char() == 'a' );
      }

   }  // namespace test1

   void unit_test()
   {
      test1::apply_bool_true();
      test1::apply_bool_false();
      test1::apply0_bool_true();
      test1::apply0_bool_false();
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
