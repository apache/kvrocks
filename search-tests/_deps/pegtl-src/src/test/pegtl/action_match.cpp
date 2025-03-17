// Copyright (c) 2019-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   struct remove_state
   {
      template< typename Rule,
                apply_mode A,
                rewind_mode M,
                template< typename... >
                class Action,
                template< typename... >
                class Control,
                typename ParseInput,
                typename... States >
      [[nodiscard]] static bool match( ParseInput& in, States&&... /*unused*/ )
      {
         return TAO_PEGTL_NAMESPACE::match< Rule, A, M, Action, Control >( in );
      }
   };

   // further generic helpers could be build, e.g.
   //
   // - change_control
   // - remove prefix/suffix from input (e.g. remove surrounding quotes)
   // - replace the input completely?
   // - append states
   // - prepend states
   // - ...

   std::size_t global_state = 0;

   struct state_one
   {
      std::size_t byte_in_line_a;
      std::size_t byte_in_line_b;
   };

   // clang-format off
   struct grammar_inner : one< 'a' > {};
   struct grammar_one_c : seq< grammar_inner > {};
   struct grammar_one_b : seq< grammar_inner, grammar_one_c > {};
   struct grammar_one_a : seq< grammar_inner, grammar_one_b, eof > {};
   // clang-format on

   template< typename Rule >
   struct action_one_b
   {};

   template< typename Rule >
   struct action_one_t
   {};

   template< typename Rule >
   struct action_one_a
   {};

   template<>
   struct action_one_b< grammar_one_c >
      : remove_state
   {};

   template<>
   struct action_one_b< grammar_inner >
   {
      // used inside of remove_state
      template< typename ActionInput >
      static void apply( const ActionInput& /*unused*/ )
      {
         ++global_state;
      }

      // used outside of remove_state
      template< typename ActionInput >
      static void apply( const ActionInput& in, state_one& state )
      {
         state.byte_in_line_b += in.input().byte();
      }
   };

   template<>
   struct action_one_t< grammar_one_b >
      : change_action< action_one_b >
   {};

   template<>
   struct action_one_a< grammar_one_b >
      : change_action< action_one_t >
   {};

   template<>
   struct action_one_a< grammar_inner >
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, state_one& state )
      {
         state.byte_in_line_a += in.input().byte();
      }
   };

   void unit_test()
   {
      state_one state{ 0, 0 };
      bool parse_result = parse< grammar_one_a, action_one_a >( memory_input( "aaa", __FUNCTION__ ), state );
      TAO_PEGTL_TEST_ASSERT( parse_result );
      TAO_PEGTL_TEST_ASSERT( state.byte_in_line_a == 1 );
      TAO_PEGTL_TEST_ASSERT( state.byte_in_line_b == 2 );
      TAO_PEGTL_TEST_ASSERT( global_state == 1 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
