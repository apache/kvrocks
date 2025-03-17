// Copyright (c) 2018-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <cassert>
#include <cstring>
#include <iostream>
#include <string>

#include <tao/pegtl.hpp>

namespace pegtl = TAO_PEGTL_NAMESPACE;

namespace example
{
   // Examples for grammars on all levels of the Chomsky hierarchy.

   // Type 3 - Regular Languages

   // The regular language (ab+)* implemented in a straight-forward fashion.

   struct type_3
      : pegtl::star< pegtl::one< 'a' >, pegtl::plus< pegtl::one< 'b' > > >
   {};

   // Type 2 - Context Free Languages

   // The context-free, but not regular, language a^n b^n.

   // Implementation that implicitly uses the C++ call stack.

   struct type_2_recursive
      : pegtl::sor< pegtl::string< 'a', 'b' >, pegtl::seq< pegtl::one< 'a' >, type_2_recursive, pegtl::one< 'b' > > >
   {};

   // Implementation that uses state instead of recursion, an
   // action to set the state, and a custom rule to use it.

   template< char C >
   struct match_n
   {
      template< pegtl::apply_mode,
                pegtl::rewind_mode,
                template< typename... >
                class Action,
                template< typename... >
                class Control,
                typename ParseInput,
                typename... States >
      static bool match( ParseInput& in, std::size_t& count, States&&... /*unused*/ )
      {
         if( in.size( count ) >= count ) {
            for( std::size_t i = 0; i < count; ++i ) {
               if( in.peek_char( i ) != C ) {
                  return false;
               }
            }
            in.bump_in_this_line( count );
            return true;
         }
         return false;
      }
   };

   struct type_2_with_state
      : pegtl::seq< pegtl::star< pegtl::one< 'a' > >, match_n< 'b' > >
   {};

   template< typename Rule >
   struct action_2_with_state
   {};

   template<>
   struct action_2_with_state< pegtl::star< pegtl::one< 'a' > > >
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, std::size_t& count )
      {
         count = in.size();
      }
   };

   // Type 1 - Context Sensitive Languages

   // The context-sensitive, but not context-free, language a^n b^n c^n.

   // Here only the implementation that uses state can be used.
   // Both the match_n<> rule and the action are the same as in
   // the previous non-recursive implementation of a^n b^n.

   struct type_1
      : pegtl::seq< pegtl::star< pegtl::one< 'a' > >, match_n< 'b' >, match_n< 'c' > >
   {};

   template< typename Rule >
   struct action_1
      : action_2_with_state< Rule >
   {};

   // Type 0 - Recursively Enumerable Languages

   // We can use the entire Turing-complete C++ language in custom rules,
   // so we can pretty much do everything if we decide to (pun intended),
   // including recursive and recursively enumerable languages ... within
   // the limits of time and space that we are able to dedicate.

}  // namespace example

int main( int argc, char** argv )
{
   for( int i = 1; i < argc; ++i ) {
      pegtl::argv_input in( argv, i );
      const auto r3 = pegtl::parse< pegtl::seq< example::type_3, pegtl::eof > >( in );
      in.restart();
      const auto r2r = pegtl::parse< pegtl::seq< example::type_2_recursive, pegtl::eof > >( in );
      in.restart();
      std::size_t count = 0;
      const auto r2s = pegtl::parse< pegtl::seq< example::type_2_with_state, pegtl::eof >, example::action_2_with_state >( in, count );
      in.restart();
      const auto r1 = pegtl::parse< pegtl::seq< example::type_1, pegtl::eof >, example::action_1 >( in, count );
      std::cout << r3 << r2r << r2s << r1 << std::endl;
      assert( r2r == r2s );
   }
   return 0;
}
