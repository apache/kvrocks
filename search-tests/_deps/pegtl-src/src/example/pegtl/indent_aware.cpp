// Copyright (c) 2018-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#if !defined( __cpp_exceptions )
#include <iostream>
int main()
{
   std::cerr << "Exception support required, example unavailable." << std::endl;
   return 1;
}
#else

#include <cassert>
#include <cstddef>
#include <iostream>

#include <tao/pegtl.hpp>

namespace pegtl = TAO_PEGTL_NAMESPACE;

/*

# Example input for Python-esque
# indentation aware grammar example.

a = 1 # foo

def a():
    a = 1
    if 1:
# bar
        if 2:
            a = 1
    else:
        a = 1

def b():
    a = 1

 */

namespace example
{
   // clang-format off
   struct eq : pegtl::one< '=' > {};
   struct co : pegtl::one< ':' > {};
   struct hs : pegtl::one< '#' > {};
   struct ba : pegtl::one< '(' > {};
   struct bz : pegtl::one< ')' > {};

   struct sa : pegtl::star< pegtl::any > {};
   struct s0 : pegtl::star< pegtl::one< ' ' > > {};
   struct s1 : pegtl::plus< pegtl::one< ' ' > > {};

   struct name : pegtl::plus< pegtl::alpha > {};

   struct expression : pegtl::plus< pegtl::digit > {};  // Simplified; this example is about indentation.

   struct space_line : pegtl::seq< s1, pegtl::eolf > {};
   struct hash_line : pegtl::seq< s0, hs, pegtl::until< pegtl::eolf > > {};

   struct trailer : pegtl::must< s0, pegtl::opt< hs, pegtl::until< pegtl::at< pegtl::eolf > > >, pegtl::eolf > {};

   struct def_line : pegtl::if_must< pegtl::string< 'd', 'e', 'f' >, s1, name, s0, ba, s0, bz, s0, co, trailer > {};
   struct if_line : pegtl::if_must< pegtl::string< 'i', 'f' >, s1, expression, s0, co, trailer > {};
   struct else_line : pegtl::if_must< pegtl::string< 'e', 'l', 's', 'e' >, s0, co, trailer > {};
   struct let_line : pegtl::if_must< name, s0, eq, s0, expression, trailer > {};

   struct indent : pegtl::star< pegtl::one< ' ' > > {};
   struct indented : pegtl::must< indent, pegtl::sor< def_line, if_line, else_line, let_line > > {};

   struct something : pegtl::sor< space_line, hash_line, indented > {};

   struct nothing : pegtl::eolf {};

   struct line : pegtl::sor< nothing, something > {};

   struct grammar : pegtl::until< pegtl::eof, pegtl::must< line > > {};
   // clang-format on

   enum class type
   {
      def,
      if_,
      else_,
      let
   };

   struct entry
   {
      entry( const std::size_t i, const example::type t )
         : indent( i ),
           type( t )
      {}

      std::size_t indent;
      example::type type;
   };

   struct state
   {
      std::size_t current_indent = 0;  // Temporary value, the indentation of the current line.
      std::size_t minimum_indent = 0;  // Set to non-zero when the next line needs a greater indent.

      std::vector< entry > stack;  // Follows the nesting of the indented blocks.
   };

   template< typename Rule >
   struct action
   {};

   template<>
   struct action< co >
   {
      static void apply0( state& s )
      {
         s.minimum_indent = s.current_indent + 1;
      }
   };

   template<>
   struct action< def_line >
   {
      static void apply0( state& s )
      {
         s.stack.emplace_back( s.current_indent, type::def );
      }
   };

   template<>
   struct action< if_line >
   {
      static void apply0( state& s )
      {
         s.stack.emplace_back( s.current_indent, type::if_ );
      }
   };

   template<>
   struct action< else_line >
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, state& s )
      {
         assert( !s.stack.empty() );
         if( ( s.stack.back().type != type::if_ ) || ( s.stack.back().indent != s.current_indent ) ) {
            throw pegtl::parse_error( "expected previous 'if' on same indent as current 'else'", in );
         }
         s.stack.back().type = type::else_;
      }
   };

   template<>
   struct action< let_line >
   {
      static void apply0( state& s )
      {
         s.stack.emplace_back( s.current_indent, type::let );
      }
   };

   template<>
   struct action< nothing >
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, state& s )
      {
         if( s.minimum_indent > 0 ) {
            throw pegtl::parse_error( "expected indented block instead of empty line", in );
         }
         s.stack.clear();
      }
   };

   template<>
   struct action< indent >
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, state& s )
      {
         s.current_indent = in.size();
         if( s.current_indent != 0 ) {
            while( ( !s.stack.empty() ) && ( s.stack.back().indent > s.current_indent ) ) {
               s.stack.pop_back();
            }
         }
         if( s.minimum_indent > 0 ) {
            if( s.current_indent < s.minimum_indent ) {
               throw pegtl::parse_error( "expected indented block with more indent", in );
            }
            s.minimum_indent = 0;
         }
         else if( ( !s.stack.empty() ) && ( s.current_indent != s.stack.back().indent ) ) {
            throw pegtl::parse_error( "indentation mismatch", in );
         }
      }
   };

   template<>
   struct action< grammar >
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, state& s )
      {
         if( s.minimum_indent > 0 ) {
            throw pegtl::parse_error( "expected indented block instead of eof", in );
         }
      }
   };

}  // namespace example

int main( int argc, char** argv )  // NOLINT(bugprone-exception-escape)
{
   for( int i = 1; i < argc; ++i ) {
      pegtl::file_input in( argv[ i ] );
      example::state is;
      pegtl::parse< example::grammar, example::action >( in, is );
   }
   return 0;
}

#endif
