// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <cassert>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>

#include <tao/pegtl.hpp>
#include <tao/pegtl/contrib/json.hpp>

#include "json_classes.hpp"
#include "json_errors.hpp"
#include "json_unescape.hpp"

namespace pegtl = TAO_PEGTL_NAMESPACE;

namespace example
{
   // State class that stores the result of a JSON parsing run -- a single JSON object.
   // The other members are used temporarily, at the end of a (successful) parsing run.
   // They are expected to be empty.

   struct json_state
   {
      std::shared_ptr< json_base > result;
      std::vector< std::string > keys;
      std::vector< std::shared_ptr< array_json > > arrays;
      std::vector< std::shared_ptr< object_json > > objects;
   };

   // Action class

   template< typename Rule >
   struct action
   {};

   template<>
   struct action< pegtl::json::null >
   {
      static void apply0( json_state& state )
      {
         state.result = std::make_shared< null_json >();
      }
   };

   template<>
   struct action< pegtl::json::true_ >
   {
      static void apply0( json_state& state )
      {
         state.result = std::make_shared< boolean_json >( true );
      }
   };

   template<>
   struct action< pegtl::json::false_ >
   {
      static void apply0( json_state& state )
      {
         state.result = std::make_shared< boolean_json >( false );
      }
   };

   template<>
   struct action< pegtl::json::number >
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, json_state& state )
      {
         std::stringstream ss( in.string() );
         long double v;
         ss >> v;  // NOTE: not quite correct for JSON but we'll use it for this simple example.
         state.result = std::make_shared< number_json >( v );
      }
   };

   template<>
   struct action< pegtl::json::string::content >
      : json_unescape
   {
      template< typename ParseInput >
      static void success( const ParseInput& /*unused*/, std::string& s, json_state& state )
      {
         state.result = std::make_shared< string_json >( std::move( s ) );
      }
   };

   template<>
   struct action< pegtl::json::array::begin >
   {
      static void apply0( json_state& state )
      {
         state.arrays.push_back( std::make_shared< array_json >() );
      }
   };

   template<>
   struct action< pegtl::json::array::element >
   {
      static void apply0( json_state& state )
      {
         state.arrays.back()->data.push_back( std::move( state.result ) );
      }
   };

   template<>
   struct action< pegtl::json::array::end >
   {
      static void apply0( json_state& state )
      {
         state.result = std::move( state.arrays.back() );
         state.arrays.pop_back();
      }
   };

   template<>
   struct action< pegtl::json::object::begin >
   {
      static void apply0( json_state& state )
      {
         state.objects.push_back( std::make_shared< object_json >() );
      }
   };

   // To parse a key, we change the state to decouple string parsing/unescaping

   template<>
   struct action< pegtl::json::key::content >
      : json_unescape
   {
      template< typename ParseInput >
      static void success( const ParseInput& /*unused*/, std::string& s, json_state& state )
      {
         state.keys.push_back( std::move( s ) );
      }
   };

   template<>
   struct action< pegtl::json::object::element >
   {
      static void apply0( json_state& state )
      {
         state.objects.back()->data[ std::move( state.keys.back() ) ] = std::move( state.result );
         state.keys.pop_back();
      }
   };

   template<>
   struct action< pegtl::json::object::end >
   {
      static void apply0( json_state& state )
      {
         state.result = std::move( state.objects.back() );
         state.objects.pop_back();
      }
   };

   using grammar = pegtl::seq< pegtl::json::text, pegtl::eof >;

}  // namespace example

int main( int argc, char** argv )  // NOLINT(bugprone-exception-escape)
{
   if( argc != 2 ) {
      std::cerr << "usage: " << argv[ 0 ] << " <json>\n";
   }
   else {
      example::json_state state;
      pegtl::file_input in( argv[ 1 ] );
#if defined( __cpp_exceptions )
      try {
         pegtl::parse< example::grammar, example::action, example::control >( in, state );
      }
      catch( const pegtl::parse_error& e ) {
         const auto p = e.positions().front();
         std::cerr << e.what() << '\n'
                   << in.line_at( p ) << '\n'
                   << std::setw( p.column ) << '^' << std::endl;
         return 1;
      }
#else

      if( !pegtl::parse< example::grammar, example::action, example::control >( in, state ) ) {
         std::cerr << "error occurred" << std::endl;
         return 1;
      }
#endif
      assert( state.keys.empty() );
      assert( state.arrays.empty() );
      assert( state.objects.empty() );
      std::cout << state.result << std::endl;
   }
   return 0;
}
