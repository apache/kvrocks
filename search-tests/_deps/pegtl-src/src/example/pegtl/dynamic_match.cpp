// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <cassert>
#include <cstring>

#include <iostream>
#include <string>

#include <tao/pegtl.hpp>

#include <tao/pegtl/contrib/analyze.hpp>

namespace pegtl = TAO_PEGTL_NAMESPACE;

namespace dynamic
{
   struct long_literal_id
      : pegtl::plus< pegtl::not_one< '[' > >
   {};

   struct long_literal_open
      : pegtl::seq< pegtl::one< '[' >, long_literal_id, pegtl::one< '[' > >
   {};

   struct long_literal_mark
   {
      using rule_t = long_literal_mark;

      template< pegtl::apply_mode,
                pegtl::rewind_mode,
                template< typename... >
                class Action,
                template< typename... >
                class Control,
                typename ParseInput,
                typename... States >
      static bool match( ParseInput& in, const std::string& id, const std::string& /*unused*/, States&&... /*unused*/ )
      {
         if( in.size( id.size() ) >= id.size() ) {
            if( std::memcmp( in.current(), id.data(), id.size() ) == 0 ) {
               in.bump( id.size() );
               return true;
            }
         }
         return false;
      }
   };

   struct long_literal_close
      : pegtl::seq< pegtl::one< ']' >, long_literal_mark, pegtl::one< ']' > >
   {};

   struct long_literal_body
      : pegtl::any
   {};

   struct grammar
      : pegtl::seq< long_literal_open, pegtl::until< long_literal_close, long_literal_body >, pegtl::eof >
   {};

   template< typename Rule >
   struct action
   {};

   template<>
   struct action< long_literal_id >
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, std::string& id, const std::string& /*unused*/ )
      {
         id = in.string();
      }
   };

   template<>
   struct action< long_literal_body >
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, const std::string& /*unused*/, std::string& body )
      {
         body += in.string();
      }
   };

}  // namespace dynamic

namespace TAO_PEGTL_NAMESPACE
{
   template< typename Name >
   struct analyze_traits< Name, dynamic::long_literal_mark >
      : analyze_any_traits<>
   {};

}  // namespace TAO_PEGTL_NAMESPACE

int main( int argc, char** argv )  // NOLINT(bugprone-exception-escape)
{
   if( pegtl::analyze< dynamic::grammar >() != 0 ) {
      std::cerr << "cycles without progress detected!" << std::endl;
      return 1;
   }

   if( argc > 1 ) {
      std::string id;
      std::string body;

      pegtl::argv_input in( argv, 1 );
      if( pegtl::parse< dynamic::grammar, dynamic::action >( in, id, body ) ) {
         std::cout << "long literal id was: " << id << std::endl;
         std::cout << "long literal body was: " << body << std::endl;
      }
      else {
         std::cerr << "parse error for: " << argv[ 1 ] << std::endl;
         return 1;
      }
   }
   return 0;
}
