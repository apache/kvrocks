// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <iostream>
#include <string>

#include <tao/pegtl.hpp>

namespace pegtl = TAO_PEGTL_NAMESPACE;

namespace hello
{
   // clang-format off
   struct prefix : pegtl::string< 'H', 'e', 'l', 'l', 'o', ',', ' ' > {};
   struct name : pegtl::plus< pegtl::alpha > {};
   struct grammar : pegtl::seq< prefix, name, pegtl::one< '!' >, pegtl::eof > {};
   // clang-format on

   template< typename Rule >
   struct action
   {};

   template<>
   struct action< name >
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, std::string& v )
      {
         v = in.string();
      }
   };

}  // namespace hello

int main( int argc, char** argv )  // NOLINT(bugprone-exception-escape)
{
   if( argc > 1 ) {
      std::string name;

      pegtl::argv_input in( argv, 1 );
      if( pegtl::parse< hello::grammar, hello::action >( in, name ) ) {
         std::cout << "Good bye, " << name << "!" << std::endl;
      }
      else {
         std::cerr << "I don't understand." << std::endl;
      }
   }
}
