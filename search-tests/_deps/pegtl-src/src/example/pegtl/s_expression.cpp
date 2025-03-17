// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#if !defined( __cpp_exceptions )
#include <iostream>
int main()
{
   std::cerr << "Exception support required, example unavailable." << std::endl;
   return 1;
}
#else

#include <iomanip>
#include <iostream>

#include <tao/pegtl.hpp>
#include <tao/pegtl/contrib/analyze.hpp>

namespace sexpr
{
   // clang-format off
   struct hash_comment : tao::pegtl::until< tao::pegtl::eolf > {};

   struct list;
   struct list_comment : tao::pegtl::if_must< tao::pegtl::at< tao::pegtl::one< '(' > >, tao::pegtl::disable< list > > {};

   struct read_include : tao::pegtl::seq< tao::pegtl::one< ' ' >, tao::pegtl::one< '"' >, tao::pegtl::plus< tao::pegtl::not_one< '"' > >, tao::pegtl::one< '"' > > {};
   struct hash_include : tao::pegtl::if_must< tao::pegtl::string< 'i', 'n', 'c', 'l', 'u', 'd', 'e' >, read_include > {};

   struct hashed : tao::pegtl::if_must< tao::pegtl::one< '#' >, tao::pegtl::sor< hash_include, list_comment, hash_comment > > {};

   struct number : tao::pegtl::plus< tao::pegtl::digit > {};
   struct symbol : tao::pegtl::identifier {};

   struct atom : tao::pegtl::sor< number, symbol > {};

   struct anything;

   struct list : tao::pegtl::if_must< tao::pegtl::one< '(' >, tao::pegtl::until< tao::pegtl::one< ')' >, anything > > {};

   struct normal : tao::pegtl::sor< atom, list > {};

   struct anything : tao::pegtl::sor< tao::pegtl::space, hashed, normal > {};

   struct main : tao::pegtl::until< tao::pegtl::eof, tao::pegtl::must< anything > > {};
   // clang-format on

   template< typename Rule >
   struct action
   {};

   template<>
   struct action< tao::pegtl::plus< tao::pegtl::not_one< '"' > > >
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, std::string& fn )
      {
         fn = in.string();
      }
   };

   template<>
   struct action< hash_include >
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, std::string& fn )
      {
         std::string f2;
         // Here f2 is the state argument for the nested parsing
         // run (to store the value of the string literal like in
         // the upper-level parsing run), fn is the value of the
         // last string literal that we use as filename here, and
         // the input is passed on for chained error messages (as
         // in "error in line x file foo included from file bar...)
         tao::pegtl::file_input i2( fn );
         tao::pegtl::parse_nested< main, sexpr::action >( in, i2, f2 );
      }
   };

}  // namespace sexpr

int main( int argc, char** argv )  // NOLINT(bugprone-exception-escape)
{
   if( tao::pegtl::analyze< sexpr::main >() != 0 ) {
      return 1;
   }
   for( int i = 1; i < argc; ++i ) {
      std::string fn;
      tao::pegtl::argv_input in( argv, i );
      try {
         tao::pegtl::parse< sexpr::main, sexpr::action >( in, fn );
      }
      catch( const tao::pegtl::parse_error& e ) {
         const auto p = e.positions().front();
         std::cerr << e.what() << '\n'
                   << in.line_at( p ) << '\n'
                   << std::setw( p.column ) << '^' << '\n';
      }
   }
   return 0;
}

#endif
