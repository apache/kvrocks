// Copyright (c) 2021 Kelvin Hammond
// Copyright (c) 2021-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#if !defined( __cpp_exceptions )
#include <iostream>
int main()
{
   std::cerr << "Exception support required, example unavailable." << std::endl;
   return 1;
}
#else

#include <tao/pegtl.hpp>
#include <tao/pegtl/contrib/iri.hpp>

#include <iostream>

namespace pegtl = TAO_PEGTL_NAMESPACE;

struct IRI
{
   std::string scheme;
   std::string authority;
   std::string userinfo;
   std::string host;
   std::string port;
   std::string path;
   std::string query;
   std::string fragment;

   explicit IRI( const std::string& iri );
};

namespace iri
{
   template< std::string IRI::*Field >
   struct bind
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, IRI& iri )
      {
         iri.*Field = in.string();
      }
   };

   // clang-format off
   template< typename Rule > struct action {};

   template<> struct action< pegtl::iri::scheme > : bind< &IRI::scheme > {};
   template<> struct action< pegtl::iri::iauthority > : bind< &IRI::authority > {};
   // userinfo: see below
   template<> struct action< pegtl::iri::ihost > : bind< &IRI::host > {};
   template<> struct action< pegtl::iri::port > : bind< &IRI::port > {};
   template<> struct action< pegtl::iri::ipath_noscheme > : bind< &IRI::path > {};
   template<> struct action< pegtl::iri::ipath_rootless > : bind< &IRI::path > {};
   template<> struct action< pegtl::iri::ipath_absolute > : bind< &IRI::path > {};
   template<> struct action< pegtl::iri::ipath_abempty > : bind< &IRI::path > {};
   template<> struct action< pegtl::iri::iquery > : bind< &IRI::query > {};
   template<> struct action< pegtl::iri::ifragment > : bind< &IRI::fragment > {};
   // clang-format on

   template<>
   struct action< pegtl::iri::opt_iuserinfo >
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, IRI& iri )
      {
         if( !in.empty() ) {
            iri.userinfo = std::string( in.begin(), in.size() - 1 );
         }
      }
   };

}  // namespace iri

IRI::IRI( const std::string& iri )
{
   using grammar = pegtl::must< pegtl::iri::IRI >;
   pegtl::memory_input input( iri, "iri" );
   pegtl::parse< grammar, iri::action >( input, *this );
}

int main( int argc, char** argv )
{
   for( int i = 1; i < argc; ++i ) {
      std::cout << "Parsing " << argv[ i ] << std::endl;
      const IRI iri( argv[ i ] );
      std::cout << "IRI.scheme: " << iri.scheme << std::endl;
      std::cout << "IRI.authority: " << iri.authority << std::endl;
      std::cout << "IRI.userinfo: " << iri.userinfo << std::endl;
      std::cout << "IRI.host: " << iri.host << std::endl;
      std::cout << "IRI.port: " << iri.port << std::endl;
      std::cout << "IRI.path: " << iri.path << std::endl;
      std::cout << "IRI.query: " << iri.query << std::endl;
      std::cout << "IRI.fragment: " << iri.fragment << std::endl;
   }
   return 0;
}

#endif
