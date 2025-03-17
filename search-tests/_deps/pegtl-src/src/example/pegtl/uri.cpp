// Copyright (c) 2017-2022 Dr. Colin Hirsch and Daniel Frey
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
#include <tao/pegtl/contrib/uri.hpp>

#include <iostream>

namespace pegtl = TAO_PEGTL_NAMESPACE;

struct URI
{
   std::string scheme;
   std::string authority;
   std::string userinfo;
   std::string host;
   std::string port;
   std::string path;
   std::string query;
   std::string fragment;

   explicit URI( const std::string& uri );
};

namespace uri
{
   template< std::string URI::*Field >
   struct bind
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, URI& uri )
      {
         uri.*Field = in.string();
      }
   };

   // clang-format off
   template< typename Rule > struct action {};

   template<> struct action< pegtl::uri::scheme > : bind< &URI::scheme > {};
   template<> struct action< pegtl::uri::authority > : bind< &URI::authority > {};
   // userinfo: see below
   template<> struct action< pegtl::uri::host > : bind< &URI::host > {};
   template<> struct action< pegtl::uri::port > : bind< &URI::port > {};
   template<> struct action< pegtl::uri::path_noscheme > : bind< &URI::path > {};
   template<> struct action< pegtl::uri::path_rootless > : bind< &URI::path > {};
   template<> struct action< pegtl::uri::path_absolute > : bind< &URI::path > {};
   template<> struct action< pegtl::uri::path_abempty > : bind< &URI::path > {};
   template<> struct action< pegtl::uri::query > : bind< &URI::query > {};
   template<> struct action< pegtl::uri::fragment > : bind< &URI::fragment > {};
   // clang-format on

   template<>
   struct action< pegtl::uri::opt_userinfo >
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, URI& uri )
      {
         if( !in.empty() ) {
            uri.userinfo = std::string( in.begin(), in.size() - 1 );
         }
      }
   };

}  // namespace uri

URI::URI( const std::string& uri )
{
   using grammar = pegtl::must< pegtl::uri::URI >;
   pegtl::memory_input input( uri, "uri" );
   pegtl::parse< grammar, uri::action >( input, *this );
}

int main( int argc, char** argv )
{
   for( int i = 1; i < argc; ++i ) {
      std::cout << "Parsing " << argv[ i ] << std::endl;
      const URI uri( argv[ i ] );
      std::cout << "URI.scheme: " << uri.scheme << std::endl;
      std::cout << "URI.authority: " << uri.authority << std::endl;
      std::cout << "URI.userinfo: " << uri.userinfo << std::endl;
      std::cout << "URI.host: " << uri.host << std::endl;
      std::cout << "URI.port: " << uri.port << std::endl;
      std::cout << "URI.path: " << uri.path << std::endl;
      std::cout << "URI.query: " << uri.query << std::endl;
      std::cout << "URI.fragment: " << uri.fragment << std::endl;
   }
   return 0;
}

#endif
