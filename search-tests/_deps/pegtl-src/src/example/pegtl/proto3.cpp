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

#include <iomanip>

#include <tao/pegtl.hpp>
#include <tao/pegtl/contrib/analyze.hpp>
#include <tao/pegtl/contrib/proto3.hpp>

int main( int argc, char** argv )  // NOLINT(bugprone-exception-escape)
{
   using namespace TAO_PEGTL_NAMESPACE;

   if( analyze< proto3::proto >() != 0 ) {
      return 1;
   }

   for( int i = 1; i < argc; ++i ) {
      file_input in( argv[ i ] );
      try {
         parse< proto3::proto >( in );
      }
      catch( const parse_error& e ) {
         const auto p = e.positions().front();
         std::cerr << e.what() << '\n'
                   << in.line_at( p ) << '\n'
                   << std::setw( p.column ) << '^' << '\n';
      }
   }
   return 0;
}

#endif
