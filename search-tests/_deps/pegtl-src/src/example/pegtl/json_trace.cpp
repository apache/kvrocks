// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <iomanip>
#include <iostream>

#include <tao/pegtl.hpp>
#include <tao/pegtl/contrib/json.hpp>
#include <tao/pegtl/contrib/trace.hpp>

#include "json_errors.hpp"

namespace pegtl = TAO_PEGTL_NAMESPACE;

namespace example
{
   using grammar = pegtl::seq< pegtl::json::text, pegtl::eof >;

}  // namespace example

int main( int argc, char** argv )  // NOLINT(bugprone-exception-escape)
{
   if( argc != 2 ) {
      std::cerr << "Usage: " << argv[ 0 ] << " JSON\n"
                << "Trace parsing a JSON text.\n\n"
                << "Example: " << argv[ 0 ] << " '{\"foo\":[42,null]}'" << std::endl;
      return 1;
   }

   pegtl::argv_input in( argv, 1 );
#if defined( __cpp_exceptions )
   try {
      pegtl::standard_trace< example::grammar, pegtl::nothing, example::control >( in );
   }
   catch( const pegtl::parse_error& e ) {
      const auto p = e.positions().front();
      std::cerr << e.what() << '\n'
                << in.line_at( p ) << '\n'
                << std::setw( p.column ) << '^' << std::endl;
      return 1;
   }
#else
   if( !pegtl::standard_trace< example::grammar, pegtl::nothing, example::control >( in ) ) {
      std::cerr << "error occurred" << std::endl;
      return 1;
   }
#endif

   return 0;
}
