// Copyright (c) 2016-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <cerrno>
#include <fstream>

#include "test.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   struct file_content
      : seq< TAO_PEGTL_STRING( "dummy content" ), eol, discard >
   {};

   struct file_grammar
      : seq< rep_min_max< 11, 11, file_content >, eof >
   {};

   void unit_test()
   {
#if defined( __cpp_exceptions )
      {
         const char* filename = "src/test/pegtl/no_such_file.txt";
         try {
            std::ifstream stream( filename );
            parse< file_grammar >( istream_input( stream, 16, filename ) );
            TAO_PEGTL_TEST_UNREACHABLE;  // LCOV_EXCL_LINE
         }
         catch( const std::system_error& e ) {
            TAO_PEGTL_TEST_ASSERT( e.code().category() == std::system_category() );
            TAO_PEGTL_TEST_ASSERT( e.code().value() == ENOENT );
         }
      }
#endif

      const char* filename = "src/test/pegtl/file_data.txt";
      std::ifstream stream( filename );
      TAO_PEGTL_TEST_ASSERT( parse< file_grammar >( istream_input( stream, 16, filename ) ) );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
