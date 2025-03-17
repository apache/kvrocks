// Copyright (c) 2020-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#if !defined( __cpp_exceptions )
#include <iostream>
int main()
{
   std::cout << "Exception support disabled, skipping test..." << std::endl;
}
#else

#include "test.hpp"

#include "verify_meta.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   template< tracking_mode M >
   void unit_test()
   {
      const std::string rulename{ demangle< digit >() };

      memory_input< M > in( "foo\nbar bla blubb\nbaz", "test_source" );

      try {
         parse< seq< identifier, eol, identifier, one< ' ' >, must< digit > > >( in );
      }
      catch( const parse_error& e ) {
         TAO_PEGTL_TEST_ASSERT( e.what() == "test_source:2:5: parse error matching " + rulename );

         TAO_PEGTL_TEST_ASSERT( e.message() == "parse error matching " + rulename );

         TAO_PEGTL_TEST_ASSERT( e.positions().size() == 1 );
         const auto& p = e.positions().front();

         TAO_PEGTL_TEST_ASSERT( p.byte == 8 );
         TAO_PEGTL_TEST_ASSERT( p.line == 2 );
         TAO_PEGTL_TEST_ASSERT( p.column == 5 );
         TAO_PEGTL_TEST_ASSERT( p.source == "test_source" );

         TAO_PEGTL_TEST_ASSERT( in.line_at( p ) == "bar bla blubb" );

         position p2 = p;
         p2.source = "foo";
         p2.line = 42;
         p2.column = 123;

         parse_error e2 = e;
         e2.add_position( std::move( p2 ) );

         TAO_PEGTL_TEST_ASSERT( e2.what() == "foo:42:123: test_source:2:5: parse error matching " + rulename );
         TAO_PEGTL_TEST_ASSERT( e.what() == "test_source:2:5: parse error matching " + rulename );

         return;
      }
      TAO_PEGTL_TEST_UNREACHABLE;  // LCOV_EXCL_LINE
   }

   void unit_test()
   {
      unit_test< tracking_mode::eager >();
      unit_test< tracking_mode::lazy >();
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"

#endif
