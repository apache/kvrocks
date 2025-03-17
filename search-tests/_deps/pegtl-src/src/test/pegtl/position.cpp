// Copyright (c) 2016-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#if !defined( __cpp_exceptions )
#include <iostream>
int main()
{
   std::cout << "Exception support disabled, skipping test..." << std::endl;
}
#else

#include "test.hpp"

#include <tao/pegtl/internal/cstring_reader.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   struct buffer_input_t
      : buffer_input< internal::cstring_reader >
   {
      buffer_input_t( const std::string& in_string, const std::string& in_source )
         : buffer_input< internal::cstring_reader >( in_source, 42, in_string.c_str() )
      {}
   };

   template< typename Rule, typename ParseInput = memory_input<> >
   void test_matches_lf()
   {
      static const std::string s1 = "\n";

      ParseInput i1( s1, __FUNCTION__ );

      TAO_PEGTL_TEST_ASSERT( parse< Rule >( i1 ) );
      TAO_PEGTL_TEST_ASSERT( i1.line() == 2 );
      TAO_PEGTL_TEST_ASSERT( i1.column() == 1 );
   }

   template< typename Rule, typename ParseInput = memory_input<> >
   void test_matches_other( const std::string& s2 )
   {
      TAO_PEGTL_TEST_ASSERT( s2.size() == 1 );

      ParseInput i2( s2, __FUNCTION__ );

      TAO_PEGTL_TEST_ASSERT( parse< Rule >( i2 ) );
      TAO_PEGTL_TEST_ASSERT( i2.line() == 1 );
      TAO_PEGTL_TEST_ASSERT( i2.column() == 2 );
   }

   template< typename Rule, typename ParseInput = memory_input<> >
   void test_mismatch( const std::string& s3 )
   {
      TAO_PEGTL_TEST_ASSERT( s3.size() == 1 );

      ParseInput i3( s3, __FUNCTION__ );

      TAO_PEGTL_TEST_ASSERT( !parse< Rule >( i3 ) );
      TAO_PEGTL_TEST_ASSERT( i3.line() == 1 );
      TAO_PEGTL_TEST_ASSERT( i3.column() == 1 );
   }

   struct outer_grammar
      : must< two< 'a' >, two< 'b' >, two< 'c' >, eof >
   {};

   struct inner_grammar
      : must< one< 'd' >, two< 'e' >, eof >
   {};

   template< typename Rule >
   struct outer_action
   {};

   template<>
   struct outer_action< two< 'b' > >
   {
      template< typename ActionInput >
      static void apply( const ActionInput& oi, const bool mode )
      {
         const auto p = oi.position();
         TAO_PEGTL_TEST_ASSERT( p.source == "outer" );
         TAO_PEGTL_TEST_ASSERT( p.byte == 2 );
         TAO_PEGTL_TEST_ASSERT( p.line == 1 );
         TAO_PEGTL_TEST_ASSERT( p.column == 3 );
         memory_input in( "dFF", "inner" );
         if( mode ) {
            parse_nested< inner_grammar >( oi, in );
         }
         else {
            parse_nested< inner_grammar >( oi.position(), in );
         }
      }
   };

   void test_nested_asserts( const parse_error& e )
   {
      TAO_PEGTL_TEST_ASSERT( e.positions().size() == 2 );
      TAO_PEGTL_TEST_ASSERT( e.positions()[ 0 ].source == "inner" );
      TAO_PEGTL_TEST_ASSERT( e.positions()[ 0 ].byte == 1 );
      TAO_PEGTL_TEST_ASSERT( e.positions()[ 0 ].line == 1 );
      TAO_PEGTL_TEST_ASSERT( e.positions()[ 0 ].column == 2 );
      TAO_PEGTL_TEST_ASSERT( e.positions()[ 1 ].source == "outer" );
      TAO_PEGTL_TEST_ASSERT( e.positions()[ 1 ].byte == 2 );
      TAO_PEGTL_TEST_ASSERT( e.positions()[ 1 ].line == 1 );
      TAO_PEGTL_TEST_ASSERT( e.positions()[ 1 ].column == 3 );
   }

   template< typename ParseInput = memory_input<> >
   void test_nested()
   {
      try {
         memory_input oi( "aabbcc", "outer" );
         parse< outer_grammar, outer_action >( oi, true );
      }
      catch( const parse_error& e ) {
         test_nested_asserts( e );
      }
      try {
         memory_input oi( "aabbcc", "outer" );
         parse< outer_grammar, outer_action >( oi, false );
      }
      catch( const parse_error& e ) {
         test_nested_asserts( e );
      }
   }

   void test_iterator()
   {
      const std::string s = "source";
      const internal::iterator i( nullptr, 1, 2, 3 );
      const position p( i, s );
      TAO_PEGTL_TEST_ASSERT( p.byte == 1 );
      TAO_PEGTL_TEST_ASSERT( p.line == 2 );
      TAO_PEGTL_TEST_ASSERT( p.column == 3 );
      TAO_PEGTL_TEST_ASSERT( p.source == s );
      const position q( 1, 2, 3, s );
      TAO_PEGTL_TEST_ASSERT( q.byte == 1 );
      TAO_PEGTL_TEST_ASSERT( q.line == 2 );
      TAO_PEGTL_TEST_ASSERT( q.column == 3 );
      TAO_PEGTL_TEST_ASSERT( q.source == s );
   }

   void unit_test()
   {
      test_matches_lf< any >();
      test_matches_lf< any, buffer_input_t >();
      test_matches_other< any >( " " );
      test_matches_other< any, buffer_input_t >( " " );

      test_matches_lf< one< '\n' > >();
      test_matches_lf< one< '\n' >, buffer_input_t >();
      test_mismatch< one< '\n' > >( " " );
      test_mismatch< one< '\n' >, buffer_input_t >( " " );

      test_matches_lf< one< ' ', '\n' > >();
      test_matches_lf< one< ' ', '\n' >, buffer_input_t >();
      test_matches_other< one< ' ', '\n' > >( " " );
      test_matches_other< one< ' ', '\n' >, buffer_input_t >( " " );

      test_matches_lf< one< ' ', '\n', 'b' > >();
      test_matches_lf< one< ' ', '\n', 'b' >, buffer_input_t >();
      test_matches_other< one< ' ', '\n', 'b' > >( " " );
      test_matches_other< one< ' ', '\n', 'b' >, buffer_input_t >( " " );

      test_matches_lf< string< '\n' > >();
      test_matches_lf< string< '\n' >, buffer_input_t >();
      test_mismatch< string< '\n' > >( " " );
      test_mismatch< string< '\n' >, buffer_input_t >( " " );

      test_matches_other< string< ' ' > >( " " );
      test_matches_other< string< ' ' >, buffer_input_t >( " " );
      test_mismatch< string< ' ' > >( "\n" );
      test_mismatch< string< ' ' >, buffer_input_t >( "\n" );

      test_matches_lf< range< 8, 33 > >();
      test_matches_lf< range< 8, 33 >, buffer_input_t >();
      test_matches_other< range< 8, 33 > >( " " );
      test_matches_other< range< 8, 33 >, buffer_input_t >( " " );

      test_mismatch< range< 11, 30 > >( "\n" );
      test_mismatch< range< 11, 30 >, buffer_input_t >( "\n" );
      test_mismatch< range< 11, 30 > >( " " );
      test_mismatch< range< 11, 30 >, buffer_input_t >( " " );

      test_matches_lf< not_range< 20, 30 > >();
      test_matches_lf< not_range< 20, 30 >, buffer_input_t >();
      test_matches_other< not_range< 20, 30 > >( " " );
      test_matches_other< not_range< 20, 30 >, buffer_input_t >( " " );

      test_mismatch< not_range< 5, 35 > >( "\n" );
      test_mismatch< not_range< 5, 35 >, buffer_input_t >( "\n" );
      test_mismatch< not_range< 5, 35 > >( " " );
      test_mismatch< not_range< 5, 35 >, buffer_input_t >( " " );

      test_matches_lf< ranges< 'a', 'z', 8, 33, 'A', 'Z' > >();
      test_matches_lf< ranges< 'a', 'z', 8, 33, 'A', 'Z' >, buffer_input_t >();
      test_matches_other< ranges< 'a', 'z', 8, 33, 'A', 'Z' > >( "N" );
      test_mismatch< ranges< 'a', 'z', 8, 33, 'A', 'Z' > >( "9" );
      test_mismatch< ranges< 'a', 'z', 8, 33, 'A', 'Z' >, buffer_input_t >( "9" );

      test_matches_lf< ranges< 'a', 'z', 'A', 'Z', '\n' > >();
      test_matches_lf< ranges< 'a', 'z', 'A', 'Z', '\n' >, buffer_input_t >();
      test_matches_other< ranges< 'a', 'z', 'A', 'Z', '\n' > >( "P" );
      test_matches_other< ranges< 'a', 'z', 'A', 'Z', '\n' >, buffer_input_t >( "P" );
      test_mismatch< ranges< 'a', 'z', 'A', 'Z', '\n' > >( "8" );
      test_mismatch< ranges< 'a', 'z', 'A', 'Z', '\n' >, buffer_input_t >( "8" );

      test_nested<>();
      test_nested< buffer_input_t >();

      test_iterator();
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"

#endif
