// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

#include <tao/pegtl/contrib/json.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   using GRAMMAR = seq< json::text, eof >;

   void unit_test()
   {
      verify_analyze< GRAMMAR >( __LINE__, __FILE__, true, false );

      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[]", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "{}", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, " [ ] ", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, " { } ", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "   [   ]   ", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "   {   }   ", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[[{}],[],{}]", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[ null, true, false, 0, 1, 2, 123, 1.23, 0.12, -1, -0, -1.23, \"\", \"abc\" ]", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"\\b\"]", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"\\f\"]", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"\\n\"]", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"\\r\"]", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"\\t\"]", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"\\/\"]", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"\\\\\"]", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"\\\"\"]", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"\\u002C\"]", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"\\u002c\"]", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"ab\\u002Ccd\"]", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"ab\\u002ccd\"]", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"\\uD834\\uDD1E\"]", result_type::success, 0 );
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"\\uD834\"]", result_type::success, 0 );           // unfortunately, this is valid for the grammar...
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"\\uDD1E\"]", result_type::success, 0 );           // ...although both inputs are invalid in unicode.
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"\xC3\x84\"]", result_type::success, 0 );          // German a-umlaut
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"\xF4\x8F\xBF\xBF\"]", result_type::success, 0 );  // largest allowed codepoint U+10FFFF
      verify_rule< GRAMMAR >( __LINE__, __FILE__, "[\"\U0010FFFF\"]", result_type::success, 0 );        // largest allowed codepoint U+10FFFF

      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( " ", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "   ", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( " [", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( " ]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[ ", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "] ", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( " [ ", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( " ] ", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\\a\"]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\\c\"]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\\d\"]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\\e\"]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\\v\"]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\\'\"]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\b\"]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\f\"]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\n\"]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\r\"]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\t\"]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\\\"]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\\\\\\\"]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\\u12\"]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\xFF\"]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\xF4\x90\x80\x80\"]", "" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( memory_input( "[\"\xF7\xBF\xBF\xBF\"]", "" ) ) );

      TAO_PEGTL_TEST_ASSERT( parse< GRAMMAR >( file_input( "src/test/pegtl/data/pass1.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( parse< GRAMMAR >( file_input( "src/test/pegtl/data/pass2.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( parse< GRAMMAR >( file_input( "src/test/pegtl/data/pass3.json" ) ) );

      TAO_PEGTL_TEST_ASSERT( parse< GRAMMAR >( file_input( "src/test/pegtl/data/blns.json" ) ) );

      // TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail1.json" ) )); // disabled as it is valid now
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail2.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail3.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail4.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail5.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail6.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail7.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail8.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail9.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail10.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail11.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail12.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail13.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail14.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail15.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail16.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail17.json" ) ) );
      // TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail18.json" ) )); // disabled as deep nesting is allowed
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail19.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail20.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail21.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail22.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail23.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail24.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail25.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail26.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail27.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail28.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail29.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail30.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail31.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail32.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail33.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail34.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail35.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail36.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail37.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail38.json" ) ) );
      TAO_PEGTL_TEST_ASSERT( !parse< GRAMMAR >( file_input( "src/test/pegtl/data/fail39.json" ) ) );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
