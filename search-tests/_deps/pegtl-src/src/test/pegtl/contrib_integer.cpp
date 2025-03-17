// Copyright (c) 2018-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#if !defined( __cpp_exceptions )
#include <iostream>
int main()
{
   std::cout << "Exception support disabled, skipping test..." << std::endl;
}
#else

#include <limits>
#include <sstream>
#include <utility>

#include "test.hpp"

#include "verify_meta.hpp"
#include "verify_rule.hpp"

#include <tao/pegtl/contrib/integer.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   template< typename Rule >
   struct int_action
      : nothing< Rule >
   {};

   template<>
   struct int_action< signed_rule >
      : signed_action
   {};

   template<>
   struct int_action< unsigned_rule >
      : unsigned_action
   {};

   template< typename S >
   void test_signed( const std::string& i, const S s )
   {
      {
         S st = -123;
         memory_input in( i, __FUNCTION__ );
         parse< must< signed_rule, eof >, int_action >( in, st );
         TAO_PEGTL_TEST_ASSERT( st == s );
      }
      {
         S st = -123;
         memory_input in( i, __FUNCTION__ );
         parse< must< signed_rule_with_action, eof > >( in, st );
         TAO_PEGTL_TEST_ASSERT( st == s );
      }
   }

   template< typename S >
   void test_signed( const std::string& i )
   {
      S st;
      memory_input in( i, __FUNCTION__ );
      TAO_PEGTL_TEST_THROWS( parse< must< signed_rule, eof >, int_action >( in, st ) );
   }

   template< typename S >
   std::string lexical_cast( const S s )
   {
      std::ostringstream oss;
      oss << s;
      return std::move( oss ).str();
   }

   template< typename S >
   void test_signed( const S s )
   {
      S st;
      const auto i = lexical_cast( s );
      memory_input in( i, __FUNCTION__ );
      parse< must< signed_rule, eof >, int_action >( in, st );
      TAO_PEGTL_TEST_ASSERT( st == s );
   }

   template< typename S >
   void test_unsigned( const std::string& i, const S s )
   {
      {
         S st = 123;
         memory_input in( i, __FUNCTION__ );
         parse< must< unsigned_rule, eof >, int_action >( in, st );
         TAO_PEGTL_TEST_ASSERT( st == s );
      }
      {
         S st = 123;
         memory_input in( i, __FUNCTION__ );
         parse< must< unsigned_rule_with_action, eof > >( in, st );
         TAO_PEGTL_TEST_ASSERT( st == s );
      }
   }

   template< typename S >
   void test_unsigned( const std::string& i )
   {
      S st = 123;
      memory_input in( i, __FUNCTION__ );
      TAO_PEGTL_TEST_THROWS( parse< must< unsigned_rule, eof >, int_action >( in, st ) );
   }

   template< typename S >
   void test_unsigned( const S s )
   {
      S st = 123;
      const auto i = lexical_cast( s );
      memory_input in( i, __FUNCTION__ );
      parse< must< unsigned_rule, eof >, int_action >( in, st );
      TAO_PEGTL_TEST_ASSERT( st == s );
   }

   template< auto M >
   using max_seq_rule = seq< one< 'a' >, maximum_rule< std::uint64_t, M >, one< 'b' >, eof >;

   void unit_test()
   {
      test_signed< signed char >( "" );
      test_signed< signed char >( "-" );
      test_signed< signed char >( "+" );
      test_signed< signed char >( "a" );

      test_signed< signed char >( "--0" );
      test_signed< signed char >( "++0" );
      test_signed< signed char >( "-+0" );

      test_signed< signed char >( "0", 0 );
      test_signed< signed char >( "+0", 0 );
      test_signed< signed char >( "-0", 0 );
      test_signed< signed char >( "000" );

      test_signed< signed char >( "+000" );
      test_signed< signed char >( "-000" );

      test_signed< signed char >( "127", 127 );
      test_signed< signed char >( "0127" );

      test_signed< signed char >( "-1", -1 );
      test_signed< signed char >( "-01" );
      test_signed< signed char >( "-001" );

      test_signed< signed char >( "-127", -127 );
      test_signed< signed char >( "-128", -128 );

      test_signed< signed char >( "128" );
      test_signed< signed char >( "-129" );
      test_signed< signed char >( "0128" );
      test_signed< signed char >( "00128" );
      test_signed< signed char >( "-0129" );
      test_signed< signed char >( "-00129" );

      test_unsigned< unsigned char >( "" );
      test_unsigned< unsigned char >( "-" );
      test_unsigned< unsigned char >( "+" );
      test_unsigned< unsigned char >( "a" );

      test_unsigned< unsigned char >( "-0" );
      test_unsigned< unsigned char >( "+1" );

      test_unsigned< unsigned char >( "0", 0 );
      test_unsigned< unsigned char >( "000" );
      test_unsigned< unsigned char >( "0", 0 );
      test_unsigned< unsigned char >( "255", 255 );
      test_unsigned< unsigned char >( "0255" );
      test_unsigned< unsigned char >( "000255" );
      test_unsigned< unsigned char >( "256" );
      test_unsigned< unsigned char >( "0256" );
      test_unsigned< unsigned char >( "000256" );

      test_signed< signed long long >( "0", 0 );
      test_signed< signed long long >( ( std::numeric_limits< signed long long >::max )() );
      test_signed< signed long long >( ( std::numeric_limits< signed long long >::min )() );

      test_unsigned< unsigned long long >( "0", 0 );
      test_unsigned< unsigned long long >( ( std::numeric_limits< unsigned long long >::max )() );

      verify_rule< max_seq_rule< 0 > >( __LINE__, __FILE__, "a0b", result_type::success );
      verify_rule< max_seq_rule< 0 > >( __LINE__, __FILE__, "ab", result_type::local_failure );
      verify_rule< max_seq_rule< 0 > >( __LINE__, __FILE__, "a1b", result_type::local_failure );
      verify_rule< max_seq_rule< 0 > >( __LINE__, __FILE__, "a9b", result_type::local_failure );
      verify_rule< max_seq_rule< 0 > >( __LINE__, __FILE__, "a11b", result_type::local_failure );

      verify_rule< max_seq_rule< 1 > >( __LINE__, __FILE__, "a0b", result_type::success );
      verify_rule< max_seq_rule< 1 > >( __LINE__, __FILE__, "a1b", result_type::success );
      verify_rule< max_seq_rule< 1 > >( __LINE__, __FILE__, "ab", result_type::local_failure );
      verify_rule< max_seq_rule< 1 > >( __LINE__, __FILE__, "a2b", result_type::local_failure );
      verify_rule< max_seq_rule< 1 > >( __LINE__, __FILE__, "a9b", result_type::local_failure );
      verify_rule< max_seq_rule< 1 > >( __LINE__, __FILE__, "a11b", result_type::local_failure );

      verify_rule< max_seq_rule< 2 > >( __LINE__, __FILE__, "a0b", result_type::success );
      verify_rule< max_seq_rule< 2 > >( __LINE__, __FILE__, "a1b", result_type::success );
      verify_rule< max_seq_rule< 2 > >( __LINE__, __FILE__, "a2b", result_type::success );
      verify_rule< max_seq_rule< 2 > >( __LINE__, __FILE__, "ab", result_type::local_failure );
      verify_rule< max_seq_rule< 2 > >( __LINE__, __FILE__, "a3b", result_type::local_failure );
      verify_rule< max_seq_rule< 2 > >( __LINE__, __FILE__, "a9b", result_type::local_failure );
      verify_rule< max_seq_rule< 2 > >( __LINE__, __FILE__, "a11b", result_type::local_failure );

      verify_rule< max_seq_rule< 3 > >( __LINE__, __FILE__, "a0b", result_type::success );
      verify_rule< max_seq_rule< 3 > >( __LINE__, __FILE__, "a3b", result_type::success );
      verify_rule< max_seq_rule< 3 > >( __LINE__, __FILE__, "ab", result_type::local_failure );
      verify_rule< max_seq_rule< 3 > >( __LINE__, __FILE__, "a4b", result_type::local_failure );
      verify_rule< max_seq_rule< 3 > >( __LINE__, __FILE__, "a11b", result_type::local_failure );

      verify_rule< max_seq_rule< 4 > >( __LINE__, __FILE__, "a5b", result_type::local_failure );
      verify_rule< max_seq_rule< 4 > >( __LINE__, __FILE__, "a11b", result_type::local_failure );

      verify_rule< max_seq_rule< 9 > >( __LINE__, __FILE__, "a0b", result_type::success );
      verify_rule< max_seq_rule< 9 > >( __LINE__, __FILE__, "a9b", result_type::success );
      verify_rule< max_seq_rule< 9 > >( __LINE__, __FILE__, "ab", result_type::local_failure );
      verify_rule< max_seq_rule< 9 > >( __LINE__, __FILE__, "a10b", result_type::local_failure );
      verify_rule< max_seq_rule< 9 > >( __LINE__, __FILE__, "a11b", result_type::local_failure );

      verify_rule< max_seq_rule< 10 > >( __LINE__, __FILE__, "a0b", result_type::success );
      verify_rule< max_seq_rule< 10 > >( __LINE__, __FILE__, "a9b", result_type::success );
      verify_rule< max_seq_rule< 10 > >( __LINE__, __FILE__, "a10b", result_type::success );
      verify_rule< max_seq_rule< 10 > >( __LINE__, __FILE__, "ab", result_type::local_failure );
      verify_rule< max_seq_rule< 10 > >( __LINE__, __FILE__, "a11b", result_type::local_failure );
      verify_rule< max_seq_rule< 10 > >( __LINE__, __FILE__, "a19b", result_type::local_failure );

      verify_rule< max_seq_rule< 11 > >( __LINE__, __FILE__, "a0b", result_type::success );
      verify_rule< max_seq_rule< 11 > >( __LINE__, __FILE__, "a9b", result_type::success );
      verify_rule< max_seq_rule< 11 > >( __LINE__, __FILE__, "a10b", result_type::success );
      verify_rule< max_seq_rule< 11 > >( __LINE__, __FILE__, "a11b", result_type::success );
      verify_rule< max_seq_rule< 11 > >( __LINE__, __FILE__, "ab", result_type::local_failure );
      verify_rule< max_seq_rule< 11 > >( __LINE__, __FILE__, "a12b", result_type::local_failure );
      verify_rule< max_seq_rule< 11 > >( __LINE__, __FILE__, "a13b", result_type::local_failure );
      verify_rule< max_seq_rule< 11 > >( __LINE__, __FILE__, "a111b", result_type::local_failure );

      verify_rule< max_seq_rule< 12 > >( __LINE__, __FILE__, "a0b", result_type::success );
      verify_rule< max_seq_rule< 12 > >( __LINE__, __FILE__, "a1b", result_type::success );
      verify_rule< max_seq_rule< 12 > >( __LINE__, __FILE__, "a9b", result_type::success );
      verify_rule< max_seq_rule< 12 > >( __LINE__, __FILE__, "a10b", result_type::success );
      verify_rule< max_seq_rule< 12 > >( __LINE__, __FILE__, "a11b", result_type::success );
      verify_rule< max_seq_rule< 12 > >( __LINE__, __FILE__, "a12b", result_type::success );
      verify_rule< max_seq_rule< 12 > >( __LINE__, __FILE__, "ab", result_type::local_failure );
      verify_rule< max_seq_rule< 12 > >( __LINE__, __FILE__, "a13b", result_type::local_failure );
      verify_rule< max_seq_rule< 12 > >( __LINE__, __FILE__, "a19b", result_type::local_failure );
      verify_rule< max_seq_rule< 12 > >( __LINE__, __FILE__, "a111b", result_type::local_failure );

      verify_rule< max_seq_rule< 18446744073709551614ULL > >( __LINE__, __FILE__, "a18446744073709551614b", result_type::success );
      verify_rule< max_seq_rule< 18446744073709551614ULL > >( __LINE__, __FILE__, "a18446744073709551615b", result_type::local_failure );

      verify_rule< max_seq_rule< 18446744073709551615ULL > >( __LINE__, __FILE__, "a18446744073709551615b", result_type::success );
      verify_rule< max_seq_rule< 18446744073709551615ULL > >( __LINE__, __FILE__, "a18446744073709551616b", result_type::local_failure );
      verify_rule< max_seq_rule< 18446744073709551615ULL > >( __LINE__, __FILE__, "a98446744073709551614b", result_type::local_failure );

      verify_analyze< unsigned_rule >( __LINE__, __FILE__, true, false );
      verify_analyze< unsigned_rule_with_action >( __LINE__, __FILE__, true, false );

      verify_analyze< signed_rule >( __LINE__, __FILE__, true, false );
      verify_analyze< signed_rule_with_action >( __LINE__, __FILE__, true, false );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"

#endif
