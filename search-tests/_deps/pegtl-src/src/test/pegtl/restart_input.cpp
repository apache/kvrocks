// Copyright (c) 2020-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   using grammar = seq< string< 'a', 'b', 'c' >, eof >;

   void test_lazy()
   {
      const std::string data = "abc";
      memory_input< tracking_mode::lazy, eol::lf_crlf, std::string > in( data, __FUNCTION__ );
      bool success = parse< grammar >( in );
      TAO_PEGTL_TEST_ASSERT( success );
      in.restart();
      success = parse< grammar >( in );
      TAO_PEGTL_TEST_ASSERT( success );
   }

   void test_eager()
   {
      const std::string data = "abc";
      memory_input< tracking_mode::eager, eol::lf_crlf, std::string > in( std::string_view( data ), __FUNCTION__ );
      bool success = parse< grammar >( in );
      TAO_PEGTL_TEST_ASSERT( success );
      in.restart();
      success = parse< grammar >( in );
      TAO_PEGTL_TEST_ASSERT( success );
   }

   void unit_test()
   {
      test_lazy();
      test_eager();
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
