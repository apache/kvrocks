// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include <tao/pegtl/contrib/trace.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   // clang-format off
   struct inner : seq< one< 'a' >, sor< one< 'b' >, one< 'c' >, inner > > {};
   struct outer : seq< one< 'x' >, inner, one< 'y' > > {};

   // how to run a tracer on a *part* of the grammar:
   template< typename > struct partial_action {};
   template<> struct partial_action< inner > : trace_standard {};
   // clang-format on

   void unit_test()
   {
      memory_input in( "xaacy", "trace test please ignore" );
      const auto result = parse< outer, partial_action >( in );
      TAO_PEGTL_TEST_ASSERT( result );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
