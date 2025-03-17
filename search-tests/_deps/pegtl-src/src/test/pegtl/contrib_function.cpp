// Copyright (c) 2020-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include <tao/pegtl/contrib/function.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   bool call1 = false;

   [[nodiscard]] bool func1( memory_input<>& /*unused*/, int /*unused*/, char*& /*unused*/, const double& /*unused*/ )
   {
      call1 = true;
      return true;
   }

   struct rule1 : TAO_PEGTL_NAMESPACE::function< func1 >
   {};

   void unit_test()
   {
      int i = 42;
      char c = 'a';
      double d = 42.0;
      memory_input in( "foo", __FUNCTION__ );
      TAO_PEGTL_TEST_ASSERT( parse< rule1 >( in, i, &c, d ) );
      TAO_PEGTL_TEST_ASSERT( call1 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
