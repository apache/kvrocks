// Copyright (c) 2020-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <sstream>

#include "test.hpp"

#include "result_type.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      {
         std::ostringstream oss;
         oss << result_type::success;
         TAO_PEGTL_TEST_ASSERT( oss.str() == "success" );
      }
      {
         std::ostringstream oss;
         oss << result_type::local_failure;
         TAO_PEGTL_TEST_ASSERT( oss.str() == "local_failure" );
      }
      {
         std::ostringstream oss;
         oss << result_type::global_failure;
         TAO_PEGTL_TEST_ASSERT( oss.str() == "global_failure" );
      }
      {
         std::ostringstream oss;
         oss << result_type( 1764 );
         TAO_PEGTL_TEST_ASSERT( oss.str() == "1764" );
      }
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
