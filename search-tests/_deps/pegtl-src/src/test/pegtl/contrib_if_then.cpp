// Copyright (c) 2018-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_rule.hpp"

#include <tao/pegtl/contrib/if_then.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      // clang-format off
      using grammar =
         if_then< one< 'a' >, one< 'b' >, one< 'c' > >::
         else_if_then< one< 'a' >, one< 'b' > >::
         else_then< one< 'c' > >;
      // clang-format on

      verify_rule< grammar >( __LINE__, __FILE__, "abc", result_type::success, 0 );
      verify_rule< grammar >( __LINE__, __FILE__, "abcd", result_type::success, 1 );
      verify_rule< grammar >( __LINE__, __FILE__, "ab", result_type::local_failure, 2 );
      verify_rule< grammar >( __LINE__, __FILE__, "c", result_type::success, 0 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
