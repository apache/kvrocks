// Copyright (c) 2020-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

//#include <tao/pegtl/contrib/icu/utf8.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      //      verify_analyze< utf8::icu::alphabetic >( __LINE__, __FILE__, true, false );

      //      verify_rule< utf8::icu::alphabetic >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      //      verify_rule< utf8::icu::alphabetic >( __LINE__, __FILE__, "a", result_type::success );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
