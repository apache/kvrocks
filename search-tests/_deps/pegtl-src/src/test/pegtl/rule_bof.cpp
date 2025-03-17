// Copyright (c) 2017-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_meta< bof, internal::bof >();

      verify_analyze< bof >( __LINE__, __FILE__, false, false );

      verify_rule< bof >( __LINE__, __FILE__, "", result_type::success, 0 );

      for( char i = 1; i < 127; ++i ) {
         const char s[] = { i, 0 };
         verify_rule< bof >( __LINE__, __FILE__, s, result_type::success, 1 );
      }
      verify_rule< seq< alpha, bof > >( __LINE__, __FILE__, "a", result_type::local_failure, 1 );
      verify_rule< seq< alpha, bof > >( __LINE__, __FILE__, "ab", result_type::local_failure, 2 );
      verify_rule< seq< alpha, bof, alpha > >( __LINE__, __FILE__, "ab", result_type::local_failure, 2 );
      verify_rule< seq< alpha, eol, bof > >( __LINE__, __FILE__, "a\n", result_type::local_failure, 2 );
      verify_rule< seq< alpha, eol, bof > >( __LINE__, __FILE__, "a\nb", result_type::local_failure, 3 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
