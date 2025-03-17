// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_char.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_meta< eof, internal::eof >();

      verify_analyze< eof >( __LINE__, __FILE__, false, false );

      verify_rule< eof >( __LINE__, __FILE__, "", result_type::success, 0 );

      for( char i = 1; i < 127; ++i ) {
         verify_char< eof >( __LINE__, __FILE__, i, result_type::local_failure );
      }
      verify_rule< eof >( __LINE__, __FILE__, "abcdefghijklmn", result_type::local_failure, 14 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
