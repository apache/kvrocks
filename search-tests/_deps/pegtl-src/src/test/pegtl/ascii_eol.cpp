// Copyright (c) 2016-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_char.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_analyze< eol >( __LINE__, __FILE__, true, false );

      verify_rule< eol >( __LINE__, __FILE__, "", result_type::local_failure, 0 );

      for( char i = 1; i < 127; ++i ) {
         verify_char< eol >( __LINE__, __FILE__, i, ( i == '\n' ) ? result_type::success : result_type::local_failure );
      }
      verify_rule< eol, eol::lf >( __LINE__, __FILE__, " ", result_type::local_failure, 1 );
      verify_rule< eol, eol::lf >( __LINE__, __FILE__, "\r", result_type::local_failure, 1 );
      verify_rule< eol, eol::lf >( __LINE__, __FILE__, "\n", result_type::success, 0 );
      verify_rule< eol, eol::lf >( __LINE__, __FILE__, "\r\n", result_type::local_failure, 2 );
      verify_rule< eol, eol::lf >( __LINE__, __FILE__, "\n\r", result_type::success, 1 );
      verify_rule< eol, eol::lf >( __LINE__, __FILE__, "\n\r\n", result_type::success, 2 );
      verify_rule< eol, eol::lf >( __LINE__, __FILE__, "\n\r\r", result_type::success, 2 );
      verify_rule< eol, eol::lf >( __LINE__, __FILE__, "\na", result_type::success, 1 );
      verify_rule< eol, eol::lf >( __LINE__, __FILE__, "\ra", result_type::local_failure, 2 );
      verify_rule< eol, eol::lf >( __LINE__, __FILE__, "\r\na", result_type::local_failure, 3 );
      verify_rule< eol, eol::lf >( __LINE__, __FILE__, "\r\n\r", result_type::local_failure, 3 );
      verify_rule< eol, eol::lf >( __LINE__, __FILE__, "\r\n\n", result_type::local_failure, 3 );

      verify_rule< eol, eol::cr >( __LINE__, __FILE__, " ", result_type::local_failure, 1 );
      verify_rule< eol, eol::cr >( __LINE__, __FILE__, "\r", result_type::success, 0 );
      verify_rule< eol, eol::cr >( __LINE__, __FILE__, "\n", result_type::local_failure, 1 );
      verify_rule< eol, eol::cr >( __LINE__, __FILE__, "\r\n", result_type::success, 1 );
      verify_rule< eol, eol::cr >( __LINE__, __FILE__, "\n\r", result_type::local_failure, 2 );
      verify_rule< eol, eol::cr >( __LINE__, __FILE__, "\n\r\n", result_type::local_failure, 3 );
      verify_rule< eol, eol::cr >( __LINE__, __FILE__, "\n\r\r", result_type::local_failure, 3 );
      verify_rule< eol, eol::cr >( __LINE__, __FILE__, "\na", result_type::local_failure, 2 );
      verify_rule< eol, eol::cr >( __LINE__, __FILE__, "\ra", result_type::success, 1 );
      verify_rule< eol, eol::cr >( __LINE__, __FILE__, "\r\na", result_type::success, 2 );
      verify_rule< eol, eol::cr >( __LINE__, __FILE__, "\r\n\r", result_type::success, 2 );
      verify_rule< eol, eol::cr >( __LINE__, __FILE__, "\r\n\n", result_type::success, 2 );

      verify_rule< eol, eol::crlf >( __LINE__, __FILE__, " ", result_type::local_failure, 1 );
      verify_rule< eol, eol::crlf >( __LINE__, __FILE__, "\r", result_type::local_failure, 1 );
      verify_rule< eol, eol::crlf >( __LINE__, __FILE__, "\n", result_type::local_failure, 1 );
      verify_rule< eol, eol::crlf >( __LINE__, __FILE__, "\r\n", result_type::success, 0 );
      verify_rule< eol, eol::crlf >( __LINE__, __FILE__, "\n\r", result_type::local_failure, 2 );
      verify_rule< eol, eol::crlf >( __LINE__, __FILE__, "\n\r\n", result_type::local_failure, 3 );
      verify_rule< eol, eol::crlf >( __LINE__, __FILE__, "\n\r\r", result_type::local_failure, 3 );
      verify_rule< eol, eol::crlf >( __LINE__, __FILE__, "\na", result_type::local_failure, 2 );
      verify_rule< eol, eol::crlf >( __LINE__, __FILE__, "\ra", result_type::local_failure, 2 );
      verify_rule< eol, eol::crlf >( __LINE__, __FILE__, "\r\na", result_type::success, 1 );
      verify_rule< eol, eol::crlf >( __LINE__, __FILE__, "\r\n\r", result_type::success, 1 );
      verify_rule< eol, eol::crlf >( __LINE__, __FILE__, "\r\n\n", result_type::success, 1 );

      verify_rule< eol, eol::lf_crlf >( __LINE__, __FILE__, " ", result_type::local_failure, 1 );
      verify_rule< eol, eol::lf_crlf >( __LINE__, __FILE__, "\r", result_type::local_failure, 1 );
      verify_rule< eol, eol::lf_crlf >( __LINE__, __FILE__, "\n", result_type::success, 0 );
      verify_rule< eol, eol::lf_crlf >( __LINE__, __FILE__, "\r\n", result_type::success, 0 );
      verify_rule< eol, eol::lf_crlf >( __LINE__, __FILE__, "\n\r", result_type::success, 1 );
      verify_rule< eol, eol::lf_crlf >( __LINE__, __FILE__, "\n\r\n", result_type::success, 2 );
      verify_rule< eol, eol::lf_crlf >( __LINE__, __FILE__, "\n\r\r", result_type::success, 2 );
      verify_rule< eol, eol::lf_crlf >( __LINE__, __FILE__, "\na", result_type::success, 1 );
      verify_rule< eol, eol::lf_crlf >( __LINE__, __FILE__, "\ra", result_type::local_failure, 2 );
      verify_rule< eol, eol::lf_crlf >( __LINE__, __FILE__, "\r\na", result_type::success, 1 );
      verify_rule< eol, eol::lf_crlf >( __LINE__, __FILE__, "\r\n\r", result_type::success, 1 );
      verify_rule< eol, eol::lf_crlf >( __LINE__, __FILE__, "\r\n\n", result_type::success, 1 );

      verify_rule< eol, eol::cr_crlf >( __LINE__, __FILE__, " ", result_type::local_failure, 1 );
      verify_rule< eol, eol::cr_crlf >( __LINE__, __FILE__, "\r", result_type::success, 0 );
      verify_rule< eol, eol::cr_crlf >( __LINE__, __FILE__, "\n", result_type::local_failure, 1 );
      verify_rule< eol, eol::cr_crlf >( __LINE__, __FILE__, "\r\n", result_type::success, 0 );
      verify_rule< eol, eol::cr_crlf >( __LINE__, __FILE__, "\n\r", result_type::local_failure, 2 );
      verify_rule< eol, eol::cr_crlf >( __LINE__, __FILE__, "\n\r\n", result_type::local_failure, 3 );
      verify_rule< eol, eol::cr_crlf >( __LINE__, __FILE__, "\n\r\r", result_type::local_failure, 3 );
      verify_rule< eol, eol::cr_crlf >( __LINE__, __FILE__, "\na", result_type::local_failure, 2 );
      verify_rule< eol, eol::cr_crlf >( __LINE__, __FILE__, "\ra", result_type::success, 1 );
      verify_rule< eol, eol::cr_crlf >( __LINE__, __FILE__, "\r\na", result_type::success, 1 );
      verify_rule< eol, eol::cr_crlf >( __LINE__, __FILE__, "\r\n\r", result_type::success, 1 );
      verify_rule< eol, eol::cr_crlf >( __LINE__, __FILE__, "\r\n\n", result_type::success, 1 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
