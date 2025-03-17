// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_analyze< pad< eof, eof, eof > >( __LINE__, __FILE__, false, true );
      verify_analyze< pad< eof, eof, any > >( __LINE__, __FILE__, false, true );
      verify_analyze< pad< eof, any, eof > >( __LINE__, __FILE__, false, true );
      verify_analyze< pad< eof, any, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< pad< any, eof, eof > >( __LINE__, __FILE__, true, true );
      verify_analyze< pad< any, eof, any > >( __LINE__, __FILE__, true, true );
      verify_analyze< pad< any, any, eof > >( __LINE__, __FILE__, true, true );
      verify_analyze< pad< any, any, any > >( __LINE__, __FILE__, true, false );

      verify_rule< pad< one< 'a' >, space > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< pad< one< 'a' >, space > >( __LINE__, __FILE__, " ", result_type::local_failure, 1 );
      verify_rule< pad< one< 'a' >, space > >( __LINE__, __FILE__, "  ", result_type::local_failure, 2 );
      verify_rule< pad< one< 'a' >, space > >( __LINE__, __FILE__, "b", result_type::local_failure, 1 );
      verify_rule< pad< one< 'a' >, space > >( __LINE__, __FILE__, "ba", result_type::local_failure, 2 );
      verify_rule< pad< one< 'a' >, space > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< pad< one< 'a' >, space > >( __LINE__, __FILE__, " a", result_type::success, 0 );
      verify_rule< pad< one< 'a' >, space > >( __LINE__, __FILE__, "a ", result_type::success, 0 );
      verify_rule< pad< one< 'a' >, space > >( __LINE__, __FILE__, "  a", result_type::success, 0 );
      verify_rule< pad< one< 'a' >, space > >( __LINE__, __FILE__, "a  ", result_type::success, 0 );
      verify_rule< pad< one< 'a' >, space > >( __LINE__, __FILE__, "  a  ", result_type::success, 0 );
      verify_rule< pad< one< 'a' >, space > >( __LINE__, __FILE__, "   a   ", result_type::success, 0 );
      verify_rule< pad< one< 'a' >, space > >( __LINE__, __FILE__, "aa", result_type::success, 1 );
      verify_rule< pad< one< 'a' >, space > >( __LINE__, __FILE__, "a a", result_type::success, 1 );
      verify_rule< pad< one< 'a' >, space > >( __LINE__, __FILE__, "  a  a ", result_type::success, 2 );

      verify_rule< pad< one< 'a' >, digit, blank > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< pad< one< 'a' >, digit, blank > >( __LINE__, __FILE__, "1a", result_type::success, 0 );
      verify_rule< pad< one< 'a' >, digit, blank > >( __LINE__, __FILE__, "123a", result_type::success, 0 );
      verify_rule< pad< one< 'a' >, digit, blank > >( __LINE__, __FILE__, "a ", result_type::success, 0 );
      verify_rule< pad< one< 'a' >, digit, blank > >( __LINE__, __FILE__, "a   ", result_type::success, 0 );
      verify_rule< pad< one< 'a' >, digit, blank > >( __LINE__, __FILE__, "123a   ", result_type::success, 0 );
      verify_rule< pad< one< 'a' >, digit, blank > >( __LINE__, __FILE__, " a", result_type::local_failure, 2 );
      verify_rule< pad< one< 'a' >, digit, blank > >( __LINE__, __FILE__, "a1", result_type::success, 1 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
