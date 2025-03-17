// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_analyze< list< eof, eof > >( __LINE__, __FILE__, false, true );
      verify_analyze< list< eof, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< list< any, eof > >( __LINE__, __FILE__, true, false );
      verify_analyze< list< any, any > >( __LINE__, __FILE__, true, false );

      verify_analyze< list< eof, eof, eof > >( __LINE__, __FILE__, false, true );
      verify_analyze< list< eof, eof, any > >( __LINE__, __FILE__, false, true );
      verify_analyze< list< eof, any, eof > >( __LINE__, __FILE__, false, true );
      verify_analyze< list< eof, any, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< list< any, eof, eof > >( __LINE__, __FILE__, true, true );
      verify_analyze< list< any, eof, any > >( __LINE__, __FILE__, true, false );
      verify_analyze< list< any, any, eof > >( __LINE__, __FILE__, true, true );
      verify_analyze< list< any, any, any > >( __LINE__, __FILE__, true, false );

      verify_rule< list< one< 'a' >, one< ',' > > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< list< one< 'a' >, one< ',' > > >( __LINE__, __FILE__, "b", result_type::local_failure, 1 );
      verify_rule< list< one< 'a' >, one< ',' > > >( __LINE__, __FILE__, ",", result_type::local_failure, 1 );
      verify_rule< list< one< 'a' >, one< ',' > > >( __LINE__, __FILE__, ",a", result_type::local_failure, 2 );
      verify_rule< list< one< 'a' >, one< ',' > > >( __LINE__, __FILE__, "a,", result_type::success, 1 );
      verify_rule< list< one< 'a' >, one< ',' > > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< list< one< 'a' >, one< ',' > > >( __LINE__, __FILE__, "a,a", result_type::success, 0 );
      verify_rule< list< one< 'a' >, one< ',' > > >( __LINE__, __FILE__, "a,b", result_type::success, 2 );
      verify_rule< list< one< 'a' >, one< ',' > > >( __LINE__, __FILE__, "a,a,a", result_type::success, 0 );
      verify_rule< list< one< 'a' >, one< ',' > > >( __LINE__, __FILE__, "a,a,a,a", result_type::success, 0 );
      verify_rule< list< one< 'a' >, one< ',' > > >( __LINE__, __FILE__, "a,a,a,b", result_type::success, 2 );
      verify_rule< list< one< 'a' >, one< ',' > > >( __LINE__, __FILE__, "a,a,a,,", result_type::success, 2 );

      verify_rule< list< one< 'a' >, one< ',' > > >( __LINE__, __FILE__, "a ", result_type::success, 1 );
      verify_rule< list< one< 'a' >, one< ',' > > >( __LINE__, __FILE__, " a", result_type::local_failure, 2 );
      verify_rule< list< one< 'a' >, one< ',' > > >( __LINE__, __FILE__, "a ,a", result_type::success, 3 );
      verify_rule< list< one< 'a' >, one< ',' > > >( __LINE__, __FILE__, "a, a", result_type::success, 3 );

      verify_rule< list< one< 'a' >, one< ',' >, blank > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< list< one< 'a' >, one< ',' >, blank > >( __LINE__, __FILE__, " ", result_type::local_failure, 1 );
      verify_rule< list< one< 'a' >, one< ',' >, blank > >( __LINE__, __FILE__, ",", result_type::local_failure, 1 );
      verify_rule< list< one< 'a' >, one< ',' >, blank > >( __LINE__, __FILE__, "a ", result_type::success, 1 );
      verify_rule< list< one< 'a' >, one< ',' >, blank > >( __LINE__, __FILE__, " a", result_type::local_failure, 2 );
      verify_rule< list< one< 'a' >, one< ',' >, blank > >( __LINE__, __FILE__, "a ,a", result_type::success, 0 );
      verify_rule< list< one< 'a' >, one< ',' >, blank > >( __LINE__, __FILE__, "a, a", result_type::success, 0 );
      verify_rule< list< one< 'a' >, one< ',' >, blank > >( __LINE__, __FILE__, "a, a,", result_type::success, 1 );
      verify_rule< list< one< 'a' >, one< ',' >, blank > >( __LINE__, __FILE__, "a, a ,", result_type::success, 2 );
      verify_rule< list< one< 'a' >, one< ',' >, blank > >( __LINE__, __FILE__, " a , a ", result_type::local_failure, 7 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
