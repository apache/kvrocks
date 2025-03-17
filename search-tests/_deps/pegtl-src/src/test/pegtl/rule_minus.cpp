// Copyright (c) 2016-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_analyze< minus< alpha, digit > >( __LINE__, __FILE__, true, false );
      verify_analyze< minus< opt< alpha >, digit > >( __LINE__, __FILE__, false, false );

      verify_rule< minus< alnum, digit > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< minus< alnum, digit > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< minus< alnum, digit > >( __LINE__, __FILE__, "1", result_type::local_failure, 1 );
      verify_rule< minus< alnum, digit > >( __LINE__, __FILE__, "%", result_type::local_failure, 1 );
      verify_rule< minus< alnum, digit > >( __LINE__, __FILE__, "a%", result_type::success, 1 );

#if defined( __cpp_exceptions )
      verify_rule< must< minus< alnum, digit > > >( __LINE__, __FILE__, "%", result_type::global_failure, 1 );
      verify_rule< must< minus< alnum, digit > > >( __LINE__, __FILE__, "1", result_type::global_failure, 0 );
#endif

      verify_rule< minus< plus< alnum >, digit > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< minus< plus< alnum >, digit > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< minus< plus< alnum >, digit > >( __LINE__, __FILE__, "1", result_type::local_failure, 1 );
      verify_rule< minus< plus< alnum >, digit > >( __LINE__, __FILE__, "%", result_type::local_failure, 1 );
      verify_rule< minus< plus< alnum >, digit > >( __LINE__, __FILE__, "a%", result_type::success, 1 );
      verify_rule< minus< plus< alnum >, digit > >( __LINE__, __FILE__, "aa", result_type::success, 0 );
      verify_rule< minus< plus< alnum >, digit > >( __LINE__, __FILE__, "a1", result_type::success, 0 );
      verify_rule< minus< plus< alnum >, digit > >( __LINE__, __FILE__, "1a", result_type::success, 0 );
      verify_rule< minus< plus< alnum >, digit > >( __LINE__, __FILE__, "11", result_type::success, 0 );
      verify_rule< minus< plus< alnum >, digit > >( __LINE__, __FILE__, "%%", result_type::local_failure, 2 );

      verify_rule< minus< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< minus< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< minus< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "1", result_type::local_failure, 1 );
      verify_rule< minus< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "%", result_type::local_failure, 1 );
      verify_rule< minus< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "a%", result_type::success, 1 );
      verify_rule< minus< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "aaa", result_type::success, 0 );
      verify_rule< minus< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "aaa%", result_type::success, 1 );
      verify_rule< minus< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "111", result_type::local_failure, 3 );
      verify_rule< minus< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "111%", result_type::local_failure, 4 );
      verify_rule< minus< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "a1a", result_type::success, 0 );
      verify_rule< minus< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "1a1", result_type::success, 0 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
