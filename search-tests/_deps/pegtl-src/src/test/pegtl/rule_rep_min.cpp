// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_analyze< rep_min< 0, eof > >( __LINE__, __FILE__, false, true );
      verify_analyze< rep_min< 1, eof > >( __LINE__, __FILE__, false, true );
      verify_analyze< rep_min< 0, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_min< 1, any > >( __LINE__, __FILE__, true, false );

      verify_rule< rep_min< 3, one< 'a' > > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< rep_min< 3, one< 'a' > > >( __LINE__, __FILE__, "a", result_type::local_failure, 1 );
      verify_rule< rep_min< 3, one< 'a' > > >( __LINE__, __FILE__, "aa", result_type::local_failure, 2 );
      verify_rule< rep_min< 3, one< 'a' > > >( __LINE__, __FILE__, "b", result_type::local_failure, 1 );
      verify_rule< rep_min< 3, one< 'a' > > >( __LINE__, __FILE__, "bb", result_type::local_failure, 2 );
      verify_rule< rep_min< 3, one< 'a' > > >( __LINE__, __FILE__, "bbb", result_type::local_failure, 3 );
      verify_rule< rep_min< 3, one< 'a' > > >( __LINE__, __FILE__, "aaa", result_type::success, 0 );
      verify_rule< rep_min< 3, one< 'a' > > >( __LINE__, __FILE__, "aaaa", result_type::success, 0 );
      verify_rule< rep_min< 3, one< 'a' > > >( __LINE__, __FILE__, "aaab", result_type::success, 1 );
      verify_rule< rep_min< 3, one< 'a' > > >( __LINE__, __FILE__, "baaab", result_type::local_failure, 5 );

      verify_rule< rep_min< 2, two< 'a' > > >( __LINE__, __FILE__, "a", result_type::local_failure, 1 );
      verify_rule< rep_min< 2, two< 'a' > > >( __LINE__, __FILE__, "aa", result_type::local_failure, 2 );
      verify_rule< rep_min< 2, two< 'a' > > >( __LINE__, __FILE__, "aaa", result_type::local_failure, 3 );
      verify_rule< rep_min< 2, two< 'a' > > >( __LINE__, __FILE__, "aaaa", result_type::success, 0 );
      verify_rule< rep_min< 2, two< 'a' > > >( __LINE__, __FILE__, "aaaaa", result_type::success, 1 );
      verify_rule< rep_min< 2, two< 'a' > > >( __LINE__, __FILE__, "aaaaaa", result_type::success, 0 );
      verify_rule< rep_min< 2, two< 'a' > > >( __LINE__, __FILE__, "aaaaaaa", result_type::success, 1 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
