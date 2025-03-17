// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_analyze< rep_opt< 1, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_opt< 6, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_opt< 1, eof > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_opt< 6, eof > >( __LINE__, __FILE__, false, false );

      verify_analyze< rep_opt< 1, any, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_opt< 1, eof, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_opt< 1, any, eof > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_opt< 1, eof, eof > >( __LINE__, __FILE__, false, false );

      verify_rule< rep_opt< 3, one< 'a' > > >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< rep_opt< 3, one< 'a' > > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< rep_opt< 3, one< 'a' > > >( __LINE__, __FILE__, "aa", result_type::success, 0 );
      verify_rule< rep_opt< 3, one< 'a' > > >( __LINE__, __FILE__, "b", result_type::success, 1 );
      verify_rule< rep_opt< 3, one< 'a' > > >( __LINE__, __FILE__, "bb", result_type::success, 2 );
      verify_rule< rep_opt< 3, one< 'a' > > >( __LINE__, __FILE__, "bbb", result_type::success, 3 );
      verify_rule< rep_opt< 3, one< 'a' > > >( __LINE__, __FILE__, "aaa", result_type::success, 0 );
      verify_rule< rep_opt< 3, one< 'a' > > >( __LINE__, __FILE__, "aaaa", result_type::success, 1 );
      verify_rule< rep_opt< 3, one< 'a' > > >( __LINE__, __FILE__, "aaab", result_type::success, 1 );
      verify_rule< rep_opt< 3, one< 'a' > > >( __LINE__, __FILE__, "baaab", result_type::success, 5 );

      verify_rule< rep_opt< 2, two< 'a' > > >( __LINE__, __FILE__, "a", result_type::success, 1 );
      verify_rule< rep_opt< 2, two< 'a' > > >( __LINE__, __FILE__, "aa", result_type::success, 0 );
      verify_rule< rep_opt< 2, two< 'a' > > >( __LINE__, __FILE__, "aaa", result_type::success, 1 );
      verify_rule< rep_opt< 2, two< 'a' > > >( __LINE__, __FILE__, "aaaa", result_type::success, 0 );
      verify_rule< rep_opt< 2, two< 'a' > > >( __LINE__, __FILE__, "aaaaa", result_type::success, 1 );
      verify_rule< rep_opt< 2, two< 'a' > > >( __LINE__, __FILE__, "aaaaaa", result_type::success, 2 );
      verify_rule< rep_opt< 2, two< 'a' > > >( __LINE__, __FILE__, "aaaaaaa", result_type::success, 3 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
