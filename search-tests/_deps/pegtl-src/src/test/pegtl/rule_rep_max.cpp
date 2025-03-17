// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_analyze< rep_max< 1, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_max< 2, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_max< 1, eof > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_max< 2, eof > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_max< 1, any, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_max< 2, any, any > >( __LINE__, __FILE__, false, false );

      verify_rule< rep_max< 3, one< 'a' > > >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< rep_max< 3, one< 'a' > > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< rep_max< 3, one< 'a' > > >( __LINE__, __FILE__, "aa", result_type::success, 0 );
      verify_rule< rep_max< 3, one< 'a' > > >( __LINE__, __FILE__, "b", result_type::success, 1 );
      verify_rule< rep_max< 3, one< 'a' > > >( __LINE__, __FILE__, "bb", result_type::success, 2 );
      verify_rule< rep_max< 3, one< 'a' > > >( __LINE__, __FILE__, "bbb", result_type::success, 3 );
      verify_rule< rep_max< 3, one< 'a' > > >( __LINE__, __FILE__, "aaa", result_type::success, 0 );
      verify_rule< rep_max< 3, one< 'a' > > >( __LINE__, __FILE__, "aaaa", result_type::local_failure, 4 );
      verify_rule< rep_max< 3, one< 'a' > > >( __LINE__, __FILE__, "aaab", result_type::success, 1 );
      verify_rule< rep_max< 3, one< 'a' > > >( __LINE__, __FILE__, "baaab", result_type::success, 5 );

      verify_rule< rep_max< 2, one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< rep_max< 2, one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "a", result_type::success, 1 );
      verify_rule< rep_max< 2, one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "aa", result_type::success, 2 );
      verify_rule< rep_max< 2, one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ba", result_type::success, 2 );
      verify_rule< rep_max< 2, one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ab", result_type::success, 0 );
      verify_rule< rep_max< 2, one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "aba", result_type::success, 1 );
      verify_rule< rep_max< 2, one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "abb", result_type::success, 1 );
      verify_rule< rep_max< 2, one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "aab", result_type::success, 3 );
      verify_rule< rep_max< 2, one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "abab", result_type::success, 0 );
      verify_rule< rep_max< 2, one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ababb", result_type::success, 1 );
      verify_rule< rep_max< 2, one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ababa", result_type::success, 1 );
      verify_rule< rep_max< 2, one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ababab", result_type::local_failure, 6 );

      verify_rule< rep_max< 2, two< 'a' > > >( __LINE__, __FILE__, "a", result_type::success, 1 );
      verify_rule< rep_max< 2, two< 'a' > > >( __LINE__, __FILE__, "aa", result_type::success, 0 );
      verify_rule< rep_max< 2, two< 'a' > > >( __LINE__, __FILE__, "aaa", result_type::success, 1 );
      verify_rule< rep_max< 2, two< 'a' > > >( __LINE__, __FILE__, "aaaa", result_type::success, 0 );
      verify_rule< rep_max< 2, two< 'a' > > >( __LINE__, __FILE__, "aaaaa", result_type::success, 1 );
      verify_rule< rep_max< 2, two< 'a' > > >( __LINE__, __FILE__, "aaaaaa", result_type::local_failure, 6 );
      verify_rule< rep_max< 2, two< 'a' > > >( __LINE__, __FILE__, "aaaaaaa", result_type::local_failure, 7 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
