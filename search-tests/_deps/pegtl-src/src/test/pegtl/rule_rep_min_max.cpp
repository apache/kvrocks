// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_analyze< rep_min_max< 0, 1, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_min_max< 0, 2, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_min_max< 1, 2, any > >( __LINE__, __FILE__, true, false );
      verify_analyze< rep_min_max< 0, 1, eof > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_min_max< 0, 2, eof > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_min_max< 1, 2, eof > >( __LINE__, __FILE__, false, false );

      verify_analyze< rep_min_max< 0, 1, any, eof > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_min_max< 0, 2, any, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_min_max< 1, 2, eof, any > >( __LINE__, __FILE__, true, false );
      verify_analyze< rep_min_max< 0, 1, eof, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_min_max< 0, 2, eof, eof > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_min_max< 1, 2, eof, eof > >( __LINE__, __FILE__, false, false );

      verify_rule< rep_min_max< 2, 4, one< 'a' > > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< rep_min_max< 2, 4, one< 'a' > > >( __LINE__, __FILE__, "a", result_type::local_failure, 1 );
      verify_rule< rep_min_max< 2, 4, one< 'a' > > >( __LINE__, __FILE__, "aa", result_type::success, 0 );
      verify_rule< rep_min_max< 2, 4, one< 'a' > > >( __LINE__, __FILE__, "aaa", result_type::success, 0 );
      verify_rule< rep_min_max< 2, 4, one< 'a' > > >( __LINE__, __FILE__, "aaaa", result_type::success, 0 );
      verify_rule< rep_min_max< 2, 4, one< 'a' > > >( __LINE__, __FILE__, "aaaaa", result_type::local_failure, 5 );
      verify_rule< rep_min_max< 2, 4, one< 'a' > > >( __LINE__, __FILE__, "b", result_type::local_failure, 1 );
      verify_rule< rep_min_max< 2, 4, one< 'a' > > >( __LINE__, __FILE__, "bb", result_type::local_failure, 2 );
      verify_rule< rep_min_max< 2, 4, one< 'a' > > >( __LINE__, __FILE__, "bbb", result_type::local_failure, 3 );
      verify_rule< rep_min_max< 2, 4, one< 'a' > > >( __LINE__, __FILE__, "bbbb", result_type::local_failure, 4 );
      verify_rule< rep_min_max< 2, 4, one< 'a' > > >( __LINE__, __FILE__, "bbbbb", result_type::local_failure, 5 );
      verify_rule< rep_min_max< 2, 4, one< 'a' > > >( __LINE__, __FILE__, "ba", result_type::local_failure, 2 );
      verify_rule< rep_min_max< 2, 4, one< 'a' > > >( __LINE__, __FILE__, "baa", result_type::local_failure, 3 );
      verify_rule< rep_min_max< 2, 4, one< 'a' > > >( __LINE__, __FILE__, "baaa", result_type::local_failure, 4 );
      verify_rule< rep_min_max< 2, 4, one< 'a' > > >( __LINE__, __FILE__, "baaaa", result_type::local_failure, 5 );

#if defined( __cpp_exceptions )
      verify_rule< must< rep_min_max< 3, 4, one< 'a' > > > >( __LINE__, __FILE__, "aa", result_type::global_failure, 0 );

      verify_rule< try_catch< must< rep_min_max< 3, 4, one< 'a' > > > > >( __LINE__, __FILE__, "aa", result_type::local_failure, 2 );
#endif
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
