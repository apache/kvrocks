// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_analyze< string<> >( __LINE__, __FILE__, false, false );
      verify_analyze< string< 1 > >( __LINE__, __FILE__, true, false );
      verify_analyze< string< 1, 2 > >( __LINE__, __FILE__, true, false );
      verify_analyze< string< 1, 2, 3, 4 > >( __LINE__, __FILE__, true, false );
      verify_analyze< string< 1, 2, 3, 4, 5, 6, 7 > >( __LINE__, __FILE__, true, false );

      verify_rule< string<> >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "a", result_type::local_failure, 1 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "b", result_type::local_failure, 1 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "c", result_type::local_failure, 1 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "aa", result_type::local_failure, 2 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "aB", result_type::local_failure, 2 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "AB", result_type::local_failure, 2 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "Ab", result_type::local_failure, 2 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "ac", result_type::local_failure, 2 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "ba", result_type::local_failure, 2 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "bb", result_type::local_failure, 2 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "aab", result_type::local_failure, 3 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "aab", result_type::local_failure, 3 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "ab", result_type::success, 0 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "aba", result_type::success, 1 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "abb", result_type::success, 1 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "abc", result_type::success, 1 );
      verify_rule< string< 'a', 'b' > >( __LINE__, __FILE__, "abab", result_type::success, 2 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
