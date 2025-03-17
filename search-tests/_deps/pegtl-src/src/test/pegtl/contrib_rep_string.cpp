// Copyright (c) 2020-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include "verify_meta.hpp"
#include "verify_rule.hpp"

#include <tao/pegtl/contrib/rep_string.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_analyze< rep_string< 0 > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_string< 1 > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_string< 0, 'a' > >( __LINE__, __FILE__, false, false );
      verify_analyze< rep_string< 0, 'a', 'b' > >( __LINE__, __FILE__, false, false );

      verify_analyze< rep_string< 1, 'a' > >( __LINE__, __FILE__, true, false );
      verify_analyze< rep_string< 2, 'a', 'b' > >( __LINE__, __FILE__, true, false );
      verify_analyze< rep_string< 3, 'a' > >( __LINE__, __FILE__, true, false );
      verify_analyze< rep_string< 4, 'a', 'b' > >( __LINE__, __FILE__, true, false );

      verify_rule< rep_string< 0 > >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< rep_string< 1 > >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< rep_string< 0, 'a' > >( __LINE__, __FILE__, "", result_type::success, 0 );

      verify_rule< rep_string< 0 > >( __LINE__, __FILE__, "a", result_type::success, 1 );
      verify_rule< rep_string< 1 > >( __LINE__, __FILE__, "a", result_type::success, 1 );
      verify_rule< rep_string< 0, 'a' > >( __LINE__, __FILE__, "a", result_type::success, 1 );

      verify_rule< rep_string< 1, 'a', 'b' > >( __LINE__, __FILE__, "a", result_type::local_failure, 1 );
      verify_rule< rep_string< 1, 'a', 'b' > >( __LINE__, __FILE__, "aa", result_type::local_failure, 2 );
      verify_rule< rep_string< 1, 'a', 'b' > >( __LINE__, __FILE__, "ab", result_type::success, 0 );
      verify_rule< rep_string< 1, 'a', 'b' > >( __LINE__, __FILE__, "abab", result_type::success, 2 );

      verify_rule< rep_string< 2, 'a', 'b' > >( __LINE__, __FILE__, "a", result_type::local_failure, 1 );
      verify_rule< rep_string< 2, 'a', 'b' > >( __LINE__, __FILE__, "aa", result_type::local_failure, 2 );
      verify_rule< rep_string< 2, 'a', 'b' > >( __LINE__, __FILE__, "ab", result_type::local_failure, 2 );
      verify_rule< rep_string< 2, 'a', 'b' > >( __LINE__, __FILE__, "aabb", result_type::local_failure, 4 );
      verify_rule< rep_string< 2, 'a', 'b' > >( __LINE__, __FILE__, "abaa", result_type::local_failure, 4 );
      verify_rule< rep_string< 2, 'a', 'b' > >( __LINE__, __FILE__, "abab", result_type::success, 0 );
      verify_rule< rep_string< 2, 'a', 'b' > >( __LINE__, __FILE__, "ababab", result_type::success, 2 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
