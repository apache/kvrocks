// Copyright (c) 2018-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_analyze< three< 'a' > >( __LINE__, __FILE__, true, false );

      verify_rule< three< 'a' > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< three< 'a' > >( __LINE__, __FILE__, "a", result_type::local_failure );
      verify_rule< three< 'a' > >( __LINE__, __FILE__, "aa", result_type::local_failure );
      verify_rule< three< 'a' > >( __LINE__, __FILE__, "ab", result_type::local_failure );
      verify_rule< three< 'a' > >( __LINE__, __FILE__, "aab", result_type::local_failure );
      verify_rule< three< 'a' > >( __LINE__, __FILE__, "aaa", result_type::success );
      verify_rule< three< 'a' > >( __LINE__, __FILE__, "aaaa", result_type::success, 1 );
      verify_rule< three< 'a' > >( __LINE__, __FILE__, "aaaaa", result_type::success, 2 );
      verify_rule< three< 'a' > >( __LINE__, __FILE__, "aaaaaa", result_type::success, 3 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
