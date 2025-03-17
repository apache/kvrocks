// Copyright (c) 2018-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#if !defined( __cpp_exceptions )
#include <iostream>
int main()
{
   std::cout << "Exception support disabled, skipping test..." << std::endl;
}
#else

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_analyze< opt_must< any, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< opt_must< eof, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< opt_must< opt< any >, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< opt_must< any, opt< any > > >( __LINE__, __FILE__, false, false );
      verify_analyze< opt_must< any, eof > >( __LINE__, __FILE__, false, false );
      verify_analyze< opt_must< opt< any >, opt< any > > >( __LINE__, __FILE__, false, false );
      verify_analyze< opt_must< eof, eof > >( __LINE__, __FILE__, false, false );

      verify_rule< opt_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "", result_type::success );
      verify_rule< opt_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "a", result_type::global_failure, 0 );
      verify_rule< opt_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "b", result_type::success, 1 );
      verify_rule< opt_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ba", result_type::success, 2 );
      verify_rule< opt_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ab", result_type::success );
      verify_rule< opt_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "aba", result_type::success, 1 );
      verify_rule< opt_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "abb", result_type::success, 1 );
      verify_rule< opt_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "abab", result_type::success, 2 );
      verify_rule< opt_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ac", result_type::global_failure, 1 );
      verify_rule< opt_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "acb", result_type::global_failure, 2 );
      verify_rule< opt_must< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "", result_type::success );
      verify_rule< opt_must< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "b", result_type::success, 1 );
      verify_rule< opt_must< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "bc", result_type::success, 2 );
      verify_rule< opt_must< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "a", result_type::global_failure, 1 );
      verify_rule< opt_must< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "ab", result_type::global_failure, 2 );
      verify_rule< opt_must< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "ac", result_type::global_failure, 2 );
      verify_rule< opt_must< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "abb", result_type::global_failure, 3 );
      verify_rule< opt_must< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "acc", result_type::global_failure, 3 );
      verify_rule< opt_must< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "acb", result_type::global_failure, 3 );
      verify_rule< opt_must< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "abc", result_type::success, 0 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"

#endif
