// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
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
      verify_analyze< star_must< eof > >( __LINE__, __FILE__, false, true );
      verify_analyze< star_must< any > >( __LINE__, __FILE__, false, false );
      verify_analyze< star_must< eof, eof, eof > >( __LINE__, __FILE__, false, true );
      verify_analyze< star_must< any, eof, any > >( __LINE__, __FILE__, false, false );

      verify_rule< star_must< one< 'a' > > >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< star_must< one< 'a' > > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< star_must< one< 'a' > > >( __LINE__, __FILE__, "aa", result_type::success, 0 );
      verify_rule< star_must< one< 'a' > > >( __LINE__, __FILE__, "aaa", result_type::success, 0 );
      verify_rule< star_must< one< 'a' > > >( __LINE__, __FILE__, "ba", result_type::success, 2 );
      verify_rule< star_must< one< 'a' > > >( __LINE__, __FILE__, "b", result_type::success, 1 );

      verify_rule< star_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< star_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "a", result_type::global_failure, 1 );
      verify_rule< star_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ab", result_type::success, 0 );
      verify_rule< star_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "aba", result_type::global_failure, 3 );
      verify_rule< star_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "abb", result_type::success, 1 );
      verify_rule< star_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "abab", result_type::success, 0 );
      verify_rule< star_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ababc", result_type::success, 1 );
      verify_rule< star_must< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ababab", result_type::success, 0 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"

#endif
