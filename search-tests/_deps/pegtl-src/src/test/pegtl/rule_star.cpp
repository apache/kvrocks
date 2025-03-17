// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_meta< star< alpha >, internal::star< alpha >, alpha >();
      verify_meta< star< alpha, digit >, internal::star< internal::seq< alpha, digit > >, internal::seq< alpha, digit > >();

      verify_analyze< star< eof > >( __LINE__, __FILE__, false, true );
      verify_analyze< star< any > >( __LINE__, __FILE__, false, false );
      verify_analyze< star< eof, eof, eof > >( __LINE__, __FILE__, false, true );
      verify_analyze< star< any, eof, any > >( __LINE__, __FILE__, false, false );

      verify_rule< star< one< 'a' > > >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< star< one< 'a' > > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< star< one< 'a' > > >( __LINE__, __FILE__, "aa", result_type::success, 0 );
      verify_rule< star< one< 'a' > > >( __LINE__, __FILE__, "aaa", result_type::success, 0 );
      verify_rule< star< one< 'a' > > >( __LINE__, __FILE__, "ba", result_type::success, 2 );
      verify_rule< star< one< 'a' > > >( __LINE__, __FILE__, "b", result_type::success, 1 );

      verify_rule< star< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< star< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "a", result_type::success, 1 );
      verify_rule< star< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ab", result_type::success, 0 );
      verify_rule< star< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "aba", result_type::success, 1 );
      verify_rule< star< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "abb", result_type::success, 1 );
      verify_rule< star< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "abab", result_type::success, 0 );
      verify_rule< star< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ababc", result_type::success, 1 );
      verify_rule< star< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ababab", result_type::success, 0 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
