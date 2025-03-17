// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#ifndef TAO_PEGTL_SRC_TEST_PEGTL_VERIFY_SEQS_HPP
#define TAO_PEGTL_SRC_TEST_PEGTL_VERIFY_SEQS_HPP

#include <tao/pegtl.hpp>

#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   template< template< typename... > class S >
   void verify_seqs( const result_type failure = result_type::local_failure )
   {
      verify_analyze< S< any > >( __LINE__, __FILE__, true, false );
      verify_analyze< S< eof > >( __LINE__, __FILE__, false, false );
      verify_analyze< S< any, eof > >( __LINE__, __FILE__, true, false );
      verify_analyze< S< opt< any >, eof > >( __LINE__, __FILE__, false, false );

      verify_rule< S<> >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< S<> >( __LINE__, __FILE__, "a", result_type::success, 1 );

      verify_rule< S< eof > >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< S< eof > >( __LINE__, __FILE__, "a", failure, 1 );
      verify_rule< S< one< 'c' > > >( __LINE__, __FILE__, "", failure, 0 );
      verify_rule< S< one< 'c' >, eof > >( __LINE__, __FILE__, "", failure, 0 );
      verify_rule< S< one< 'c' > > >( __LINE__, __FILE__, "c", result_type::success, 0 );
      verify_rule< S< one< 'c' > > >( __LINE__, __FILE__, "a", failure, 1 );
      verify_rule< S< one< 'c' > > >( __LINE__, __FILE__, "b", failure, 1 );
      verify_rule< S< one< 'c' > > >( __LINE__, __FILE__, "cc", result_type::success, 1 );
      verify_rule< S< one< 'c' > > >( __LINE__, __FILE__, "bc", failure, 2 );
      verify_rule< S< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "", failure, 0 );
      verify_rule< S< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "a", failure, 1 );
      verify_rule< S< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "b", failure, 1 );
      verify_rule< S< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "c", failure, 1 );
      verify_rule< S< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ab", result_type::success, 0 );
      verify_rule< S< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "aba", result_type::success, 1 );
      verify_rule< S< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "abb", result_type::success, 1 );
      verify_rule< S< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "abc", result_type::success, 1 );
      verify_rule< S< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "abab", result_type::success, 2 );
      verify_rule< S< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "", failure, 0 );
      verify_rule< S< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "a", failure, 1 );
      verify_rule< S< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "ab", failure, 2 );
      verify_rule< S< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "abc", result_type::success, 0 );
      verify_rule< S< one< 'a' >, one< 'b' >, one< 'c' >, eof > >( __LINE__, __FILE__, "abc", result_type::success, 0 );
      verify_rule< S< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "abcd", result_type::success, 1 );

#if defined( __cpp_exceptions )
      verify_rule< must< S< one< 'a' >, one< 'b' > > > >( __LINE__, __FILE__, "", result_type::global_failure, 0 );
      verify_rule< must< S< one< 'a' >, one< 'b' > > > >( __LINE__, __FILE__, "a", result_type::global_failure, 0 );
      verify_rule< must< S< one< 'a' >, one< 'b' > > > >( __LINE__, __FILE__, "b", result_type::global_failure, 1 );
      verify_rule< must< S< one< 'a' >, one< 'b' > > > >( __LINE__, __FILE__, "c", result_type::global_failure, 1 );
      verify_rule< must< S< one< 'a' >, one< 'b' > > > >( __LINE__, __FILE__, "ab", result_type::success, 0 );
      verify_rule< must< S< one< 'a' >, one< 'b' > > > >( __LINE__, __FILE__, "aba", result_type::success, 1 );

      verify_rule< try_catch< must< S< one< 'a' >, one< 'b' > > > > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< try_catch< must< S< one< 'a' >, one< 'b' > > > > >( __LINE__, __FILE__, "a", result_type::local_failure, 1 );
      verify_rule< try_catch< must< S< one< 'a' >, one< 'b' > > > > >( __LINE__, __FILE__, "b", result_type::local_failure, 1 );
      verify_rule< try_catch< must< S< one< 'a' >, one< 'b' > > > > >( __LINE__, __FILE__, "c", result_type::local_failure, 1 );
      verify_rule< try_catch< must< S< one< 'a' >, one< 'b' > > > > >( __LINE__, __FILE__, "ab", result_type::success, 0 );
      verify_rule< try_catch< must< S< one< 'a' >, one< 'b' > > > > >( __LINE__, __FILE__, "aba", result_type::success, 1 );
#endif
   }

}  // namespace TAO_PEGTL_NAMESPACE

#endif
