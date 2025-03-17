// Copyright (c) 2019-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_rule< rematch< one< 'c' > > >( __LINE__, __FILE__, "c", result_type::success );
      verify_rule< rematch< one< 'c' > > >( __LINE__, __FILE__, "a", result_type::local_failure );
      verify_rule< rematch< one< 'c' > > >( __LINE__, __FILE__, "b", result_type::local_failure );
      verify_rule< rematch< one< 'c' > > >( __LINE__, __FILE__, "cc", result_type::success, 1 );
      verify_rule< rematch< one< 'c' > > >( __LINE__, __FILE__, "bc", result_type::local_failure );

      verify_analyze< rematch< alpha, digit > >( __LINE__, __FILE__, true, false );
      verify_analyze< rematch< opt< alpha >, digit > >( __LINE__, __FILE__, false, false );
      verify_analyze< rematch< alnum, opt< alpha > > >( __LINE__, __FILE__, true, false );
      {
         struct foo
            : rematch< foo, alnum >
         {};
         verify_analyze< foo >( __LINE__, __FILE__, false, true );
      }
      {
         struct foo
            : rematch< alnum, foo >
         {};
         verify_analyze< foo >( __LINE__, __FILE__, true, true );
      }
      {
         struct foo
            : rematch< alnum, alpha, foo >
         {};
         verify_analyze< foo >( __LINE__, __FILE__, true, true );
      }
      verify_rule< rematch< alnum, digit > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< rematch< alnum, digit > >( __LINE__, __FILE__, "1", result_type::success, 0 );
      verify_rule< rematch< alnum, digit > >( __LINE__, __FILE__, "a", result_type::local_failure, 1 );
      verify_rule< rematch< alnum, digit > >( __LINE__, __FILE__, "%", result_type::local_failure, 1 );
      verify_rule< rematch< alnum, digit > >( __LINE__, __FILE__, "1%", result_type::success, 1 );
      verify_rule< rematch< alnum, digit > >( __LINE__, __FILE__, "a%", result_type::local_failure, 2 );
      verify_rule< rematch< alnum, digit > >( __LINE__, __FILE__, "12", result_type::success, 1 );
      verify_rule< rematch< alnum, digit > >( __LINE__, __FILE__, "1c", result_type::success, 1 );

      verify_rule< rematch< alnum, digit, success > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< rematch< alnum, digit, success > >( __LINE__, __FILE__, "1", result_type::success, 0 );
      verify_rule< rematch< alnum, digit, success > >( __LINE__, __FILE__, "a", result_type::local_failure, 1 );
      verify_rule< rematch< alnum, digit, success > >( __LINE__, __FILE__, "%", result_type::local_failure, 1 );
      verify_rule< rematch< alnum, digit, success > >( __LINE__, __FILE__, "1%", result_type::success, 1 );
      verify_rule< rematch< alnum, digit, success > >( __LINE__, __FILE__, "a%", result_type::local_failure, 2 );
      verify_rule< rematch< alnum, digit, success > >( __LINE__, __FILE__, "12", result_type::success, 1 );
      verify_rule< rematch< alnum, digit, success > >( __LINE__, __FILE__, "1c", result_type::success, 1 );

      verify_rule< rematch< alnum, success, digit > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< rematch< alnum, success, digit > >( __LINE__, __FILE__, "1", result_type::success, 0 );
      verify_rule< rematch< alnum, success, digit > >( __LINE__, __FILE__, "a", result_type::local_failure, 1 );
      verify_rule< rematch< alnum, success, digit > >( __LINE__, __FILE__, "%", result_type::local_failure, 1 );
      verify_rule< rematch< alnum, success, digit > >( __LINE__, __FILE__, "1%", result_type::success, 1 );
      verify_rule< rematch< alnum, success, digit > >( __LINE__, __FILE__, "a%", result_type::local_failure, 2 );
      verify_rule< rematch< alnum, success, digit > >( __LINE__, __FILE__, "12", result_type::success, 1 );
      verify_rule< rematch< alnum, success, digit > >( __LINE__, __FILE__, "1c", result_type::success, 1 );

      verify_rule< rematch< plus< alnum >, digit > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< rematch< plus< alnum >, digit > >( __LINE__, __FILE__, "1", result_type::success );
      verify_rule< rematch< plus< alnum >, digit > >( __LINE__, __FILE__, "a", result_type::local_failure );
      verify_rule< rematch< plus< alnum >, digit > >( __LINE__, __FILE__, "%", result_type::local_failure );
      verify_rule< rematch< plus< alnum >, digit > >( __LINE__, __FILE__, "1%", result_type::success, 1 );
      verify_rule< rematch< plus< alnum >, digit > >( __LINE__, __FILE__, "a%", result_type::local_failure );
      verify_rule< rematch< plus< alnum >, digit > >( __LINE__, __FILE__, "12", result_type::success, 0 );
      verify_rule< rematch< plus< alnum >, digit > >( __LINE__, __FILE__, "1c", result_type::success, 0 );
      verify_rule< rematch< plus< alnum >, digit > >( __LINE__, __FILE__, "aa", result_type::local_failure, 2 );
      verify_rule< rematch< plus< alnum >, digit > >( __LINE__, __FILE__, "a1", result_type::local_failure, 2 );
      verify_rule< rematch< plus< alnum >, digit > >( __LINE__, __FILE__, "%%", result_type::local_failure, 2 );

      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "1", result_type::success, 0 );
      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "a", result_type::local_failure, 1 );
      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "%", result_type::local_failure, 1 );
      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "1%", result_type::success, 1 );
      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "a%", result_type::local_failure );
      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "12", result_type::success, 0 );
      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "1c", result_type::success, 0 );
      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "aa", result_type::local_failure, 2 );
      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "a1", result_type::local_failure, 2 );
      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "%%", result_type::local_failure, 2 );
      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "aaa", result_type::local_failure, 3 );
      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "aaa%", result_type::local_failure, 4 );
      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "111", result_type::success, 0 );
      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "111%", result_type::success, 1 );
      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "a1a", result_type::local_failure, 3 );
      verify_rule< rematch< plus< alnum >, plus< digit > > >( __LINE__, __FILE__, "1a1", result_type::success, 0 );

      verify_rule< rematch< plus< alnum >, seq< string< 'f', 'o', 'o' >, eof > > >( __LINE__, __FILE__, "foo", result_type::success, 0 );
      verify_rule< rematch< plus< alnum >, seq< string< 'f', 'o', 'o' >, eof > > >( __LINE__, __FILE__, "foo%", result_type::success, 1 );
      verify_rule< rematch< plus< alnum >, seq< string< 'f', 'o', 'o' >, eof > > >( __LINE__, __FILE__, "foo5", result_type::local_failure, 4 );

      verify_rule< rematch< plus< alnum >, success, seq< string< 'f', 'o', 'o' >, eof > > >( __LINE__, __FILE__, "foo", result_type::success, 0 );
      verify_rule< rematch< plus< alnum >, success, seq< string< 'f', 'o', 'o' >, eof > > >( __LINE__, __FILE__, "foo%", result_type::success, 1 );
      verify_rule< rematch< plus< alnum >, success, seq< string< 'f', 'o', 'o' >, eof > > >( __LINE__, __FILE__, "foo5", result_type::local_failure, 4 );

      verify_rule< rematch< plus< alnum >, seq< string< 'f', 'o', 'o' >, eof >, success > >( __LINE__, __FILE__, "foo", result_type::success, 0 );
      verify_rule< rematch< plus< alnum >, seq< string< 'f', 'o', 'o' >, eof >, success > >( __LINE__, __FILE__, "foo%", result_type::success, 1 );
      verify_rule< rematch< plus< alnum >, seq< string< 'f', 'o', 'o' >, eof >, success > >( __LINE__, __FILE__, "foo5", result_type::local_failure, 4 );

      verify_rule< rematch< plus< alnum >, seq< string< 'f', 'o', 'o' >, eof >, string< 'f', 'o', 'o' > > >( __LINE__, __FILE__, "foo", result_type::success, 0 );
      verify_rule< rematch< plus< alnum >, seq< string< 'f', 'o', 'o' >, eof >, string< 'f', 'o', 'o' > > >( __LINE__, __FILE__, "foo%", result_type::success, 1 );
      verify_rule< rematch< plus< alnum >, seq< string< 'f', 'o', 'o' >, eof >, string< 'f', 'o', 'o' > > >( __LINE__, __FILE__, "foo5", result_type::local_failure, 4 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
