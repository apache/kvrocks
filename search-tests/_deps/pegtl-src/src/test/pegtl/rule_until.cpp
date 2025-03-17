// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   struct my_rule
   {
      template< apply_mode A,
                rewind_mode M,
                template< typename... >
                class Action,
                template< typename... >
                class Control,
                typename ParseInput,
                typename... States >
      static bool match( ParseInput& /*unused*/, bool& v, States... /*unused*/ )
      {
         return v;
      }
   };

   template< typename Rule >
   struct my_action
   {};

   template<>
   struct my_action< eof >
   {
      static void apply0( bool& v )
      {
         v = true;
      }
   };

   void unit_test()
   {
      verify_analyze< until< eof > >( __LINE__, __FILE__, false, false );
      verify_analyze< until< any > >( __LINE__, __FILE__, true, false );
      verify_analyze< until< eof, any > >( __LINE__, __FILE__, false, false );
      verify_analyze< until< any, any > >( __LINE__, __FILE__, true, false );

      verify_rule< until< eof > >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< until< any > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< until< one< 'a' > > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< until< one< 'a' > > >( __LINE__, __FILE__, "ba", result_type::success, 0 );
      verify_rule< until< one< 'a' > > >( __LINE__, __FILE__, "bba", result_type::success, 0 );
      verify_rule< until< one< 'a' > > >( __LINE__, __FILE__, "bbbbbbbbbbbbbbba", result_type::success, 0 );
      verify_rule< until< one< 'a' > > >( __LINE__, __FILE__, "ab", result_type::success, 1 );
      verify_rule< until< one< 'a' > > >( __LINE__, __FILE__, "bab", result_type::success, 1 );
      verify_rule< until< one< 'a' > > >( __LINE__, __FILE__, "bbab", result_type::success, 1 );
      verify_rule< until< one< 'a' > > >( __LINE__, __FILE__, "bbbbbbbbbbbbbbbab", result_type::success, 1 );

#if defined( __cpp_exceptions )
      verify_rule< must< until< one< 'a' > > > >( __LINE__, __FILE__, "bbb", result_type::global_failure, 0 );

      verify_rule< try_catch< must< until< one< 'a' > > > > >( __LINE__, __FILE__, "bbb", result_type::local_failure, 3 );
#endif

      verify_rule< until< eof, any > >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< until< any, any > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< until< one< 'a' >, any > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< until< one< 'a' >, any > >( __LINE__, __FILE__, "ba", result_type::success, 0 );
      verify_rule< until< one< 'a' >, any > >( __LINE__, __FILE__, "bba", result_type::success, 0 );
      verify_rule< until< one< 'a' >, any > >( __LINE__, __FILE__, "bbbbbbbbbbbbbbba", result_type::success, 0 );
      verify_rule< until< one< 'a' >, any > >( __LINE__, __FILE__, "ab", result_type::success, 1 );
      verify_rule< until< one< 'a' >, any > >( __LINE__, __FILE__, "bab", result_type::success, 1 );
      verify_rule< until< one< 'a' >, any > >( __LINE__, __FILE__, "bbab", result_type::success, 1 );
      verify_rule< until< one< 'a' >, any > >( __LINE__, __FILE__, "bbbbbbbbbbbbbbbab", result_type::success, 1 );

      verify_rule< until< eof, one< 'a' > > >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< until< eof, one< 'a' > > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< until< eof, one< 'a' > > >( __LINE__, __FILE__, "aa", result_type::success, 0 );
      verify_rule< until< eof, one< 'a' > > >( __LINE__, __FILE__, "aaaaab", result_type::local_failure, 6 );
      verify_rule< until< eof, one< 'a' > > >( __LINE__, __FILE__, "baaaaa", result_type::local_failure, 6 );

      verify_rule< until< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< until< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< until< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "aa", result_type::success, 1 );
      verify_rule< until< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ab", result_type::success, 1 );
      verify_rule< until< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "b", result_type::local_failure, 1 );
      verify_rule< until< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "bb", result_type::local_failure, 2 );
      verify_rule< until< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "ba", result_type::success, 0 );
      verify_rule< until< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "bba", result_type::success, 0 );
      verify_rule< until< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "bbbbbbbbbbbbbba", result_type::success, 0 );
      verify_rule< until< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "baa", result_type::success, 1 );
      verify_rule< until< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "bbaa", result_type::success, 1 );
      verify_rule< until< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "bbbbbbbbbbbbbbaa", result_type::success, 1 );
      verify_rule< until< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "bab", result_type::success, 1 );
      verify_rule< until< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "bbab", result_type::success, 1 );
      verify_rule< until< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "bbbbbbbbbbbbbbab", result_type::success, 1 );

      verify_rule< until< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< until< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< until< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "bca", result_type::success, 0 );
      verify_rule< until< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "bcbca", result_type::success, 0 );
      verify_rule< until< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "bcbcbcbcbca", result_type::success, 0 );
      verify_rule< until< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "babca", result_type::local_failure, 5 );
      verify_rule< until< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "bcbcb", result_type::local_failure, 5 );
      verify_rule< until< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "cbcbc", result_type::local_failure, 5 );
      verify_rule< until< one< 'a' >, one< 'b' >, one< 'c' > > >( __LINE__, __FILE__, "bcbcbc", result_type::local_failure, 6 );

#if defined( __cpp_exceptions )
      verify_rule< must< until< one< 'a' >, one< 'b' > > > >( __LINE__, __FILE__, "bbb", result_type::global_failure, 0 );
      verify_rule< must< until< one< 'a' >, one< 'b' > > > >( __LINE__, __FILE__, "bbbc", result_type::global_failure, 1 );

      verify_rule< try_catch< must< until< one< 'a' >, one< 'b' > > > > >( __LINE__, __FILE__, "bbb", result_type::local_failure, 3 );
      verify_rule< try_catch< must< until< one< 'a' >, one< 'b' > > > > >( __LINE__, __FILE__, "bbbc", result_type::local_failure, 4 );
#endif

      bool success = false;
      const bool result = parse< until< my_rule, eof >, my_action >( memory_input<>( "", __FUNCTION__ ), success );
      TAO_PEGTL_TEST_ASSERT( result );
      TAO_PEGTL_TEST_ASSERT( success );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
