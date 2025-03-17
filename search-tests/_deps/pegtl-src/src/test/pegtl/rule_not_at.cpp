// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   int at_counter = 0;

   template< typename Rule >
   struct at_action
   {};

   template<>
   struct at_action< alpha >
   {
      static void apply0()
      {
         ++at_counter;
      }
   };

   void unit_test()
   {
      TAO_PEGTL_TEST_ASSERT( at_counter == 0 );

      verify_meta< not_at<>, internal::failure >();
      verify_meta< not_at< eof >, internal::not_at< eof >, eof >();
      verify_meta< not_at< eof, any >, internal::not_at< internal::seq< eof, any > >, internal::seq< eof, any > >();

      verify_analyze< not_at< eof > >( __LINE__, __FILE__, false, false );
      verify_analyze< not_at< any > >( __LINE__, __FILE__, false, false );

      verify_rule< not_at< eof > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< not_at< eof > >( __LINE__, __FILE__, " ", result_type::success, 1 );
      verify_rule< not_at< any > >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< not_at< any > >( __LINE__, __FILE__, "a", result_type::local_failure, 1 );
      verify_rule< not_at< any > >( __LINE__, __FILE__, "aa", result_type::local_failure, 2 );
      verify_rule< not_at< any > >( __LINE__, __FILE__, "aaaa", result_type::local_failure, 4 );

#if defined( __cpp_exceptions )
      verify_rule< must< not_at< alpha > > >( __LINE__, __FILE__, "a", result_type::global_failure, 1 );
      verify_rule< must< not_at< alpha, alpha > > >( __LINE__, __FILE__, "aa1", result_type::global_failure, 3 );
#endif

      {
         memory_input in( "a", 1, __FILE__ );
         parse< alpha, at_action >( in );
         TAO_PEGTL_TEST_ASSERT( at_counter == 1 );
      }
      {
         memory_input in( "1", 1, __FILE__ );
         parse< not_at< alpha >, at_action >( in );
         TAO_PEGTL_TEST_ASSERT( at_counter == 1 );
      }
      {
         memory_input in( "a", 1, __FILE__ );
         parse< not_at< alpha >, at_action >( in );
         TAO_PEGTL_TEST_ASSERT( at_counter == 1 );
      }
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
