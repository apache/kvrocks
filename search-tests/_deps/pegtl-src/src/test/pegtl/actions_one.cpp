// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include <string>
#include <utility>
#include <vector>

namespace TAO_PEGTL_NAMESPACE
{
   std::vector< std::pair< std::string, std::string > > applied;

   namespace test1
   {
      struct fiz
         : seq< at< one< 'a' > >, two< 'a' > >
      {};

      struct foo
         : sor< fiz, one< 'b' > >
      {};

      struct bar
         : until< eof, foo >
      {};

      void test_result()
      {
         TAO_PEGTL_TEST_ASSERT( applied.size() == 10 );

         TAO_PEGTL_TEST_ASSERT( applied[ 0 ].first == demangle< one< 'b' > >() );
         TAO_PEGTL_TEST_ASSERT( applied[ 1 ].first == demangle< foo >() );
         TAO_PEGTL_TEST_ASSERT( applied[ 2 ].first == demangle< at< one< 'a' > > >() );
         TAO_PEGTL_TEST_ASSERT( applied[ 3 ].first == demangle< two< 'a' > >() );
         TAO_PEGTL_TEST_ASSERT( applied[ 4 ].first == demangle< fiz >() );
         TAO_PEGTL_TEST_ASSERT( applied[ 5 ].first == demangle< foo >() );
         TAO_PEGTL_TEST_ASSERT( applied[ 6 ].first == demangle< one< 'b' > >() );
         TAO_PEGTL_TEST_ASSERT( applied[ 7 ].first == demangle< foo >() );
         TAO_PEGTL_TEST_ASSERT( applied[ 8 ].first == demangle< eof >() );
         TAO_PEGTL_TEST_ASSERT( applied[ 9 ].first == demangle< bar >() );

         TAO_PEGTL_TEST_ASSERT( applied[ 0 ].second == "b" );
         TAO_PEGTL_TEST_ASSERT( applied[ 1 ].second == "b" );
         TAO_PEGTL_TEST_ASSERT( applied[ 2 ].second.empty() );
         TAO_PEGTL_TEST_ASSERT( applied[ 3 ].second == "aa" );
         TAO_PEGTL_TEST_ASSERT( applied[ 4 ].second == "aa" );
         TAO_PEGTL_TEST_ASSERT( applied[ 5 ].second == "aa" );
         TAO_PEGTL_TEST_ASSERT( applied[ 6 ].second == "b" );
         TAO_PEGTL_TEST_ASSERT( applied[ 7 ].second == "b" );
         TAO_PEGTL_TEST_ASSERT( applied[ 8 ].second.empty() );
         TAO_PEGTL_TEST_ASSERT( applied[ 9 ].second == "baab" );
      }

   }  // namespace test1

   template< typename Rule >
   struct test_action
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in )
      {
         applied.emplace_back( demangle< Rule >(), in.string() );
      }
   };

   void unit_test()
   {
      parse< disable< test1::bar >, test_action >( memory_input( "baab", __FUNCTION__ ) );
      TAO_PEGTL_TEST_ASSERT( applied.size() == 1 );

      TAO_PEGTL_TEST_ASSERT( applied[ 0 ].first == demangle< disable< test1::bar > >() );
      TAO_PEGTL_TEST_ASSERT( applied[ 0 ].second == "baab" );

      applied.clear();

      parse< at< action< test_action, test1::bar > > >( memory_input( "baab", __FUNCTION__ ) );

      TAO_PEGTL_TEST_ASSERT( applied.empty() );

      applied.clear();

      parse< test1::bar, test_action >( memory_input( "baab", __FUNCTION__ ) );

      test1::test_result();

      applied.clear();

      parse< action< test_action, test1::bar > >( memory_input( "baab", __FUNCTION__ ) );

      test1::test_result();

      applied.clear();

      parse< disable< enable< action< test_action, test1::bar > > > >( memory_input( "baab", __FUNCTION__ ) );

      test1::test_result();
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
