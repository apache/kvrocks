// Copyright (c) 2020-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <iostream>

#include "test.hpp"

#include <tao/pegtl/contrib/coverage.hpp>
#include <tao/pegtl/contrib/print_coverage.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   [[nodiscard]] inline bool operator==( const coverage_info& l, const coverage_info& r ) noexcept
   {
      return ( l.start == r.start ) && ( l.success == r.success ) && ( l.failure == r.failure ) && ( l.unwind == r.unwind ) && ( l.raise == r.raise );
   }

   template< typename Rule >
   [[nodiscard]] bool equals( const coverage_result& result, const coverage_info& i )
   {
      const coverage_entry m = result.at( demangle< Rule >() );  // Slice
      return i == m;
   }

#if defined( __cpp_exceptions )
   using grammar = seq< sor< try_catch< must< one< 'a' > > >, one< 'F' > >, eof >;

   void unit_test()
   {
      const std::string data = "F";
      coverage_result result;
      memory_input in( data, __FILE__ );
      const bool success = coverage< grammar >( in, result );
      std::cout << result;  // To manually see that printing does the right thing, too.
      TAO_PEGTL_TEST_ASSERT( success );
      TAO_PEGTL_TEST_ASSERT( result.size() == 7 );
      TAO_PEGTL_TEST_ASSERT( equals< grammar >( result, coverage_info{ 1, 1, 0, 0, 0 } ) );
      TAO_PEGTL_TEST_ASSERT( equals< one< 'a' > >( result, coverage_info{ 1, 0, 1, 0, 1 } ) );  // TODO: Should this really be counted as both failure and raise?
      TAO_PEGTL_TEST_ASSERT( equals< one< 'F' > >( result, coverage_info{ 1, 1, 0, 0, 0 } ) );
      TAO_PEGTL_TEST_ASSERT( equals< eof >( result, coverage_info{ 1, 1, 0, 0, 0 } ) );
      TAO_PEGTL_TEST_ASSERT( equals< try_catch< must< one< 'a' > > > >( result, coverage_info{ 1, 0, 1, 0, 0 } ) );
      TAO_PEGTL_TEST_ASSERT( equals< must< one< 'a' > > >( result, coverage_info{ 1, 0, 0, 1, 0 } ) );
      TAO_PEGTL_TEST_ASSERT( equals< sor< try_catch< must< one< 'a' > > >, one< 'F' > > >( result, coverage_info{ 1, 1, 0, 0, 0 } ) );
   }
#else
   using grammar = seq< sor< one< 'a' >, one< 'F' > >, eof >;

   void unit_test()
   {
      const std::string data = "F";
      coverage_result result;
      memory_input in( data, __FILE__ );
      const bool success = coverage< grammar >( in, result );
      std::cout << result;  // To manually see that printing does the right thing, too.
      TAO_PEGTL_TEST_ASSERT( success );
      TAO_PEGTL_TEST_ASSERT( result.size() == 5 );
      TAO_PEGTL_TEST_ASSERT( equals< grammar >( result, coverage_info{ 1, 1, 0, 0, 0 } ) );
      TAO_PEGTL_TEST_ASSERT( equals< one< 'a' > >( result, coverage_info{ 1, 0, 1, 0, 0 } ) );  // TODO: Should this really be counted as both failure and raise?
      TAO_PEGTL_TEST_ASSERT( equals< one< 'F' > >( result, coverage_info{ 1, 1, 0, 0, 0 } ) );
      TAO_PEGTL_TEST_ASSERT( equals< eof >( result, coverage_info{ 1, 1, 0, 0, 0 } ) );
      TAO_PEGTL_TEST_ASSERT( equals< sor< one< 'a' >, one< 'F' > > >( result, coverage_info{ 1, 1, 0, 0, 0 } ) );
   }
#endif
}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
