// Copyright (c) 2020-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#if !defined( __cpp_exceptions )
#include <iostream>
int main()
{
   std::cout << "Exception support disabled, skipping test..." << std::endl;
}
#else

#include "test.hpp"

namespace test1
{
   using namespace TAO_PEGTL_NAMESPACE;

   // clang-format off
   struct a : one< 'a' > {};
   struct b : one< 'b' > {};
   struct grammar : sor< a, b > {};

   template< typename > inline constexpr const char* error_message = nullptr;
   template<> inline constexpr auto error_message< test1::b > = "test123";
   // clang-format on

   struct error
   {
      template< typename Rule >
      static constexpr auto message = error_message< Rule >;
   };

   template< typename Rule >
   using control = must_if< error >::control< Rule >;

}  // namespace test1

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      try {
         parse< test1::grammar, nothing, test1::control >( memory_input( "c", __FUNCTION__ ) );
         TAO_PEGTL_TEST_UNREACHABLE;  // LCOV_EXCL_LINE
      }
      catch( const parse_error& e ) {
         TAO_PEGTL_TEST_ASSERT( e.message() == "test123" );
      }
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"

#endif
