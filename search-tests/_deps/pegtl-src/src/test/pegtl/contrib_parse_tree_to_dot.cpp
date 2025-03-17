// Copyright (c) 2020-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include <tao/pegtl/contrib/parse_tree.hpp>
#include <tao/pegtl/contrib/parse_tree_to_dot.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   // clang-format off
   struct A : one< 'a' > {};
   struct B : one< 'b' > {};
   struct C : one< 'c' > {};

#if defined( __cpp_exceptions )
   struct S : if_must< one< '[' >, until< one< ']' > > > {};
#else
   struct S : seq< one< '[' >, until< one< ']' > > > {};
#endif

   struct D : sor< seq< A, B >, seq< A, C, S > > {};
   // clang-format on

   template< typename Rule >
   struct selector
      : parse_tree::selector< Rule, parse_tree::store_content::on< A, B, C, D, S > >
   {};

   void unit_test()
   {
      memory_input in( "ac[\"\\\x01\x7f\b\n\r\f\t\a\v]", "input" );
      const auto root = parse_tree::parse< D, selector >( in );
      parse_tree::print_dot( std::cout, *root );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
