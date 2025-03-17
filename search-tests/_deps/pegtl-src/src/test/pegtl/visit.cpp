// Copyright (c) 2020-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <string>
#include <vector>

#include "test.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   using grammar = seq< plus< alpha >, star< sor< space, digit > > >;

   template< typename Name >
   struct visitor
   {
      static void visit( std::vector< std::string >& names )
      {
         names.emplace_back( demangle< Name >() );
      }
   };

   void unit_test()
   {
      std::vector< std::string > names;
      visit< grammar, visitor >( names );
      TAO_PEGTL_TEST_ASSERT( names.size() == 7 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
