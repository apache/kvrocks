// Copyright (c) 2020-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <iostream>

#include "test.hpp"

#include <tao/pegtl/contrib/print.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   using grammar = seq< alpha, digit >;

   void unit_test()
   {
      // Just enough to see that it compiles and nothing explodes;
      // the output format probabaly changes between compilers and
      // versions making a proper test difficult.
      print_names< grammar >( std::cout );
      print_debug< grammar >( std::cout );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
