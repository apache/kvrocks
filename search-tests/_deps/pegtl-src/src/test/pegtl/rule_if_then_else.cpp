// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_ifmt.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_meta< if_then_else< digit, alpha, print >, internal::if_then_else< digit, alpha, print >, digit, alpha, print >();

      verify_ifmt< if_then_else >();
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
