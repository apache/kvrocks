// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include "verify_meta.hpp"
#include "verify_seqs.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_meta< seq<>, internal::success >();
      verify_meta< seq< alpha >, internal::seq< alpha >, alpha >();
      verify_meta< seq< alpha, digit >, internal::seq< alpha, digit >, alpha, digit >();

      verify_seqs< seq >();
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
