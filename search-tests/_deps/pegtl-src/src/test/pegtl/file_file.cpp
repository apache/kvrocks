// Copyright (c) 2015-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_file.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_file< file_input<> >();
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
