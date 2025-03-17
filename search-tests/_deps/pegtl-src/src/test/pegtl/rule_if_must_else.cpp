// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#if !defined( __cpp_exceptions )
#include <iostream>
int main()
{
   std::cout << "Exception support disabled, skipping test..." << std::endl;
}
#else

#include "test.hpp"
#include "verify_ifmt.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_ifmt< if_must_else >( result_type::global_failure );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"

#endif
