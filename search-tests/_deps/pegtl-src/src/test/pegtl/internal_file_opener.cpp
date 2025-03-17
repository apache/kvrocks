// Copyright (c) 2015-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#if !defined( __cpp_exceptions ) || !defined( _POSIX_MAPPED_FILES )
#include <iostream>
int main()
{
   std::cout << "Exception support disabled, skipping test..." << std::endl;
}
#else

#include <tao/pegtl/file_input.hpp>

#include "test.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      const internal::file_opener fo( "Makefile" );
      ::close( fo.m_fd );  // Provoke exception, nobody would normally do this.
      try {
         (void)fo.size();  // expected to throw

         // LCOV_EXCL_START
         std::cerr << "pegtl: unit test failed for [ internal::file_opener ] " << std::endl;
         ++failed;
         // LCOV_EXCL_STOP
      }
      catch( const std::exception& ) {
      }
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"

#endif
