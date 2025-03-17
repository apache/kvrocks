// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_file.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   template< tracking_mode P = tracking_mode::eager, typename Eol = eol::lf_crlf >
   struct open_input
      : public read_input< P, Eol >
   {
      explicit open_input( const internal::filesystem::path& path )
         : read_input< P, Eol >( internal::file_open( path ), path )
      {}
   };

   void unit_test()
   {
      verify_file< read_input<> >();
      verify_file< open_input<> >();
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
