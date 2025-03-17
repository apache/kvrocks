// Copyright (c) 2018-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_char.hpp"
#include "verify_rule.hpp"

#include <tao/pegtl/contrib/uint8.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_rule< uint8::any >( __LINE__, __FILE__, "", result_type::local_failure, 0 );

      for( int i = -100; i < 200; ++i ) {
         verify_char< uint8::any >( __LINE__, __FILE__, char( i ), true );
      }
      verify_rule< uint8::mask_not_one< 0xff, 0x01, 0x02 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::mask_not_one< 0xff, 0x01, 0x02 > >( __LINE__, __FILE__, "\x03", result_type::success );
      verify_rule< uint8::mask_not_one< 0xff, 0x01, 0x02 > >( __LINE__, __FILE__, "\x03\x01", result_type::success, 1 );
      verify_rule< uint8::mask_not_one< 0xff, 0x01, 0x10 > >( __LINE__, __FILE__, "\x01", result_type::local_failure );
      verify_rule< uint8::mask_not_one< 0xff, 0x01, 0x10 > >( __LINE__, __FILE__, "\x01\x01", result_type::local_failure );

      verify_rule< uint8::mask_not_one< 0xf0, 0x01, 0x02 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::mask_not_one< 0xf0, 0x01, 0x02 > >( __LINE__, __FILE__, "\x03", result_type::success );
      verify_rule< uint8::mask_not_one< 0xf0, 0x01, 0x02 > >( __LINE__, __FILE__, "\x03\x01", result_type::success, 1 );
      verify_rule< uint8::mask_not_one< 0xf0, 0x01, 0x10 > >( __LINE__, __FILE__, "\x01", result_type::success );
      verify_rule< uint8::mask_not_one< 0xf0, 0x01, 0x10 > >( __LINE__, __FILE__, "\x01\x01", result_type::success, 1 );
      verify_rule< uint8::mask_not_one< 0xf0, 0x01, 0x10 > >( __LINE__, __FILE__, "\x31", result_type::success );
      verify_rule< uint8::mask_not_one< 0xf0, 0x01, 0x10 > >( __LINE__, __FILE__, "\x31\x01", result_type::success, 1 );
      verify_rule< uint8::mask_not_one< 0xf0, 0x01, 0x10 > >( __LINE__, __FILE__, "\x11", result_type::local_failure );
      verify_rule< uint8::mask_not_one< 0xf0, 0x01, 0x10 > >( __LINE__, __FILE__, "\x11\x01", result_type::local_failure );

      verify_rule< uint8::mask_not_range< 0xff, 0x10, 0x2f > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::mask_not_range< 0xff, 0x10, 0x2f > >( __LINE__, __FILE__, "\x0f", result_type::success );
      verify_rule< uint8::mask_not_range< 0xff, 0x10, 0x2f > >( __LINE__, __FILE__, "\x0f\x0f", result_type::success, 1 );
      verify_rule< uint8::mask_not_range< 0xff, 0x10, 0x2f > >( __LINE__, __FILE__, "\x30", result_type::success );
      verify_rule< uint8::mask_not_range< 0xff, 0x10, 0x2f > >( __LINE__, __FILE__, "\x30\x30", result_type::success, 1 );
      verify_rule< uint8::mask_not_range< 0xff, 0x10, 0x2f > >( __LINE__, __FILE__, "\x10", result_type::local_failure );
      verify_rule< uint8::mask_not_range< 0xff, 0x10, 0x2f > >( __LINE__, __FILE__, "\x10\x10", result_type::local_failure );
      verify_rule< uint8::mask_not_range< 0xff, 0x10, 0x2f > >( __LINE__, __FILE__, "\x2f", result_type::local_failure );
      verify_rule< uint8::mask_not_range< 0xff, 0x10, 0x2f > >( __LINE__, __FILE__, "\x2f\x2f", result_type::local_failure );

      verify_rule< uint8::mask_not_range< 0xf0, 0x10, 0x30 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::mask_not_range< 0xf0, 0x10, 0x30 > >( __LINE__, __FILE__, "\x0f", result_type::success );
      verify_rule< uint8::mask_not_range< 0xf0, 0x10, 0x30 > >( __LINE__, __FILE__, "\x0f\x0f", result_type::success, 1 );
      verify_rule< uint8::mask_not_range< 0xf0, 0x10, 0x30 > >( __LINE__, __FILE__, "\x40", result_type::success );
      verify_rule< uint8::mask_not_range< 0xf0, 0x10, 0x30 > >( __LINE__, __FILE__, "\x40\x40", result_type::success, 1 );
      verify_rule< uint8::mask_not_range< 0xf0, 0x10, 0x30 > >( __LINE__, __FILE__, "\x30", result_type::local_failure );
      verify_rule< uint8::mask_not_range< 0xf0, 0x10, 0x30 > >( __LINE__, __FILE__, "\x30\x30", result_type::local_failure );
      verify_rule< uint8::mask_not_range< 0xf0, 0x10, 0x30 > >( __LINE__, __FILE__, "\x31", result_type::local_failure );
      verify_rule< uint8::mask_not_range< 0xf0, 0x10, 0x30 > >( __LINE__, __FILE__, "\x31\x31", result_type::local_failure );

      verify_rule< uint8::mask_one< 0xff, 0x10, 0x40 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::mask_one< 0xff, 0x10, 0x40 > >( __LINE__, __FILE__, "\x10", result_type::success );
      verify_rule< uint8::mask_one< 0xff, 0x10, 0x40 > >( __LINE__, __FILE__, "\x10\x10", result_type::success, 1 );
      verify_rule< uint8::mask_one< 0xff, 0x10, 0x40 > >( __LINE__, __FILE__, "\x40", result_type::success );
      verify_rule< uint8::mask_one< 0xff, 0x10, 0x40 > >( __LINE__, __FILE__, "\x40\x40", result_type::success, 1 );
      verify_rule< uint8::mask_one< 0xff, 0x10, 0x40 > >( __LINE__, __FILE__, "\x20", result_type::local_failure );
      verify_rule< uint8::mask_one< 0xff, 0x10, 0x40 > >( __LINE__, __FILE__, "\x20\x20", result_type::local_failure );
      verify_rule< uint8::mask_one< 0xff, 0x10, 0x40 > >( __LINE__, __FILE__, "\x11", result_type::local_failure );
      verify_rule< uint8::mask_one< 0xff, 0x10, 0x40 > >( __LINE__, __FILE__, "\x11\x10", result_type::local_failure );
      verify_rule< uint8::mask_one< 0xff, 0x10, 0x40 > >( __LINE__, __FILE__, "\x3f", result_type::local_failure );
      verify_rule< uint8::mask_one< 0xff, 0x10, 0x40 > >( __LINE__, __FILE__, "\x3f\x10", result_type::local_failure );

      verify_rule< uint8::mask_one< 0x1f, 0x10, 0x14 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::mask_one< 0x1f, 0x10, 0x14 > >( __LINE__, __FILE__, "\x10", result_type::success );
      verify_rule< uint8::mask_one< 0x1f, 0x10, 0x14 > >( __LINE__, __FILE__, "\x10\x10", result_type::success, 1 );
      verify_rule< uint8::mask_one< 0x1f, 0x10, 0x14 > >( __LINE__, __FILE__, "\xf0", result_type::success );
      verify_rule< uint8::mask_one< 0x1f, 0x10, 0x14 > >( __LINE__, __FILE__, "\xf0\x10", result_type::success, 1 );
      verify_rule< uint8::mask_one< 0x1f, 0x10, 0x14 > >( __LINE__, __FILE__, "\x54", result_type::success );
      verify_rule< uint8::mask_one< 0x1f, 0x10, 0x14 > >( __LINE__, __FILE__, "\x54\x54", result_type::success, 1 );
      verify_rule< uint8::mask_one< 0x1f, 0x10, 0x40 > >( __LINE__, __FILE__, "\x11", result_type::local_failure );
      verify_rule< uint8::mask_one< 0x1f, 0x10, 0x40 > >( __LINE__, __FILE__, "\x11\x10", result_type::local_failure );
      verify_rule< uint8::mask_one< 0x1f, 0x10, 0x40 > >( __LINE__, __FILE__, "\x3f", result_type::local_failure );
      verify_rule< uint8::mask_one< 0x1f, 0x10, 0x40 > >( __LINE__, __FILE__, "\x3f\x10", result_type::local_failure );

      verify_rule< uint8::mask_range< 0xff, 0x17, 0x27 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xff, 0x17, 0x27 > >( __LINE__, __FILE__, "\x01", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xff, 0x17, 0x27 > >( __LINE__, __FILE__, "\x16", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xff, 0x17, 0x27 > >( __LINE__, __FILE__, "\x16\x17", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xff, 0x17, 0x27 > >( __LINE__, __FILE__, "\x17", result_type::success );
      verify_rule< uint8::mask_range< 0xff, 0x17, 0x27 > >( __LINE__, __FILE__, "\x17\x17", result_type::success, 1 );
      verify_rule< uint8::mask_range< 0xff, 0x17, 0x27 > >( __LINE__, __FILE__, "\x27", result_type::success );
      verify_rule< uint8::mask_range< 0xff, 0x17, 0x27 > >( __LINE__, __FILE__, "\x27\x17", result_type::success, 1 );
      verify_rule< uint8::mask_range< 0xff, 0x17, 0x27 > >( __LINE__, __FILE__, "\x28", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xff, 0x17, 0x27 > >( __LINE__, __FILE__, "\x28\x17", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xff, 0x17, 0x27 > >( __LINE__, __FILE__, "\x7f", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xff, 0x17, 0x27 > >( __LINE__, __FILE__, "\xff", result_type::local_failure );

      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x01", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x91", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x16", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x16\x17", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x96", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x96\x17", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x17", result_type::success );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x17\x17", result_type::success, 1 );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x57", result_type::success );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x57\x17", result_type::success, 1 );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x27", result_type::success );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x27\x17", result_type::success, 1 );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x67", result_type::success );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x67\x17", result_type::success, 1 );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x28", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x28\x17", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x68", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x68\x17", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\x7f", result_type::local_failure );
      verify_rule< uint8::mask_range< 0xbf, 0x17, 0x27 > >( __LINE__, __FILE__, "\xff", result_type::local_failure );

      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47 > >( __LINE__, __FILE__, "\x01", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47 > >( __LINE__, __FILE__, "\x0f", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47 > >( __LINE__, __FILE__, "\x18", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47 > >( __LINE__, __FILE__, "\x3f", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47 > >( __LINE__, __FILE__, "\x48", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47 > >( __LINE__, __FILE__, "\x94", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47 > >( __LINE__, __FILE__, "\x10", result_type::success );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47 > >( __LINE__, __FILE__, "\x17", result_type::success );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47 > >( __LINE__, __FILE__, "\x40", result_type::success );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47 > >( __LINE__, __FILE__, "\x47", result_type::success );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47 > >( __LINE__, __FILE__, "\xf0", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47 > >( __LINE__, __FILE__, "\xf1", result_type::local_failure );

      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47, 0xf0 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47, 0xf0 > >( __LINE__, __FILE__, "\x01", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47, 0xf0 > >( __LINE__, __FILE__, "\x0f", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47, 0xf0 > >( __LINE__, __FILE__, "\x18", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47, 0xf0 > >( __LINE__, __FILE__, "\x3f", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47, 0xf0 > >( __LINE__, __FILE__, "\x48", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47, 0xf0 > >( __LINE__, __FILE__, "\x94", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47, 0xf0 > >( __LINE__, __FILE__, "\x10", result_type::success );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47, 0xf0 > >( __LINE__, __FILE__, "\x17", result_type::success );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47, 0xf0 > >( __LINE__, __FILE__, "\x40", result_type::success );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47, 0xf0 > >( __LINE__, __FILE__, "\x47", result_type::success );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47, 0xf0 > >( __LINE__, __FILE__, "\xf0", result_type::success );
      verify_rule< uint8::mask_ranges< 0xff, 0x10, 0x17, 0x40, 0x47, 0xf0 > >( __LINE__, __FILE__, "\xf1", result_type::local_failure );

      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41 > >( __LINE__, __FILE__, "\x01", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41 > >( __LINE__, __FILE__, "\x0f", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41 > >( __LINE__, __FILE__, "\x18", result_type::success );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41 > >( __LINE__, __FILE__, "\x3f", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41 > >( __LINE__, __FILE__, "\x48", result_type::success );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41 > >( __LINE__, __FILE__, "\x94", result_type::success );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41 > >( __LINE__, __FILE__, "\x10", result_type::success );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41 > >( __LINE__, __FILE__, "\x11", result_type::success );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41 > >( __LINE__, __FILE__, "\x40", result_type::success );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41 > >( __LINE__, __FILE__, "\x47", result_type::local_failure );

      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41, 0x73 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41, 0x73 > >( __LINE__, __FILE__, "\x01", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41, 0x73 > >( __LINE__, __FILE__, "\x0f", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41, 0x73 > >( __LINE__, __FILE__, "\x18", result_type::success );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41, 0x73 > >( __LINE__, __FILE__, "\x3f", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41, 0x73 > >( __LINE__, __FILE__, "\x48", result_type::success );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41, 0x73 > >( __LINE__, __FILE__, "\x94", result_type::success );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41, 0x73 > >( __LINE__, __FILE__, "\x10", result_type::success );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41, 0x73 > >( __LINE__, __FILE__, "\x11", result_type::success );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41, 0x73 > >( __LINE__, __FILE__, "\x40", result_type::success );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41, 0x73 > >( __LINE__, __FILE__, "\x47", result_type::local_failure );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41, 0x73 > >( __LINE__, __FILE__, "\x73", result_type::success );
      verify_rule< uint8::mask_ranges< 0x73, 0x10, 0x11, 0x40, 0x41, 0x73 > >( __LINE__, __FILE__, "\x72", result_type::local_failure );

      verify_rule< uint8::mask_string< 0xf0, 0x10, 0x20, 0x30 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::mask_string< 0xf0, 0x10, 0x20, 0x30 > >( __LINE__, __FILE__, "\x10", result_type::local_failure );
      verify_rule< uint8::mask_string< 0xf0, 0x10, 0x20, 0x30 > >( __LINE__, __FILE__, "\x10\x20", result_type::local_failure );
      verify_rule< uint8::mask_string< 0xf0, 0x10, 0x20, 0x30 > >( __LINE__, __FILE__, "\x10\x20\x30", result_type::success );
      verify_rule< uint8::mask_string< 0xf0, 0x10, 0x20, 0x30 > >( __LINE__, __FILE__, "\x10\x20\x30\x10", result_type::success, 1 );
      verify_rule< uint8::mask_string< 0xf0, 0x10, 0x20, 0x30 > >( __LINE__, __FILE__, "\x11\x22\x33", result_type::success );
      verify_rule< uint8::mask_string< 0xf0, 0x10, 0x20, 0x30 > >( __LINE__, __FILE__, "\x1f\x2f\x3f", result_type::success );

      verify_rule< uint8::not_one< 0x10, 0x20 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::not_one< 0x10, 0x20 > >( __LINE__, __FILE__, "\x10", result_type::local_failure );
      verify_rule< uint8::not_one< 0x10, 0x20 > >( __LINE__, __FILE__, "\x20", result_type::local_failure );
      verify_rule< uint8::not_one< 0x10, 0x20 > >( __LINE__, __FILE__, "\x20\x02", result_type::local_failure );
      verify_rule< uint8::not_one< 0x10, 0x20 > >( __LINE__, __FILE__, "\x02", result_type::success );
      verify_rule< uint8::not_one< 0x10, 0x20 > >( __LINE__, __FILE__, "\x02\x20", result_type::success, 1 );
      verify_rule< uint8::not_one< 0x10, 0x20 > >( __LINE__, __FILE__, "\x11", result_type::success );
      verify_rule< uint8::not_one< 0x10, 0x20 > >( __LINE__, __FILE__, "\x1f", result_type::success );

      verify_rule< uint8::not_range< 0x10, 0x20 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::not_range< 0x10, 0x20 > >( __LINE__, __FILE__, "\x0f", result_type::success );
      verify_rule< uint8::not_range< 0x10, 0x20 > >( __LINE__, __FILE__, "\x21", result_type::success );
      verify_rule< uint8::not_range< 0x10, 0x20 > >( __LINE__, __FILE__, "\x10", result_type::local_failure );
      verify_rule< uint8::not_range< 0x10, 0x20 > >( __LINE__, __FILE__, "\x17", result_type::local_failure );
      verify_rule< uint8::not_range< 0x10, 0x20 > >( __LINE__, __FILE__, "\x20", result_type::local_failure );
      verify_rule< uint8::not_range< 0x10, 0x20 > >( __LINE__, __FILE__, "\xab", result_type::success );

      verify_rule< uint8::one< 0x10, 0x20 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::one< 0x10, 0x20 > >( __LINE__, __FILE__, "\x0f", result_type::local_failure );
      verify_rule< uint8::one< 0x10, 0x20 > >( __LINE__, __FILE__, "\x21", result_type::local_failure );
      verify_rule< uint8::one< 0x10, 0x20 > >( __LINE__, __FILE__, "\x10", result_type::success );
      verify_rule< uint8::one< 0x10, 0x20 > >( __LINE__, __FILE__, "\x17", result_type::local_failure );
      verify_rule< uint8::one< 0x10, 0x20 > >( __LINE__, __FILE__, "\x20", result_type::success );

      verify_rule< uint8::range< 0x10, 0x20 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::range< 0x10, 0x20 > >( __LINE__, __FILE__, "\x0f", result_type::local_failure );
      verify_rule< uint8::range< 0x10, 0x20 > >( __LINE__, __FILE__, "\x21", result_type::local_failure );
      verify_rule< uint8::range< 0x10, 0x20 > >( __LINE__, __FILE__, "\x10", result_type::success );
      verify_rule< uint8::range< 0x10, 0x20 > >( __LINE__, __FILE__, "\x17", result_type::success );
      verify_rule< uint8::range< 0x10, 0x20 > >( __LINE__, __FILE__, "\x20", result_type::success );
      verify_rule< uint8::range< 0x10, 0x20 > >( __LINE__, __FILE__, "\xab", result_type::local_failure );

      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40 > >( __LINE__, __FILE__, "\x0f", result_type::local_failure );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40 > >( __LINE__, __FILE__, "\x21", result_type::local_failure );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40 > >( __LINE__, __FILE__, "\x2f", result_type::local_failure );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40 > >( __LINE__, __FILE__, "\x41", result_type::local_failure );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40 > >( __LINE__, __FILE__, "\x8f", result_type::local_failure );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40 > >( __LINE__, __FILE__, "\x10", result_type::success );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40 > >( __LINE__, __FILE__, "\x16", result_type::success );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40 > >( __LINE__, __FILE__, "\x1f", result_type::success );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40 > >( __LINE__, __FILE__, "\x20", result_type::success );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40 > >( __LINE__, __FILE__, "\x30", result_type::success );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40 > >( __LINE__, __FILE__, "\x36", result_type::success );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40 > >( __LINE__, __FILE__, "\x3f", result_type::success );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40 > >( __LINE__, __FILE__, "\x40", result_type::success );

      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40, 0x8f > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40, 0x8f > >( __LINE__, __FILE__, "\x0f", result_type::local_failure );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40, 0x8f > >( __LINE__, __FILE__, "\x21", result_type::local_failure );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40, 0x8f > >( __LINE__, __FILE__, "\x2f", result_type::local_failure );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40, 0x8f > >( __LINE__, __FILE__, "\x41", result_type::local_failure );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40, 0x8f > >( __LINE__, __FILE__, "\x8f", result_type::success );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40, 0x8f > >( __LINE__, __FILE__, "\x10", result_type::success );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40, 0x8f > >( __LINE__, __FILE__, "\x16", result_type::success );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40, 0x8f > >( __LINE__, __FILE__, "\x1f", result_type::success );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40, 0x8f > >( __LINE__, __FILE__, "\x20", result_type::success );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40, 0x8f > >( __LINE__, __FILE__, "\x30", result_type::success );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40, 0x8f > >( __LINE__, __FILE__, "\x36", result_type::success );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40, 0x8f > >( __LINE__, __FILE__, "\x3f", result_type::success );
      verify_rule< uint8::ranges< 0x10, 0x20, 0x30, 0x40, 0x8f > >( __LINE__, __FILE__, "\x40", result_type::success );

      verify_rule< uint8::string< 0x10, 0x20, 0x30 > >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint8::string< 0x10, 0x20, 0x30 > >( __LINE__, __FILE__, "\x10", result_type::local_failure );
      verify_rule< uint8::string< 0x10, 0x20, 0x30 > >( __LINE__, __FILE__, "\x10\x20", result_type::local_failure );
      verify_rule< uint8::string< 0x10, 0x20, 0x30 > >( __LINE__, __FILE__, "\x10\x20\x30", result_type::success );
      verify_rule< uint8::string< 0x10, 0x20, 0x30 > >( __LINE__, __FILE__, "\x10\x20\x30\x10", result_type::success, 1 );
      verify_rule< uint8::string< 0x10, 0x20, 0x30 > >( __LINE__, __FILE__, "\x11\x22\x33", result_type::local_failure );
      verify_rule< uint8::string< 0x10, 0x20, 0x30 > >( __LINE__, __FILE__, "\x1f\x21\x31", result_type::local_failure );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
