// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_char.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_rule< utf8::any >( __LINE__, __FILE__, "", result_type::local_failure, 0 );

      for( int i = -100; i < 200; ++i ) {
         verify_char< utf8::any >( __LINE__, __FILE__, char( i ), ( i & 0x80 ) == 0 );
      }
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x30", result_type::success, 0 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xc2\xa2", result_type::success, 0 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xe2\x82\xac", result_type::success, 0 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf0\x90\x8d\x88", result_type::success, 0 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\x30\x20", result_type::success, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xc2\xa2\x20", result_type::success, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xe2\x82\xac\x20", result_type::success, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf0\x90\x8d\x88\x20", result_type::success, 1 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xc0", result_type::local_failure, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf", result_type::local_failure, 1 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xc0\x01", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xc0\x3c", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xc0\x7f", result_type::local_failure, 2 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\x01", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\x3c", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\x7f", result_type::local_failure, 2 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xc0\x01\x81", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xc0\x3c\x81", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xc0\x7f\x81", result_type::local_failure, 3 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\x01\x81", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\x3c\x81", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\x7f\x81", result_type::local_failure, 3 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xc3\x81", result_type::success, 0 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xc3\x81\x81", result_type::success, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xc3\x81\x81\x81", result_type::success, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xc3\x81\x81\x81\x81", result_type::success, 3 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\x80", result_type::success, 0 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\x80\x80", result_type::success, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\x80\x80\x80", result_type::success, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\x80\x80\x80\x80", result_type::success, 3 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\xbf", result_type::success, 0 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\xbf\xbf", result_type::success, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\xbf\xbf\xbf", result_type::success, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\xbf\xbf\xbf\xbf", result_type::success, 3 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\xff", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\xff\xff", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\xff\xff\xff", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xcf\xff\xff\xff\xff", result_type::local_failure, 5 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xe0", result_type::local_failure, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xe7", result_type::local_failure, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xef", result_type::local_failure, 1 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xe0\x0f", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xe7\x0f", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xef\x0f", result_type::local_failure, 2 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xe0\x80", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xe7\x80", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xef\x80", result_type::local_failure, 2 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xe0\xff", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xe7\xff", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xef\xff", result_type::local_failure, 2 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xe0\x80\x80", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xe0\x80\x80\x80", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xe0\xff\xff", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xef\xff\xff\xff", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xef\xff\xff", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xef\xff\xff\xff", result_type::local_failure, 4 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xe7\x80\x80", result_type::success, 0 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xe7\x80\x80\x80", result_type::success, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xe7\x80\x80\x80\x80", result_type::success, 2 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xef\xbf\xbf", result_type::success, 0 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xef\xbf\xbf\xbf", result_type::success, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xef\xbf\xbf\xbf\xbf", result_type::success, 2 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xef\x80\xff", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xef\x80\xff\xff", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xef\x80\xff\xff\xff", result_type::local_failure, 5 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xef\xff\xff", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xef\xff\xff\xff", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xef\xff\xff\xff\xff", result_type::local_failure, 5 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf0", result_type::local_failure, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf7", result_type::local_failure, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf0\x80", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf7\x80", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf0\x80\x80", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf7\x80\x80", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf0\x80\x80\xff", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf7\x80\x80\xff", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf0\xc0\x80\x80", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf7\xc0\x80\x80", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf0\x80\xf0\x80", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf7\x80\xf0\x80", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf0\x80\x80\x80", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf5\x80\x80\x80", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf6\x80\x80\x80", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf7\x80\x80\x80", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf4\x9f\xbf\xbf", result_type::local_failure, 4 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf1\x80\x80\x80", result_type::success, 0 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf2\x80\x80\x80", result_type::success, 0 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf3\x80\x80\x80", result_type::success, 0 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf4\x80\x80\x80", result_type::success, 0 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf0\xa0\x80\x80", result_type::success, 0 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf0\x90\x80\x80", result_type::success, 0 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf4\x8f\xbf\xbf", result_type::success, 0 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf1\x80\x80\x80\x80", result_type::success, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf2\x80\x80\x80\x80", result_type::success, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf3\x80\x80\x80\x80", result_type::success, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf4\x80\x80\x80\x80", result_type::success, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf0\xa0\x80\x80\x80", result_type::success, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf0\x90\x80\x80\x80", result_type::success, 1 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xff", result_type::local_failure, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xff ", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xff  ", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xff   ", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xff    ", result_type::local_failure, 5 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xff     ", result_type::local_failure, 6 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xff      ", result_type::local_failure, 7 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xff       ", result_type::local_failure, 8 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xff\x80", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xff\x80\x80", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xff\x80\x80\x80", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xff\x80\x80\x80\x80", result_type::local_failure, 5 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xff\x80\x80\x80\x80\x80", result_type::local_failure, 6 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xff\x80\x80\x80\x80\x80\x80", result_type::local_failure, 7 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xff\x80\x80\x80\x80\x80\x80\x80", result_type::local_failure, 8 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfe", result_type::local_failure, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfe ", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfe  ", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfe   ", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfe    ", result_type::local_failure, 5 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfe     ", result_type::local_failure, 6 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfe      ", result_type::local_failure, 7 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfe       ", result_type::local_failure, 8 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfe\x80", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfe\x80\x80", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfe\x80\x80\x80", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfe\x80\x80\x80\x80", result_type::local_failure, 5 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfe\x80\x80\x80\x80\x80", result_type::local_failure, 6 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfe\x80\x80\x80\x80\x80\x80", result_type::local_failure, 7 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfe\x80\x80\x80\x80\x80\x80\x80", result_type::local_failure, 8 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfc", result_type::local_failure, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfc ", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfc  ", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfc   ", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfc    ", result_type::local_failure, 5 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfc     ", result_type::local_failure, 6 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfc      ", result_type::local_failure, 7 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfc       ", result_type::local_failure, 8 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfc\x80", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfc\x80\x80", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfc\x80\x80\x80", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfc\x80\x80\x80\x80", result_type::local_failure, 5 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfc\x80\x80\x80\x80\x80", result_type::local_failure, 6 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfc\x80\x80\x80\x80\x80\x80", result_type::local_failure, 7 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xfc\x80\x80\x80\x80\x80\x80\x80", result_type::local_failure, 8 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf8", result_type::local_failure, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf8 ", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf8  ", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf8   ", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf8    ", result_type::local_failure, 5 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf8     ", result_type::local_failure, 6 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf8      ", result_type::local_failure, 7 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf8       ", result_type::local_failure, 8 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf8\x80", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf8\x80\x80", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf8\x80\x80\x80", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf8\x80\x80\x80\x80", result_type::local_failure, 5 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf8\x80\x80\x80\x80\x80", result_type::local_failure, 6 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf8\x80\x80\x80\x80\x80\x80", result_type::local_failure, 7 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\xf8\x80\x80\x80\x80\x80\x80\x80", result_type::local_failure, 8 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\x80", result_type::local_failure, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x80 ", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x80  ", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x80   ", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x80    ", result_type::local_failure, 5 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x80     ", result_type::local_failure, 6 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x80      ", result_type::local_failure, 7 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x80       ", result_type::local_failure, 8 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x80\x80", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x80\x80\x80", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x80\x80\x80\x80", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x80\x80\x80\x80\x80", result_type::local_failure, 5 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x80\x80\x80\x80\x80\x80", result_type::local_failure, 6 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x80\x80\x80\x80\x80\x80\x80", result_type::local_failure, 7 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x80\x80\x80\x80\x80\x80\x80\x80", result_type::local_failure, 8 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\x81", result_type::local_failure, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x81 ", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x81  ", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x81   ", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x81    ", result_type::local_failure, 5 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x81     ", result_type::local_failure, 6 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x81      ", result_type::local_failure, 7 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x81       ", result_type::local_failure, 8 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x81\x80", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x81\x80\x80", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x81\x80\x80\x80", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x81\x80\x80\x80\x80", result_type::local_failure, 5 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x81\x80\x80\x80\x80\x80", result_type::local_failure, 6 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x81\x80\x80\x80\x80\x80\x80", result_type::local_failure, 7 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x81\x80\x80\x80\x80\x80\x80\x80", result_type::local_failure, 8 );

      verify_rule< utf8::any >( __LINE__, __FILE__, "\x8f", result_type::local_failure, 1 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x8f ", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x8f  ", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x8f   ", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x8f    ", result_type::local_failure, 5 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x8f     ", result_type::local_failure, 6 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x8f      ", result_type::local_failure, 7 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x8f       ", result_type::local_failure, 8 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x8f\x80", result_type::local_failure, 2 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x8f\x80\x80", result_type::local_failure, 3 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x8f\x80\x80\x80", result_type::local_failure, 4 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x8f\x80\x80\x80\x80", result_type::local_failure, 5 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x8f\x80\x80\x80\x80\x80", result_type::local_failure, 6 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x8f\x80\x80\x80\x80\x80\x80", result_type::local_failure, 7 );
      verify_rule< utf8::any >( __LINE__, __FILE__, "\x8f\x80\x80\x80\x80\x80\x80\x80", result_type::local_failure, 8 );

      verify_rule< utf8::one< 0x20 > >( __LINE__, __FILE__, "\x20", result_type::success, 0 );
      verify_rule< utf8::one< 0xa2 > >( __LINE__, __FILE__, "\xc2\xa2", result_type::success, 0 );
      verify_rule< utf8::one< 0x20ac > >( __LINE__, __FILE__, "\xe2\x82\xac", result_type::success, 0 );
      verify_rule< utf8::one< 0x10348 > >( __LINE__, __FILE__, "\xf0\x90\x8d\x88", result_type::success, 0 );

      verify_rule< utf8::bom >( __LINE__, __FILE__, "\xef\xbb\xbf", result_type::success, 0 );
      verify_rule< utf8::bom >( __LINE__, __FILE__, "\xef\xbb\xbf ", result_type::success, 1 );

      verify_rule< utf8::string< 0x20, 0xa2, 0x20ac, 0x10348 > >( __LINE__, __FILE__, "\x20\xc2\xa2\xe2\x82\xac\xf0\x90\x8d\x88\x20", result_type::success, 1 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
