// Copyright (c) 2018-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_char.hpp"
#include "verify_rule.hpp"

#include <tao/pegtl/contrib/uint64.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_rule< uint64_be::any >( __LINE__, __FILE__, "", result_type::local_failure );
      verify_rule< uint64_le::any >( __LINE__, __FILE__, "", result_type::local_failure );

      verify_rule< uint64_be::any >( __LINE__, __FILE__, "\x01", result_type::local_failure );
      verify_rule< uint64_le::any >( __LINE__, __FILE__, "\x01", result_type::local_failure );

      verify_rule< uint64_be::any >( __LINE__, __FILE__, "\x71\x72", result_type::local_failure );
      verify_rule< uint64_le::any >( __LINE__, __FILE__, "\x71\x72", result_type::local_failure );

      verify_rule< uint64_be::any >( __LINE__, __FILE__, "\x71\x72\x03", result_type::local_failure );
      verify_rule< uint64_le::any >( __LINE__, __FILE__, "\x71\x72\x03", result_type::local_failure );

      verify_rule< uint64_be::any >( __LINE__, __FILE__, "\x71\x72\x03\x55", result_type::local_failure );
      verify_rule< uint64_le::any >( __LINE__, __FILE__, "\x71\x72\x03\x55", result_type::local_failure );

      verify_rule< uint64_be::any >( __LINE__, __FILE__, "\x71\x72\x03\x55\x44", result_type::local_failure );
      verify_rule< uint64_le::any >( __LINE__, __FILE__, "\x71\x72\x03\x55\x44", result_type::local_failure );

      verify_rule< uint64_be::any >( __LINE__, __FILE__, "\x71\x72\x03\x55\x44\x33", result_type::local_failure );
      verify_rule< uint64_le::any >( __LINE__, __FILE__, "\x71\x72\x03\x55\x44\x33", result_type::local_failure );

      verify_rule< uint64_be::any >( __LINE__, __FILE__, "\x71\x72\x03\x55\x44\x33\x22", result_type::local_failure );
      verify_rule< uint64_le::any >( __LINE__, __FILE__, "\x71\x72\x03\x55\x44\x33\x22", result_type::local_failure );

      verify_rule< uint64_be::any >( __LINE__, __FILE__, "\x71\x72\x03\x55\x44\x33\x22\x11", result_type::success );
      verify_rule< uint64_le::any >( __LINE__, __FILE__, "\x71\x72\x03\x55\x44\x33\x22\x11", result_type::success );

      verify_rule< uint64_be::any >( __LINE__, __FILE__, "\x71\x72\x03\x55\x44\x33\x22\x11\x99", result_type::success, 1 );
      verify_rule< uint64_le::any >( __LINE__, __FILE__, "\x71\x72\x03\x55\x44\x33\x22\x11\x99", result_type::success, 1 );

      verify_rule< uint64_be::mask_not_one< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x01\x11\x11\x11\x11\x11\x11", result_type::local_failure );

      verify_rule< uint64_be::mask_not_one< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x01\x11\x11\x11\x11\x11\x11\x11", result_type::local_failure );
      verify_rule< uint64_be::mask_not_one< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x11\x11\x11\x11\x11\x11\x11\x01", result_type::success );

      verify_rule< uint64_le::mask_not_one< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x01\x11\x11\x11\x11\x11\x11\x11", result_type::success );
      verify_rule< uint64_le::mask_not_one< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x11\x11\x11\x11\x11\x11\x11\x01", result_type::local_failure );

      verify_rule< uint64_be::mask_not_one< 0x0fffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\xf3\x11\x11\x11\x11\x11\x11\x11", result_type::success );
      verify_rule< uint64_be::mask_not_one< 0x0fffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\xf1\x11\x11\x11\x11\x11\x11\x11", result_type::local_failure );

      verify_rule< uint64_le::mask_not_one< 0x0fffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x11\x11\x11\x11\x11\x11\x11\xf3", result_type::success );
      verify_rule< uint64_le::mask_not_one< 0x0fffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x11\x11\x11\x11\x11\x11\x11\xf1", result_type::local_failure );

      verify_rule< uint64_be::mask_not_range< 0xffffffffffffffff, 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x02\x77\x77\x77\x77\x77\x77\x77", result_type::local_failure );
      verify_rule< uint64_be::mask_not_range< 0xffffffffffffffff, 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x77\x77\x77\x77\x77\x77\x77\x02", result_type::success );

      verify_rule< uint64_le::mask_not_range< 0xffffffffffffffff, 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x02\x77\x77\x77\x77\x77\x77\x77", result_type::success );
      verify_rule< uint64_le::mask_not_range< 0xffffffffffffffff, 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x77\x77\x77\x77\x77\x77\x77\x02", result_type::local_failure );

      verify_rule< uint64_be::mask_not_range< 0x0fffffffffffffff, 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x52\x77\x77\x77\x77\x77\x77\x77", result_type::local_failure );
      verify_rule< uint64_be::mask_not_range< 0x0fffffffffffffff, 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x56\x77\x77\x77\x77\x77\x77\x77", result_type::success );

      verify_rule< uint64_le::mask_not_range< 0x0fffffffffffffff, 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x77\x77\x77\x77\x77\x77\x77\x52", result_type::local_failure );
      verify_rule< uint64_le::mask_not_range< 0x0fffffffffffffff, 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x77\x77\x77\x77\x77\x77\x77\x56", result_type::success );

      verify_rule< uint64_be::mask_one< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x01\x11\x11\x11\x11\x11\x11\x11", result_type::success );
      verify_rule< uint64_be::mask_one< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x11\x11\x11\x11\x11\x11\x11\x01", result_type::local_failure );

      verify_rule< uint64_le::mask_one< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x01\x11\x11\x11\x11\x11\x11\x11", result_type::local_failure );
      verify_rule< uint64_le::mask_one< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x11\x11\x11\x11\x11\x11\x11\x01", result_type::success );

      verify_rule< uint64_be::mask_one< 0x0fffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\xf3\x11\x11\x11\x11\x11\x11\x11", result_type::local_failure );
      verify_rule< uint64_be::mask_one< 0x0fffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\xf1\x11\x11\x11\x11\x11\x11\x11", result_type::success );

      verify_rule< uint64_le::mask_one< 0x0fffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x11\x11\x11\x11\x11\x11\x11\xf3", result_type::local_failure );
      verify_rule< uint64_le::mask_one< 0x0fffffffffffffff, 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x11\x11\x11\x11\x11\x11\x11\xf1", result_type::success );

      verify_rule< uint64_be::mask_range< 0xffffffffffffffff, 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x02\x77\x77\x77\x77\x77\x77\x77", result_type::success );
      verify_rule< uint64_be::mask_range< 0xffffffffffffffff, 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x77\x77\x77\x77\x77\x77\x77\x02", result_type::local_failure );

      verify_rule< uint64_le::mask_range< 0xffffffffffffffff, 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x02\x77\x77\x77\x77\x77\x77\x77", result_type::local_failure );
      verify_rule< uint64_le::mask_range< 0xffffffffffffffff, 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x77\x77\x77\x77\x77\x77\x77\x02", result_type::success );

      verify_rule< uint64_be::mask_range< 0x0fffffffffffffff, 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x52\x77\x77\x77\x77\x77\x77\x77", result_type::success );
      verify_rule< uint64_be::mask_range< 0x0fffffffffffffff, 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x56\x77\x77\x77\x77\x77\x77\x77", result_type::local_failure );

      verify_rule< uint64_le::mask_range< 0x0fffffffffffffff, 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x77\x77\x77\x77\x77\x77\x77\x52", result_type::success );
      verify_rule< uint64_le::mask_range< 0x0fffffffffffffff, 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x77\x77\x77\x77\x77\x77\x77\x56", result_type::local_failure );

      verify_rule< uint64_be::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x01\x23\x45\x67\x99\x99\x99\x99", result_type::success );
      verify_rule< uint64_be::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x02\x34\x56\x78\x99\x99\x99\x99", result_type::local_failure );
      verify_rule< uint64_be::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x03\x45\x67\x89\x99\x99\x99\x99", result_type::success );
      verify_rule< uint64_be::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x67\x89\x12\x34\x99\x99\x99\x99", result_type::local_failure );

      verify_rule< uint64_le::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x67\x45\x23\x01", result_type::success );
      verify_rule< uint64_le::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x78\x56\x34\x02", result_type::local_failure );
      verify_rule< uint64_le::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x89\x67\x45\x03", result_type::success );
      verify_rule< uint64_le::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x34\x12\x89\x67", result_type::local_failure );

      verify_rule< uint64_be::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444, 0x6789010299999999 > >( __LINE__, __FILE__, "\x01\x23\x45\x67\x99\x99\x99\x99", result_type::success );
      verify_rule< uint64_be::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444, 0x6789010299999999 > >( __LINE__, __FILE__, "\x02\x34\x56\x78\x99\x99\x99\x99", result_type::local_failure );
      verify_rule< uint64_be::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444, 0x6789010299999999 > >( __LINE__, __FILE__, "\x03\x45\x67\x89\x99\x99\x99\x99", result_type::success );
      verify_rule< uint64_be::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444, 0x6789010299999999 > >( __LINE__, __FILE__, "\x67\x89\x01\x02\x99\x99\x99\x99", result_type::success );

      verify_rule< uint64_le::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444, 0x6789010299999999 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x67\x45\x23\x01", result_type::success );
      verify_rule< uint64_le::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444, 0x6789010299999999 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x78\x56\x34\x02", result_type::local_failure );
      verify_rule< uint64_le::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444, 0x6789010299999999 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x89\x67\x45\x03", result_type::success );
      verify_rule< uint64_le::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444, 0x6789010299999999 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x02\x01\x89\x67", result_type::success );

      verify_rule< uint64_be::mask_ranges< 0xff0fffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x02\x50\x02\x02\x99\x99\x99\x99", result_type::success );
      verify_rule< uint64_be::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x02\x50\x02\x02\x99\x99\x99\x99", result_type::local_failure );

      verify_rule< uint64_le::mask_ranges< 0xff0fffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x02\x02\x50\x02", result_type::success );
      verify_rule< uint64_le::mask_ranges< 0xffffffffffffffff, 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x02\x02\x50\x02", result_type::local_failure );

      verify_rule< uint64_be::mask_string< 0xffffffffffffffff, 0x01233210deadcafe, 0x45677654baffb1ff > >( __LINE__, __FILE__, "\x01\x23\x32\x10\xde\xad\xca\xfe\x45\x67\x76\x54\xba\xff\xb1\xff", result_type::success );
      verify_rule< uint64_be::mask_string< 0xffffffffffffffff, 0x01233210deadcafe, 0x45677654baffb1ff > >( __LINE__, __FILE__, "\xfe\xca\xad\xde\x10\x32\x23\x01\xff\xb1\xff\xba\x54\x76\x67\x45", result_type::local_failure );

      verify_rule< uint64_le::mask_string< 0xffffffffffffffff, 0x01233210deadcafe, 0x45677654baffb1ff > >( __LINE__, __FILE__, "\x01\x23\x32\x10\xde\xad\xca\xfe\x45\x67\x76\x54\xba\xff\xb1\xff", result_type::local_failure );
      verify_rule< uint64_le::mask_string< 0xffffffffffffffff, 0x01233210deadcafe, 0x45677654baffb1ff > >( __LINE__, __FILE__, "\xfe\xca\xad\xde\x10\x32\x23\x01\xff\xb1\xff\xba\x54\x76\x67\x45", result_type::success );

      verify_rule< uint64_be::mask_string< 0x4fffffffffffffff, 0x01233210deadcafe, 0x45677654baffb1ff > >( __LINE__, __FILE__, "\x81\x23\x32\x10\xde\xad\xca\xfe\x45\x67\x76\x54\xba\xff\xb1\xff", result_type::success );
      verify_rule< uint64_be::mask_string< 0x8fffffffffffffff, 0x01233210deadcafe, 0x45677654baffb1ff > >( __LINE__, __FILE__, "\x81\x23\x32\x10\xde\xad\xca\xfe\x45\x67\x76\x54\xba\xff\xb1\xff", result_type::local_failure );

      verify_rule< uint64_le::mask_string< 0x4fffffffffffffff, 0x01233210deadcafe, 0x45677654baffb1ff > >( __LINE__, __FILE__, "\xfe\xca\xad\xde\x10\x32\x23\x81\xff\xb1\xff\xba\x54\x76\x67\x45", result_type::success );
      verify_rule< uint64_le::mask_string< 0x8fffffffffffffff, 0x01233210deadcafe, 0x45677654baffb1ff > >( __LINE__, __FILE__, "\xfe\xca\xad\xde\x10\x32\x23\x81\xff\xb1\xff\xba\x54\x76\x67\x45", result_type::local_failure );

      verify_rule< uint64_be::not_one< 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x01\x11\x11\x11\x11\x11\x11\x11", result_type::local_failure );
      verify_rule< uint64_be::not_one< 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x11\x11\x11\x11\x11\x11\x11\x01", result_type::success );

      verify_rule< uint64_le::not_one< 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x01\x11\x11\x11\x11\x11\x11\x11", result_type::success );
      verify_rule< uint64_le::not_one< 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x11\x11\x11\x11\x11\x11\x11\x01", result_type::local_failure );

      verify_rule< uint64_be::not_range< 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x02\x77\x77\x77\x77\x77\x77\x77", result_type::local_failure );
      verify_rule< uint64_be::not_range< 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x77\x77\x77\x77\x77\x77\x77\x02", result_type::success );

      verify_rule< uint64_le::not_range< 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x02\x77\x77\x77\x77\x77\x77\x77", result_type::success );
      verify_rule< uint64_le::not_range< 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x77\x77\x77\x77\x77\x77\x77\x02", result_type::local_failure );

      verify_rule< uint64_be::one< 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x01\x11\x11\x11\x11\x11\x11\x11", result_type::success );
      verify_rule< uint64_be::one< 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x11\x11\x11\x11\x11\x11\x11\x01", result_type::local_failure );

      verify_rule< uint64_le::one< 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x01\x11\x11\x11\x11\x11\x11\x11", result_type::local_failure );
      verify_rule< uint64_le::one< 0x0111111111111111, 0x0222222222222222 > >( __LINE__, __FILE__, "\x11\x11\x11\x11\x11\x11\x11\x01", result_type::success );

      verify_rule< uint64_be::range< 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x02\x77\x77\x77\x77\x77\x77\x77", result_type::success );
      verify_rule< uint64_be::range< 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x77\x77\x77\x77\x77\x77\x77\x02", result_type::local_failure );

      verify_rule< uint64_le::range< 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x02\x77\x77\x77\x77\x77\x77\x77", result_type::local_failure );
      verify_rule< uint64_le::range< 0x0100000000000000, 0x0400000000000000 > >( __LINE__, __FILE__, "\x77\x77\x77\x77\x77\x77\x77\x02", result_type::success );

      verify_rule< uint64_be::ranges< 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x01\x23\x45\x67\x99\x99\x99\x99", result_type::success );
      verify_rule< uint64_be::ranges< 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x02\x34\x56\x78\x99\x99\x99\x99", result_type::local_failure );
      verify_rule< uint64_be::ranges< 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x03\x45\x67\x89\x99\x99\x99\x99", result_type::success );
      verify_rule< uint64_be::ranges< 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x67\x89\x12\x34\x99\x99\x99\x99", result_type::local_failure );

      verify_rule< uint64_le::ranges< 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x67\x45\x23\x01", result_type::success );
      verify_rule< uint64_le::ranges< 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x78\x56\x34\x02", result_type::local_failure );
      verify_rule< uint64_le::ranges< 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x89\x67\x45\x03", result_type::success );
      verify_rule< uint64_le::ranges< 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x34\x12\x89\x67", result_type::local_failure );

      verify_rule< uint64_be::ranges< 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444, 0x6789010299999999 > >( __LINE__, __FILE__, "\x01\x23\x45\x67\x99\x99\x99\x99", result_type::success );
      verify_rule< uint64_be::ranges< 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444, 0x6789010299999999 > >( __LINE__, __FILE__, "\x02\x34\x56\x78\x99\x99\x99\x99", result_type::local_failure );
      verify_rule< uint64_be::ranges< 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444, 0x6789010299999999 > >( __LINE__, __FILE__, "\x03\x45\x67\x89\x99\x99\x99\x99", result_type::success );
      verify_rule< uint64_be::ranges< 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444, 0x6789010299999999 > >( __LINE__, __FILE__, "\x67\x89\x01\x02\x99\x99\x99\x99", result_type::success );

      verify_rule< uint64_le::ranges< 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444, 0x6789010299999999 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x67\x45\x23\x01", result_type::success );
      verify_rule< uint64_le::ranges< 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444, 0x6789010299999999 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x78\x56\x34\x02", result_type::local_failure );
      verify_rule< uint64_le::ranges< 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444, 0x6789010299999999 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x89\x67\x45\x03", result_type::success );
      verify_rule< uint64_le::ranges< 0x0111111111111111, 0x0222222222222222, 0x0333333333333333, 0x0444444444444444, 0x6789010299999999 > >( __LINE__, __FILE__, "\x99\x99\x99\x99\x02\x01\x89\x67", result_type::success );

      verify_rule< uint64_be::string< 0x01233210deadcafe, 0x45677654baffb1ff > >( __LINE__, __FILE__, "\x01\x23\x32\x10\xde\xad\xca\xfe\x45\x67\x76\x54\xba\xff\xb1\xff", result_type::success );
      verify_rule< uint64_be::string< 0x01233210deadcafe, 0x45677654baffb1ff > >( __LINE__, __FILE__, "\xfe\xca\xad\xde\x10\x32\x23\x01\xff\xb1\xff\xba\x54\x76\x67\x45", result_type::local_failure );

      verify_rule< uint64_le::string< 0x01233210deadcafe, 0x45677654baffb1ff > >( __LINE__, __FILE__, "\x01\x23\x32\x10\xde\xad\xca\xfe\x45\x67\x76\x54\xba\xff\xb1\xff", result_type::local_failure );
      verify_rule< uint64_le::string< 0x01233210deadcafe, 0x45677654baffb1ff > >( __LINE__, __FILE__, "\xfe\xca\xad\xde\x10\x32\x23\x01\xff\xb1\xff\xba\x54\x76\x67\x45", result_type::success );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
