// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_rule.hpp"

#include <tao/pegtl/contrib/utf32.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   namespace
   {
      std::string u32s( const char32_t u )
      {
         return std::string( reinterpret_cast< const char* >( &u ), sizeof( u ) );
      }

      std::string u32s_be( const char32_t v )
      {
         const std::uint32_t u = internal::h_to_be( std::uint32_t( v ) );
         return std::string( reinterpret_cast< const char* >( &u ), sizeof( u ) );
      }

      std::string u32s_le( const char32_t v )
      {
         const std::uint32_t u = internal::h_to_le( std::uint32_t( v ) );
         return std::string( reinterpret_cast< const char* >( &u ), sizeof( u ) );
      }

   }  // namespace

   void test_utf32()
   {
      verify_rule< utf32::any >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< utf32::any >( __LINE__, __FILE__, "\xff", result_type::local_failure, 1 );
      verify_rule< utf32::any >( __LINE__, __FILE__, "\xff\xff", result_type::local_failure, 2 );
      verify_rule< utf32::any >( __LINE__, __FILE__, "\xff\xff\xff", result_type::local_failure, 3 );

      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0 ), result_type::success, 0 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 1 ), result_type::success, 0 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0x00ff ) + " ", result_type::success, 1 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0x0100 ) + "  ", result_type::success, 2 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0x0fff ) + "   ", result_type::success, 3 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0x1000 ) + "    ", result_type::success, 4 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0xd7ff ), result_type::success, 0 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0xe000 ), result_type::success, 0 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0xfffe ), result_type::success, 0 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0xffff ), result_type::success, 0 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0x100000 ), result_type::success, 0 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0x10fffe ), result_type::success, 0 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0x10ffff ), result_type::success, 0 );

      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0xd800 ), result_type::local_failure, 4 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0xd900 ), result_type::local_failure, 4 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0xdc00 ), result_type::local_failure, 4 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0xdfff ), result_type::local_failure, 4 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0x110000 ), result_type::local_failure, 4 );
      verify_rule< utf32::any >( __LINE__, __FILE__, u32s( 0x110000 ) + u32s( 0 ), result_type::local_failure, 8 );

      verify_rule< utf32::one< 0x20 > >( __LINE__, __FILE__, u32s( 0x20 ), result_type::success, 0 );
      verify_rule< utf32::one< 0x20ac > >( __LINE__, __FILE__, u32s( 0x20ac ), result_type::success, 0 );
      verify_rule< utf32::one< 0x10fedc > >( __LINE__, __FILE__, u32s( 0x10fedc ), result_type::success, 0 );

      verify_rule< utf32::string< 0x20, 0x20ac, 0x10fedc > >( __LINE__, __FILE__, u32s( 0x20 ) + u32s( 0x20ac ) + u32s( 0x10fedc ) + u32s( 0x20 ), result_type::success, 4 );
   }

   void test_utf32_be()
   {
      verify_rule< utf32_be::any >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< utf32_be::any >( __LINE__, __FILE__, "\xff", result_type::local_failure, 1 );
      verify_rule< utf32_be::any >( __LINE__, __FILE__, "\xff\xff", result_type::local_failure, 2 );
      verify_rule< utf32_be::any >( __LINE__, __FILE__, "\xff\xff\xff", result_type::local_failure, 3 );

      verify_rule< utf32_be::any >( __LINE__, __FILE__, u32s_be( 0 ), result_type::success, 0 );
      verify_rule< utf32_be::any >( __LINE__, __FILE__, u32s_be( 1 ), result_type::success, 0 );
      verify_rule< utf32_be::any >( __LINE__, __FILE__, u32s_be( 0x00ff ) + " ", result_type::success, 1 );
      verify_rule< utf32_be::any >( __LINE__, __FILE__, u32s_be( 0x0100 ) + "  ", result_type::success, 2 );
      verify_rule< utf32_be::any >( __LINE__, __FILE__, u32s_be( 0x0fff ) + "   ", result_type::success, 3 );
      verify_rule< utf32_be::any >( __LINE__, __FILE__, u32s_be( 0x1000 ) + "    ", result_type::success, 4 );
      verify_rule< utf32_be::any >( __LINE__, __FILE__, u32s_be( 0xfffe ), result_type::success, 0 );
      verify_rule< utf32_be::any >( __LINE__, __FILE__, u32s_be( 0xffff ), result_type::success, 0 );
      verify_rule< utf32_be::any >( __LINE__, __FILE__, u32s_be( 0x100000 ), result_type::success, 0 );
      verify_rule< utf32_be::any >( __LINE__, __FILE__, u32s_be( 0x10fffe ), result_type::success, 0 );
      verify_rule< utf32_be::any >( __LINE__, __FILE__, u32s_be( 0x10ffff ), result_type::success, 0 );

      verify_rule< utf32_be::any >( __LINE__, __FILE__, u32s_be( 0x110000 ), result_type::local_failure, 4 );
      verify_rule< utf32_be::any >( __LINE__, __FILE__, u32s_be( 0x110000 ) + u32s_be( 0 ), result_type::local_failure, 8 );

      verify_rule< utf32_be::one< 0x20 > >( __LINE__, __FILE__, u32s_be( 0x20 ), result_type::success, 0 );
      verify_rule< utf32_be::one< 0x20ac > >( __LINE__, __FILE__, u32s_be( 0x20ac ), result_type::success, 0 );
      verify_rule< utf32_be::one< 0x10fedc > >( __LINE__, __FILE__, u32s_be( 0x10fedc ), result_type::success, 0 );

      verify_rule< utf32_be::string< 0x20, 0x20ac, 0x10fedc > >( __LINE__, __FILE__, u32s_be( 0x20 ) + u32s_be( 0x20ac ) + u32s_be( 0x10fedc ) + u32s_be( 0x20 ), result_type::success, 4 );
   }

   void test_utf32_le()
   {
      verify_rule< utf32_le::any >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< utf32_le::any >( __LINE__, __FILE__, "\xff", result_type::local_failure, 1 );
      verify_rule< utf32_le::any >( __LINE__, __FILE__, "\xff\xff", result_type::local_failure, 2 );
      verify_rule< utf32_le::any >( __LINE__, __FILE__, "\xff\xff\xff", result_type::local_failure, 3 );

      verify_rule< utf32_le::any >( __LINE__, __FILE__, u32s_le( 0 ), result_type::success, 0 );
      verify_rule< utf32_le::any >( __LINE__, __FILE__, u32s_le( 1 ), result_type::success, 0 );
      verify_rule< utf32_le::any >( __LINE__, __FILE__, u32s_le( 0x00ff ) + " ", result_type::success, 1 );
      verify_rule< utf32_le::any >( __LINE__, __FILE__, u32s_le( 0x0100 ) + "  ", result_type::success, 2 );
      verify_rule< utf32_le::any >( __LINE__, __FILE__, u32s_le( 0x0fff ) + "   ", result_type::success, 3 );
      verify_rule< utf32_le::any >( __LINE__, __FILE__, u32s_le( 0x1000 ) + "    ", result_type::success, 4 );
      verify_rule< utf32_le::any >( __LINE__, __FILE__, u32s_le( 0xfffe ), result_type::success, 0 );
      verify_rule< utf32_le::any >( __LINE__, __FILE__, u32s_le( 0xffff ), result_type::success, 0 );
      verify_rule< utf32_le::any >( __LINE__, __FILE__, u32s_le( 0x100000 ), result_type::success, 0 );
      verify_rule< utf32_le::any >( __LINE__, __FILE__, u32s_le( 0x10fffe ), result_type::success, 0 );
      verify_rule< utf32_le::any >( __LINE__, __FILE__, u32s_le( 0x10ffff ), result_type::success, 0 );

      verify_rule< utf32_le::any >( __LINE__, __FILE__, u32s_le( 0x110000 ), result_type::local_failure, 4 );
      verify_rule< utf32_le::any >( __LINE__, __FILE__, u32s_le( 0x110000 ) + u32s_le( 0 ), result_type::local_failure, 8 );

      verify_rule< utf32_le::one< 0x20 > >( __LINE__, __FILE__, u32s_le( 0x20 ), result_type::success, 0 );
      verify_rule< utf32_le::one< 0x20ac > >( __LINE__, __FILE__, u32s_le( 0x20ac ), result_type::success, 0 );
      verify_rule< utf32_le::one< 0x10fedc > >( __LINE__, __FILE__, u32s_le( 0x10fedc ), result_type::success, 0 );

      verify_rule< utf32_le::string< 0x20, 0x20ac, 0x10fedc > >( __LINE__, __FILE__, u32s_le( 0x20 ) + u32s_le( 0x20ac ) + u32s_le( 0x10fedc ) + u32s_le( 0x20 ), result_type::success, 4 );
   }

   void unit_test()
   {
      test_utf32();
      test_utf32_be();
      test_utf32_le();
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
