// Copyright (c) 2018-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_analyze< forty_two< 'a' > >( __LINE__, __FILE__, true, false );
      verify_analyze< forty_two< 'a', 'z' > >( __LINE__, __FILE__, true, false );

      for( std::size_t i = 0; i < 42; ++i ) {
         verify_rule< forty_two< 'a' > >( __LINE__, __FILE__, std::string( i, 'a' ), result_type::local_failure );
      }
      for( std::size_t i = 42; i < 100; ++i ) {
         verify_rule< forty_two< 'a' > >( __LINE__, __FILE__, std::string( i, 'a' ), result_type::success, int( i - 42 ) );
      }
      for( std::size_t i = 0; i < 42; ++i ) {
         verify_rule< forty_two< 'a', 'z' > >( __LINE__, __FILE__, std::string( i, 'a' ), result_type::local_failure );
      }
      for( std::size_t i = 42; i < 100; ++i ) {
         verify_rule< forty_two< 'a', 'z' > >( __LINE__, __FILE__, std::string( i, 'a' ), result_type::success, int( i - 42 ) );
      }
      for( std::size_t i = 0; i < 42; ++i ) {
         verify_rule< forty_two< 'a', 'z' > >( __LINE__, __FILE__, std::string( i, 'z' ), result_type::local_failure );
      }
      for( std::size_t i = 42; i < 100; ++i ) {
         verify_rule< forty_two< 'a', 'z' > >( __LINE__, __FILE__, std::string( i, 'z' ), result_type::success, int( i - 42 ) );
      }
      verify_rule< forty_two< 'a', 'z' > >( __LINE__, __FILE__, "azzaazaazaaazzzaaaazzaaazzaazazzzaazzazaza", result_type::success );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
