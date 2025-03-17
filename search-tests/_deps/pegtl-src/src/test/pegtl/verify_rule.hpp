// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#ifndef TAO_PEGTL_SRC_TEST_PEGTL_VERIFY_RULE_HPP
#define TAO_PEGTL_SRC_TEST_PEGTL_VERIFY_RULE_HPP

#include <cstdlib>
#include <string>

#include <tao/pegtl/eol.hpp>
#include <tao/pegtl/memory_input.hpp>
#include <tao/pegtl/tracking_mode.hpp>
#include <tao/pegtl/type_list.hpp>

#include "result_type.hpp"
#include "verify_impl.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   template< typename Rule >
   struct verify_action_impl
   {
      template< typename ActionInput, typename... States >
      static void apply( const ActionInput& /*unused*/, States&&... /*unused*/ )
      {}
   };

   template< typename Rule >
   struct verify_action_impl0
   {
      template< typename... States >
      static void apply0( States&&... /*unused*/ )
      {}
   };

   template< typename Rule, typename Eol = eol::lf_crlf >
   void verify_rule( const std::size_t line, const char* file, const std::string& data, const result_type expected, int remain = -1 )
   {
      if( remain < 0 ) {
         remain = ( expected == result_type::success ) ? 0 : int( data.size() );
      }
      {
         memory_input< tracking_mode::eager, Eol > in( data.data(), data.data() + data.size(), file, 0, line, 1 );
         verify_impl_one< Rule, nothing >( line, file, data, in, expected, remain );
         memory_input< tracking_mode::lazy, Eol > i2( data.data(), data.data() + data.size(), file );
         verify_impl_one< Rule, nothing >( line, file, data, i2, expected, remain );
      }
      {
         memory_input< tracking_mode::eager, Eol > in( data.data(), data.data() + data.size(), file, 0, line, 1 );
         verify_impl_one< Rule, verify_action_impl >( line, file, data, in, expected, remain );
         memory_input< tracking_mode::lazy, Eol > i2( data.data(), data.data() + data.size(), file );
         verify_impl_one< Rule, verify_action_impl >( line, file, data, i2, expected, remain );
      }
      {
         memory_input< tracking_mode::eager, Eol > in( data.data(), data.data() + data.size(), file, 0, line, 1 );
         verify_impl_one< Rule, verify_action_impl0 >( line, file, data, in, expected, remain );
         memory_input< tracking_mode::lazy, Eol > i2( data.data(), data.data() + data.size(), file );
         verify_impl_one< Rule, verify_action_impl0 >( line, file, data, i2, expected, remain );
      }
   }

   template< typename Rule, typename Eol = eol::lf_crlf >
   void verify_only( const std::size_t line, const char* file, const std::string& data, const result_type expected, const std::size_t remain )
   {
      {
         memory_input< tracking_mode::eager, Eol > in( data.data(), data.data() + data.size(), file, 0, line, 1 );
         verify_impl_one< Rule, nothing >( line, file, data, in, expected, remain );
      }
      {
         memory_input< tracking_mode::eager, Eol > in( data.data(), data.data() + data.size(), file, 0, line, 1 );
         verify_impl_one< Rule, verify_action_impl >( line, file, data, in, expected, remain );
      }
      {
         memory_input< tracking_mode::eager, Eol > in( data.data(), data.data() + data.size(), file, 0, line, 1 );
         verify_impl_one< Rule, verify_action_impl0 >( line, file, data, in, expected, remain );
      }
   }

}  // namespace TAO_PEGTL_NAMESPACE

#endif
