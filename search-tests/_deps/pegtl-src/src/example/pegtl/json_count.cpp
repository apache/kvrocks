// Copyright (c) 2017-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <cstddef>
#include <iomanip>
#include <iostream>
#include <map>
#include <string_view>

#include <tao/pegtl.hpp>

#include <tao/pegtl/contrib/json.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   struct counter_data
   {
      std::size_t start = 0;
      std::size_t success = 0;
      std::size_t failure = 0;
   };

   struct counter_state
   {
      std::map< std::string_view, counter_data > counts;
   };

   template< typename Rule >
   struct counter
      : normal< Rule >
   {
      template< typename Input >
      static void start( const Input& /*unused*/, counter_state& ts )
      {
         ++ts.counts[ demangle< Rule >() ].start;
      }

      template< typename Input >
      static void success( const Input& /*unused*/, counter_state& ts )
      {
         ++ts.counts[ demangle< Rule >() ].success;
      }

      template< typename Input >
      static void failure( const Input& /*unused*/, counter_state& ts )
      {
         ++ts.counts[ demangle< Rule >() ].failure;
      }
   };

}  // namespace TAO_PEGTL_NAMESPACE

using namespace TAO_PEGTL_NAMESPACE;

using grammar = seq< json::text, eof >;

int main( int argc, char** argv )  // NOLINT(bugprone-exception-escape)
{
   counter_state cs;

   for( int i = 1; i < argc; ++i ) {
      file_input in( argv[ i ] );
      parse< grammar, nothing, counter >( in, cs );
   }
   std::cout << std::right << std::setw( 72 ) << "RULE NAME" << std::left << "      START  SUCCESS  FAILURE" << std::endl;
   for( const auto& j : cs.counts ) {
      std::cout << std::right << std::setw( 72 ) << j.first << "   " << std::setw( 8 ) << j.second.start << " " << std::setw( 8 ) << j.second.success << " " << std::setw( 8 ) << j.second.failure << std::endl;
   }
   return 0;
}
