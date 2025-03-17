// Copyright (c) 2017-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include <tao/pegtl/demangle.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   template< typename T >
   void test( const std::string& s )
   {
      TAO_PEGTL_TEST_ASSERT( demangle< T >() == s );
   }

   void unit_test()
   {
#if !defined( __clang__ ) && defined( __GNUC__ ) && ( __GNUC__ == 9 ) && ( __GNUC_MINOR__ <= 2 )
      // see https://gcc.gnu.org/bugzilla/show_bug.cgi?id=91155
      test< int >( "i" );
      test< double >( "d" );
      test< seq< bytes< 42 >, eof > >( "N3tao5pegtl3seqIJNS0_5bytesILj42EEENS0_3eofEEEE" );
#elif defined( _MSC_VER ) && !defined( __clang__ )
      test< int >( "int" );
      test< double >( "double" );
      // in the Microsoft world, class and struct are not the same!
      test< seq< bytes< 42 >, eof > >( "struct tao::pegtl::seq<struct tao::pegtl::bytes<42>,struct tao::pegtl::eof>" );
#else
      test< int >( "int" );
      test< double >( "double" );
      test< seq< bytes< 42 >, eof > >( "tao::pegtl::seq<tao::pegtl::bytes<42>, tao::pegtl::eof>" );
#endif
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
