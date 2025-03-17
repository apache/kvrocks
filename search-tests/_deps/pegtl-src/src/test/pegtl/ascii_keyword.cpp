// Copyright (c) 2017-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_analyze< keyword< 'f', 'o', 'o' > >( __LINE__, __FILE__, true, false );

      verify_rule< keyword< 'f', 'o', 'o' > >( __LINE__, __FILE__, "foo", result_type::success, 0 );
      verify_rule< keyword< 'f', 'o', 'o' > >( __LINE__, __FILE__, "foo ", result_type::success, 1 );
      verify_rule< keyword< 'f', 'o', 'o' > >( __LINE__, __FILE__, "foo foo", result_type::success, 4 );
      verify_rule< keyword< 'f', 'o', 'o' > >( __LINE__, __FILE__, "FOO", result_type::local_failure, 3 );
      verify_rule< keyword< 'f', 'o', 'o' > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< keyword< 'f', 'o', 'o' > >( __LINE__, __FILE__, "f", result_type::local_failure, 1 );
      verify_rule< keyword< 'f', 'o', 'o' > >( __LINE__, __FILE__, "fo", result_type::local_failure, 2 );
      verify_rule< keyword< 'f', 'o', 'o' > >( __LINE__, __FILE__, " foo", result_type::local_failure, 4 );
      verify_rule< keyword< 'f', 'o', 'o' > >( __LINE__, __FILE__, "foo_", result_type::local_failure, 4 );
      verify_rule< keyword< 'f', 'o', 'o' > >( __LINE__, __FILE__, "foo1", result_type::local_failure, 4 );
      verify_rule< keyword< 'f', 'o', 'o' > >( __LINE__, __FILE__, "fooa", result_type::local_failure, 4 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
