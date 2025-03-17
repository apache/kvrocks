// Copyright (c) 2017-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_meta< require< 0 >, internal::success >();
      verify_meta< require< 1 >, internal::require< 1 > >();

      verify_analyze< require< 0 > >( __LINE__, __FILE__, false, false );
      verify_analyze< require< 1 > >( __LINE__, __FILE__, false, false );
      verify_analyze< require< 9 > >( __LINE__, __FILE__, false, false );

      verify_rule< require< 0 > >( __LINE__, __FILE__, "", result_type::success, 0 );
      verify_rule< require< 0 > >( __LINE__, __FILE__, "a", result_type::success, 1 );
      verify_rule< require< 0 > >( __LINE__, __FILE__, "  ", result_type::success, 2 );
      verify_rule< require< 1 > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< require< 1 > >( __LINE__, __FILE__, "a", result_type::success, 1 );
      verify_rule< require< 1 > >( __LINE__, __FILE__, "  ", result_type::success, 2 );
      verify_rule< require< 9 > >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< require< 9 > >( __LINE__, __FILE__, "1", result_type::local_failure, 1 );
      verify_rule< require< 9 > >( __LINE__, __FILE__, "12", result_type::local_failure, 2 );
      verify_rule< require< 9 > >( __LINE__, __FILE__, "123", result_type::local_failure, 3 );
      verify_rule< require< 9 > >( __LINE__, __FILE__, "1234", result_type::local_failure, 4 );
      verify_rule< require< 9 > >( __LINE__, __FILE__, "12345", result_type::local_failure, 5 );
      verify_rule< require< 9 > >( __LINE__, __FILE__, "123456", result_type::local_failure, 6 );
      verify_rule< require< 9 > >( __LINE__, __FILE__, "1234567", result_type::local_failure, 7 );
      verify_rule< require< 9 > >( __LINE__, __FILE__, "12345678", result_type::local_failure, 8 );
      verify_rule< require< 9 > >( __LINE__, __FILE__, "123456789", result_type::success, 9 );
      verify_rule< require< 9 > >( __LINE__, __FILE__, "123456789123456789", result_type::success, 18 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
