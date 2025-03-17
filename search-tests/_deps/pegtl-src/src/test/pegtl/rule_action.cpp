// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include "verify_meta.hpp"
#include "verify_seqs.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   template< typename... Rules >
   using test_action_rule = action< nothing, Rules... >;

   void unit_test()
   {
      verify_meta< action< nothing >, internal::success >();
      verify_meta< action< nothing, eof >, internal::action< nothing, eof >, eof >();
      verify_meta< action< nothing, eof, any >, internal::action< nothing, internal::seq< eof, any > >, internal::seq< eof, any > >();

      verify_seqs< test_action_rule >();
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
