// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include "verify_meta.hpp"
#include "verify_seqs.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_meta< enable<>, internal::success >();
      verify_meta< enable< eof >, internal::enable< eof >, eof >();
      verify_meta< enable< eof, any >, internal::enable< internal::seq< eof, any > >, internal::seq< eof, any > >();

      verify_seqs< enable >();
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
