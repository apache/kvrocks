// Copyright (c) 2020-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <iostream>

#include <tao/pegtl.hpp>
#include <tao/pegtl/contrib/analyze.hpp>
#include <tao/pegtl/contrib/json.hpp>

namespace pegtl = TAO_PEGTL_NAMESPACE;

namespace example
{
   using grammar = pegtl::seq< pegtl::json::text, pegtl::eof >;

}  // namespace example

int main()  // NOLINT(bugprone-exception-escape)
{
   if( pegtl::analyze< example::grammar >() != 0 ) {
      std::cerr << "cycles without progress detected!" << std::endl;
      return 1;
   }
   return 0;
}
