// Copyright (c) 2020-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <iostream>

#include <tao/pegtl/contrib/json.hpp>
#include <tao/pegtl/contrib/print.hpp>

int main()  // NOLINT(bugprone-exception-escape)
{
   tao::pegtl::print_names< tao::pegtl::json::text >( std::cout );
   return 0;
}
