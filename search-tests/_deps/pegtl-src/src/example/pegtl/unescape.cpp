// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <iostream>

#include <tao/pegtl.hpp>
#include <tao/pegtl/contrib/unescape.hpp>

using namespace TAO_PEGTL_NAMESPACE;

namespace example
{
   // Grammar for string literals with some escape sequences from the C language:
   // - \x followed by two hex-digits to insert any byte value.
   // - \u followed by four hex-digits to insert a Unicode code point.
   // - \U followed by eight hex-digits to insert any Unicode code points.
   // - A backslash followed by one of the characters listed in the grammar below.

   // clang-format off
   struct escaped_x : seq< one< 'x' >, rep< 2, xdigit > > {};
   struct escaped_u : seq< one< 'u' >, rep< 4,  xdigit > > {};
   struct escaped_U : seq< one< 'U' >, rep< 8,  xdigit > > {};
   struct escaped_c : one< '\'', '"', '?', '\\', 'a', 'b', 'f', 'n', 'r', 't', 'v' > {};

   struct escaped : sor< escaped_x,
                         escaped_u,
                         escaped_U,
                         escaped_c > {};

   struct character : if_then_else< one< '\\' >, escaped, utf8::range< 0x20, 0x10FFFF > > {};
   struct literal : seq< one< '"' >, until< one< '"' >, character > > {};

   struct padded : seq< pad< literal, blank >, eof > {};

   // Action class that uses the actions from tao/pegtl/contrib/unescape.hpp to
   // produce a UTF-8 encoded result string where all escape sequences are
   // replaced with their intended meaning.

   template< typename Rule > struct action {};

   template<> struct action< utf8::range< 0x20, 0x10FFFF > > : unescape::append_all {};
   template<> struct action< escaped_x > : unescape::unescape_x {};
   template<> struct action< escaped_u > : unescape::unescape_u {};
   template<> struct action< escaped_U > : unescape::unescape_u {};
   template<> struct action< escaped_c > : unescape::unescape_c< escaped_c, '\'', '"', '?', '\\', '\a', '\b', '\f', '\n', '\r', '\t', '\v' > {};
   // clang-format on

}  // namespace example

int main( int argc, char** argv )  // NOLINT(bugprone-exception-escape)
{
   for( int i = 1; i < argc; ++i ) {
      std::string s;
      argv_input in( argv, i );
      if( parse< example::padded, example::action >( in, s ) ) {
         std::cout << "argv[ " << i << " ] = " << s << std::endl;
      }
      else {
         std::cerr << "error parsing: " << argv[ i ] << std::endl;
      }
   }
   return 0;
}
