// Copyright (c) 2015-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"

#include <tao/pegtl/contrib/alphabet.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      static_assert( alphabet::a == 'a' );
      static_assert( alphabet::b == 'b' );
      static_assert( alphabet::c == 'c' );
      static_assert( alphabet::d == 'd' );
      static_assert( alphabet::e == 'e' );
      static_assert( alphabet::f == 'f' );
      static_assert( alphabet::g == 'g' );
      static_assert( alphabet::h == 'h' );
      static_assert( alphabet::i == 'i' );
      static_assert( alphabet::j == 'j' );
      static_assert( alphabet::k == 'k' );
      static_assert( alphabet::l == 'l' );
      static_assert( alphabet::m == 'm' );
      static_assert( alphabet::n == 'n' );
      static_assert( alphabet::o == 'o' );
      static_assert( alphabet::p == 'p' );
      static_assert( alphabet::q == 'q' );
      static_assert( alphabet::r == 'r' );
      static_assert( alphabet::s == 's' );
      static_assert( alphabet::t == 't' );
      static_assert( alphabet::u == 'u' );
      static_assert( alphabet::v == 'v' );
      static_assert( alphabet::w == 'w' );
      static_assert( alphabet::x == 'x' );
      static_assert( alphabet::y == 'y' );
      static_assert( alphabet::z == 'z' );

      static_assert( alphabet::A == 'A' );
      static_assert( alphabet::B == 'B' );
      static_assert( alphabet::C == 'C' );
      static_assert( alphabet::D == 'D' );
      static_assert( alphabet::E == 'E' );
      static_assert( alphabet::F == 'F' );
      static_assert( alphabet::G == 'G' );
      static_assert( alphabet::H == 'H' );
      static_assert( alphabet::I == 'I' );
      static_assert( alphabet::J == 'J' );
      static_assert( alphabet::K == 'K' );
      static_assert( alphabet::L == 'L' );
      static_assert( alphabet::M == 'M' );
      static_assert( alphabet::N == 'N' );
      static_assert( alphabet::O == 'O' );
      static_assert( alphabet::P == 'P' );
      static_assert( alphabet::Q == 'Q' );
      static_assert( alphabet::R == 'R' );
      static_assert( alphabet::S == 'S' );
      static_assert( alphabet::T == 'T' );
      static_assert( alphabet::U == 'U' );
      static_assert( alphabet::V == 'V' );
      static_assert( alphabet::W == 'W' );
      static_assert( alphabet::X == 'X' );
      static_assert( alphabet::Y == 'Y' );
      static_assert( alphabet::Z == 'Z' );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
