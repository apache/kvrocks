// Copyright (c) 2021-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <tao/pegtl/contrib/separated_seq.hpp>

#include <type_traits>

// clang-format off
struct A {};
struct B {};
struct C {};
struct D {};

struct S {};
// clang-format on

using namespace TAO_PEGTL_NAMESPACE;
static_assert( std::is_base_of_v< internal::seq<>, separated_seq< S > > );
static_assert( std::is_base_of_v< internal::seq< A >, separated_seq< S, A > > );
static_assert( std::is_base_of_v< internal::seq< A, S, B >, separated_seq< S, A, B > > );
static_assert( std::is_base_of_v< internal::seq< A, S, B, S, C, S, D >, separated_seq< S, A, B, C, D > > );

int main()
{}
