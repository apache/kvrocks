// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#ifndef TAO_PEGTL_SRC_EXAMPLES_PEGTL_JSON_ERRORS_HPP
#define TAO_PEGTL_SRC_EXAMPLES_PEGTL_JSON_ERRORS_HPP

#include <tao/pegtl.hpp>
#include <tao/pegtl/contrib/json.hpp>

namespace pegtl = TAO_PEGTL_NAMESPACE;

namespace example
{
   // This file shows how to throw exceptions with
   // custom error messages for parse errors.

#if defined( __cpp_exceptions )

   // clang-format off
   template< typename > inline constexpr const char* error_message = nullptr;

   template<> inline constexpr auto error_message< pegtl::json::text > = "no valid JSON";

   template<> inline constexpr auto error_message< pegtl::json::next_array_element > = "expected value";
   template<> inline constexpr auto error_message< pegtl::json::end_array > = "incomplete array, expected ']'";

   template<> inline constexpr auto error_message< pegtl::json::name_separator > = "expected ':'";
   template<> inline constexpr auto error_message< pegtl::json::member_value > = "expected value";
   template<> inline constexpr auto error_message< pegtl::json::next_member > = "expected member";
   template<> inline constexpr auto error_message< pegtl::json::end_object > = "incomplete object, expected '}'";

   template<> inline constexpr auto error_message< pegtl::json::digits > = "expected at least one digit";
   template<> inline constexpr auto error_message< pegtl::json::xdigit > = "incomplete universal character name";
   template<> inline constexpr auto error_message< pegtl::json::escaped > = "unknown escape sequence";
   template<> inline constexpr auto error_message< pegtl::json::char_ > = "invalid character in string";
   template<> inline constexpr auto error_message< pegtl::json::string::content > = "unterminated string";
   template<> inline constexpr auto error_message< pegtl::json::key::content > = "unterminated key";

   template<> inline constexpr auto error_message< pegtl::eof > = "unexpected character after JSON value";
   // clang-format on

   // As must_if<> can not take error_message as a template parameter directly, we need to wrap it.
   struct error
   {
      template< typename Rule >
      static constexpr auto message = error_message< Rule >;
   };

   template< typename Rule >
   using control = pegtl::must_if< error >::control< Rule >;

#else

   template< typename Rule >
   using control = pegtl::normal< Rule >;

#endif

}  // namespace example

#endif
