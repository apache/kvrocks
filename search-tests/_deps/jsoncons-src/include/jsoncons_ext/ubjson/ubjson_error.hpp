/// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_UBJSON_UBJSON_ERROR_HPP
#define JSONCONS_EXT_UBJSON_UBJSON_ERROR_HPP

#include <string>
#include <system_error>
#include <type_traits>

#include <jsoncons/config/jsoncons_config.hpp>

namespace jsoncons { namespace ubjson {

enum class ubjson_errc
{
    success = 0,
    unexpected_eof = 1,
    source_error,
    count_required_after_type,
    length_is_negative,
    length_must_be_integer,
    unknown_type,
    invalid_utf8_text_string,
    too_many_items,
    too_few_items,
    number_too_large,
    max_nesting_depth_exceeded,
    key_expected,
    max_items_exceeded
};

class ubjson_error_category_impl
   : public std::error_category
{
public:
    const char* name() const noexcept override
    {
        return "jsoncons/ubjson";
    }
    std::string message(int ev) const override
    {
        switch (static_cast<ubjson_errc>(ev))
        {
            case ubjson_errc::unexpected_eof:
                return "Unexpected end of file";
            case ubjson_errc::source_error:
                return "Source error";
            case ubjson_errc::count_required_after_type:
                return "Type is specified for container, but count is not specified";
            case ubjson_errc::length_is_negative:
                return "Request for the length of an array, map or string returned a negative result";
            case ubjson_errc::length_must_be_integer:
                return "Length must be a integer numeric type (int8, uint8, int16, int32, int64)";
            case ubjson_errc::unknown_type:
                return "Unknown type";
            case ubjson_errc::invalid_utf8_text_string:
                return "Illegal UTF-8 encoding in text string";
            case ubjson_errc::too_many_items:
                return "Too many items were added to a UBJSON object or array of known length";
            case ubjson_errc::too_few_items:
                return "Too few items were added to a UBJSON object or array of known length";
            case ubjson_errc::number_too_large:
                return "Number exceeds implementation limits";
            case ubjson_errc::max_nesting_depth_exceeded:
                return "Data item nesting exceeds limit in options";
            case ubjson_errc::key_expected:
                return "Text string key in a map expected";
            case ubjson_errc::max_items_exceeded:
                return "Number of items in UBJSON object or array exceeds limit set in options";
            default:
                return "Unknown UBJSON parser error";
        }
    }
};

inline
const std::error_category& ubjson_error_category()
{
  static ubjson_error_category_impl instance;
  return instance;
}

inline 
std::error_code make_error_code(ubjson_errc e)
{
    return std::error_code(static_cast<int>(e),ubjson_error_category());
}


} // namespace ubjson
} // namespace jsoncons

namespace std {
    template<>
    struct is_error_code_enum<jsoncons::ubjson::ubjson_errc> : public true_type
    {
    };
} // namespace std

#endif // JSONCONS_EXT_UBJSON_UBJSON_ERROR_HPP
