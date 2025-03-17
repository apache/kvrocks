/// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_CSV_CSV_ERROR_HPP
#define JSONCONS_EXT_CSV_CSV_ERROR_HPP

#include <string>
#include <system_error>
#include <type_traits>

namespace jsoncons { namespace csv {

    enum class csv_errc : int
    {
        success = 0,
        unexpected_eof = 1,
        source_error,
        expected_quote,
        syntax_error,
        invalid_parse_state,
        invalid_escaped_char,
        unexpected_char_between_fields,
        max_nesting_depth_exceeded
    };

class csv_error_category_impl
   : public std::error_category
{
public:
    const char* name() const noexcept override
    {
        return "jsoncons/csv";
    }
    std::string message(int ev) const override
    {
        switch (static_cast<csv_errc>(ev))
        {
            case csv_errc::unexpected_eof:
                return "Unexpected end of file";
            case csv_errc::source_error:
                return "Source error";
            case csv_errc::expected_quote:
                return "Expected quote character";
            case csv_errc::syntax_error:
                return "CSV syntax error";
            case csv_errc::invalid_parse_state:
                return "Invalid CSV parser state";
            case csv_errc::invalid_escaped_char:
                return "Invalid character following quote escape character";
            case csv_errc::unexpected_char_between_fields:
                return "Unexpected character between fields";
            case csv_errc::max_nesting_depth_exceeded:
                return "Data item nesting exceeds limit in options";
            default:
                return "Unknown CSV parser error";
        }
    }
};

inline
const std::error_category& csv_error_category()
{
  static csv_error_category_impl instance;
  return instance;
}

inline 
std::error_code make_error_code(csv_errc result)
{
    return std::error_code(static_cast<int>(result),csv_error_category());
}

} // namespace jsonpath
} // namespace jsoncons

namespace std {
    template<>
    struct is_error_code_enum<jsoncons::csv::csv_errc> : public true_type
    {
    };
} // namespace std

#endif // JSONCONS_EXT_CSV_CSV_ERROR_HPP
