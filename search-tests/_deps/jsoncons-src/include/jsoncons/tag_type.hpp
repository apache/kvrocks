// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_TAG_TYPE_HPP
#define JSONCONS_TAG_TYPE_HPP

#include <cstdint>
#include <ostream>

#include <jsoncons/config/jsoncons_config.hpp>

namespace jsoncons {

struct null_type
{
    explicit null_type() = default; 
};

constexpr null_type null_arg{};

struct temp_allocator_arg_t
{
    explicit temp_allocator_arg_t() = default; 
};

constexpr temp_allocator_arg_t temp_allocator_arg{};

struct half_arg_t
{
    explicit half_arg_t() = default; 
};

constexpr half_arg_t half_arg{};

struct json_array_arg_t
{
    explicit json_array_arg_t() = default; 
};

constexpr json_array_arg_t json_array_arg{};

struct json_object_arg_t
{
    explicit json_object_arg_t() = default; 
};

constexpr json_object_arg_t json_object_arg{};

struct byte_string_arg_t
{
    explicit byte_string_arg_t() = default; 
};

constexpr byte_string_arg_t byte_string_arg{};

struct json_const_pointer_arg_t
{
    explicit json_const_pointer_arg_t() = default; 
};

constexpr json_const_pointer_arg_t json_const_pointer_arg{};

struct json_pointer_arg_t
{
    explicit json_pointer_arg_t() = default; 
};

constexpr json_pointer_arg_t json_pointer_arg{};
 
enum class semantic_tag : uint8_t 
{
    none = 0,                  // 00000000     
    undefined = 1,             // 00000001
    datetime = 2,              // 00000010
    epoch_second = 3,          // 00000011
    epoch_milli = 4,           // 00000100
    epoch_nano = 5,            // 00000101
    base16 = 6,                // 00000110
    base64 = 7,                // 00000111
    base64url = 8,             // 00001000
    uri = 9,
    multi_dim_row_major = 10,
    multi_dim_column_major = 11,
    bigint = 12,                // 00001100
    bigdec = 13,                // 00001101
    bigfloat = 14,              // 00001110
    float128 = 15,              // 00001111
    clamped = 16,
    ext = 17,
    id = 18,
    regex = 19,
    code = 20
};

inline bool is_number_tag(semantic_tag tag) noexcept
{
    static const uint8_t mask{ uint8_t(semantic_tag::bigint) & uint8_t(semantic_tag::bigdec) 
        & uint8_t(semantic_tag::bigfloat) & uint8_t(semantic_tag::float128) };
    return (uint8_t(tag) & mask) == mask;
}

template <typename CharT>
std::basic_ostream<CharT>& operator<<(std::basic_ostream<CharT>& os, semantic_tag tag)
{
    static constexpr const CharT* na_name = JSONCONS_CSTRING_CONSTANT(CharT, "n/a");
    static constexpr const CharT* undefined_name = JSONCONS_CSTRING_CONSTANT(CharT, "undefined");
    static constexpr const CharT* datetime_name = JSONCONS_CSTRING_CONSTANT(CharT, "datetime");
    static constexpr const CharT* epoch_second_name = JSONCONS_CSTRING_CONSTANT(CharT, "epoch-second");
    static constexpr const CharT* epoch_milli_name = JSONCONS_CSTRING_CONSTANT(CharT, "epoch-milli");
    static constexpr const CharT* epoch_nano_name = JSONCONS_CSTRING_CONSTANT(CharT, "epoch-nano");
    static constexpr const CharT* bigint_name = JSONCONS_CSTRING_CONSTANT(CharT, "bigint");
    static constexpr const CharT* bigdec_name = JSONCONS_CSTRING_CONSTANT(CharT, "bigdec");
    static constexpr const CharT* bigfloat_name = JSONCONS_CSTRING_CONSTANT(CharT, "bigfloat");
    static constexpr const CharT* base16_name = JSONCONS_CSTRING_CONSTANT(CharT, "base16");
    static constexpr const CharT* base64_name = JSONCONS_CSTRING_CONSTANT(CharT, "base64");
    static constexpr const CharT* base64url_name = JSONCONS_CSTRING_CONSTANT(CharT, "base64url");
    static constexpr const CharT* uri_name = JSONCONS_CSTRING_CONSTANT(CharT, "uri");
    static constexpr const CharT* clamped_name = JSONCONS_CSTRING_CONSTANT(CharT, "clamped");
    static constexpr const CharT* multi_dim_row_major_name = JSONCONS_CSTRING_CONSTANT(CharT, "multi-dim-row-major");
    static constexpr const CharT* multi_dim_column_major_name = JSONCONS_CSTRING_CONSTANT(CharT, "multi-dim-column-major");
    static constexpr const CharT* ext_name = JSONCONS_CSTRING_CONSTANT(CharT, "ext");
    static constexpr const CharT* id_name = JSONCONS_CSTRING_CONSTANT(CharT, "id");
    static constexpr const CharT*  float128_name = JSONCONS_CSTRING_CONSTANT(CharT, "float128");
    static constexpr const CharT*  regex_name = JSONCONS_CSTRING_CONSTANT(CharT, "regex");
    static constexpr const CharT*  code_name = JSONCONS_CSTRING_CONSTANT(CharT, "code");

    switch (tag)
    {
        case semantic_tag::none:
        {
            os << na_name;
            break;
        }
        case semantic_tag::undefined:
        {
            os << undefined_name;
            break;
        }
        case semantic_tag::datetime:
        {
            os << datetime_name;
            break;
        }
        case semantic_tag::epoch_second:
        {
            os << epoch_second_name;
            break;
        }
        case semantic_tag::epoch_milli:
        {
            os << epoch_milli_name;
            break;
        }
        case semantic_tag::epoch_nano:
        {
            os << epoch_nano_name;
            break;
        }
        case semantic_tag::bigint:
        {
            os << bigint_name;
            break;
        }
        case semantic_tag::bigdec:
        {
            os << bigdec_name;
            break;
        }
        case semantic_tag::bigfloat:
        {
            os << bigfloat_name;
            break;
        }
        case semantic_tag::float128:
        {
            os << float128_name;
            break;
        }
        case semantic_tag::base16:
        {
            os << base16_name;
            break;
        }
        case semantic_tag::base64:
        {
            os << base64_name;
            break;
        }
        case semantic_tag::base64url:
        {
            os << base64url_name;
            break;
        }
        case semantic_tag::uri:
        {
            os << uri_name;
            break;
        }
        case semantic_tag::clamped:
        {
            os << clamped_name;
            break;
        }
        case semantic_tag::multi_dim_row_major:
        {
            os << multi_dim_row_major_name;
            break;
        }
        case semantic_tag::multi_dim_column_major:
        {
            os << multi_dim_column_major_name;
            break;
        }
        case semantic_tag::ext:
        {
            os << ext_name;
            break;
        }
        case semantic_tag::id:
        {
            os << id_name;
            break;
        }
        case semantic_tag::regex:
        {
            os << regex_name;
            break;
        }
        case semantic_tag::code:
        {
            os << code_name;
            break;
        }
    }
    return os;
}

} // namespace jsoncons

#endif // JSONCONS_TAG_TYPE_HPP
