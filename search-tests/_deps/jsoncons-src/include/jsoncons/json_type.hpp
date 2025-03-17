// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSON_TYPE_HPP
#define JSONCONS_JSON_TYPE_HPP

#include <cstdint>
#include <ostream>

#include <jsoncons/config/jsoncons_config.hpp>

namespace jsoncons {

    enum class json_type : uint8_t 
    {
        null_value,
        bool_value,
        int64_value,
        uint64_value,
        half_value,
        double_value,
        string_value,
        byte_string_value,
        array_value,
        object_value
    };

    template <typename CharT>
    std::basic_ostream<CharT>& operator<<(std::basic_ostream<CharT>& os, json_type type)
    {
        static constexpr const CharT* null_value = JSONCONS_CSTRING_CONSTANT(CharT, "null");
        static constexpr const CharT* bool_value = JSONCONS_CSTRING_CONSTANT(CharT, "bool");
        static constexpr const CharT* int64_value = JSONCONS_CSTRING_CONSTANT(CharT, "int64");
        static constexpr const CharT* uint64_value = JSONCONS_CSTRING_CONSTANT(CharT, "uint64");
        static constexpr const CharT* half_value = JSONCONS_CSTRING_CONSTANT(CharT, "half");
        static constexpr const CharT* double_value = JSONCONS_CSTRING_CONSTANT(CharT, "double");
        static constexpr const CharT* string_value = JSONCONS_CSTRING_CONSTANT(CharT, "string");
        static constexpr const CharT* byte_string_value = JSONCONS_CSTRING_CONSTANT(CharT, "byte_string");
        static constexpr const CharT* array_value = JSONCONS_CSTRING_CONSTANT(CharT, "array");
        static constexpr const CharT* object_value = JSONCONS_CSTRING_CONSTANT(CharT, "object");

        switch (type)
        {
            case json_type::null_value:
            {
                os << null_value;
                break;
            }
            case json_type::bool_value:
            {
                os << bool_value;
                break;
            }
            case json_type::int64_value:
            {
                os << int64_value;
                break;
            }
            case json_type::uint64_value:
            {
                os << uint64_value;
                break;
            }
            case json_type::half_value:
            {
                os << half_value;
                break;
            }
            case json_type::double_value:
            {
                os << double_value;
                break;
            }
            case json_type::string_value:
            {
                os << string_value;
                break;
            }
            case json_type::byte_string_value:
            {
                os << byte_string_value;
                break;
            }
            case json_type::array_value:
            {
                os << array_value;
                break;
            }
            case json_type::object_value:
            {
                os << object_value;
                break;
            }
        }
        return os;
    }

    enum class json_storage_kind : uint8_t 
    {
        null = 0,                 // 0000
        boolean = 1,              // 0001
        int64 = 2,                // 0010
        uint64 = 3,               // 0011
        empty_object = 4,         // 0100
        float64 = 5,              // 0101
        half_float = 6,           // 0110
        short_str = 7,            // 0111
        json_const_reference = 8, // 1000    
        json_reference = 9,       // 1001    
        byte_str = 12,            // 1100  
        object = 13,              // 1101
        array = 14,               // 1110
        long_str = 15             // 1111
    };

    inline bool is_string_storage(json_storage_kind storage_kind) noexcept
    {
        static const uint8_t mask{ uint8_t(json_storage_kind::short_str) & uint8_t(json_storage_kind::long_str) };
        return (uint8_t(storage_kind) & mask) == mask;
    }

    inline bool is_trivial_storage(json_storage_kind storage_kind) noexcept
    {
        static const uint8_t mask{ uint8_t(json_storage_kind::long_str) & uint8_t(json_storage_kind::byte_str) 
            & uint8_t(json_storage_kind::array) & uint8_t(json_storage_kind::object) };
        return (uint8_t(storage_kind) & mask) != mask;
    }

    template <typename CharT>
    std::basic_ostream<CharT>& operator<<(std::basic_ostream<CharT>& os, json_storage_kind storage)
    {
        static constexpr const CharT* null_value = JSONCONS_CSTRING_CONSTANT(CharT, "null");
        static constexpr const CharT* bool_value = JSONCONS_CSTRING_CONSTANT(CharT, "bool");
        static constexpr const CharT* int64_value = JSONCONS_CSTRING_CONSTANT(CharT, "int64");
        static constexpr const CharT* uint64_value = JSONCONS_CSTRING_CONSTANT(CharT, "uint64");
        static constexpr const CharT* half_value = JSONCONS_CSTRING_CONSTANT(CharT, "half");
        static constexpr const CharT* double_value = JSONCONS_CSTRING_CONSTANT(CharT, "double");
        static constexpr const CharT* short_string_value = JSONCONS_CSTRING_CONSTANT(CharT, "short_string");
        static constexpr const CharT* long_string_value = JSONCONS_CSTRING_CONSTANT(CharT, "string");
        static constexpr const CharT* byte_string_value = JSONCONS_CSTRING_CONSTANT(CharT, "byte_string");
        static constexpr const CharT* array_value = JSONCONS_CSTRING_CONSTANT(CharT, "array");
        static constexpr const CharT* empty_object_value = JSONCONS_CSTRING_CONSTANT(CharT, "empty_object");
        static constexpr const CharT* object_value = JSONCONS_CSTRING_CONSTANT(CharT, "object");
        static constexpr const CharT* json_const_reference = JSONCONS_CSTRING_CONSTANT(CharT, "json_const_reference");
        static constexpr const CharT* json_reference = JSONCONS_CSTRING_CONSTANT(CharT, "json_reference");

        switch (storage)
        {
            case json_storage_kind::null:
            {
                os << null_value;
                break;
            }
            case json_storage_kind::boolean:
            {
                os << bool_value;
                break;
            }
            case json_storage_kind::int64:
            {
                os << int64_value;
                break;
            }
            case json_storage_kind::uint64:
            {
                os << uint64_value;
                break;
            }
            case json_storage_kind::half_float:
            {
                os << half_value;
                break;
            }
            case json_storage_kind::float64:
            {
                os << double_value;
                break;
            }
            case json_storage_kind::short_str:
            {
                os << short_string_value;
                break;
            }
            case json_storage_kind::long_str:
            {
                os << long_string_value;
                break;
            }
            case json_storage_kind::byte_str:
            {
                os << byte_string_value;
                break;
            }
            case json_storage_kind::array:
            {
                os << array_value;
                break;
            }
            case json_storage_kind::empty_object:
            {
                os << empty_object_value;
                break;
            }
            case json_storage_kind::object:
            {
                os << object_value;
                break;
            }
            case json_storage_kind::json_const_reference:
            {
                os << json_const_reference;
                break;
            }
            case json_storage_kind::json_reference:
            {
                os << json_reference;
                break;
            }
        }
        return os;
    }

} // namespace jsoncons

#endif // JSONCONS_JSON_TYPE_HPP
