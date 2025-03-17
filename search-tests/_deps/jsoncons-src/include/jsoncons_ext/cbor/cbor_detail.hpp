// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_CBOR_CBOR_DETAIL_HPP
#define JSONCONS_EXT_CBOR_CBOR_DETAIL_HPP

#include <cstddef>
#include <cstdint>

#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/json_visitor.hpp>

// 0x00..0x17 (0..23)
#define JSONCONS_EXT_CBOR_0x00_0x17 \
    0x00:case 0x01:case 0x02:case 0x03:case 0x04:case 0x05:case 0x06:case 0x07:case 0x08:case 0x09:case 0x0a:case 0x0b:case 0x0c:case 0x0d:case 0x0e:case 0x0f:case 0x10:case 0x11:case 0x12:case 0x13:case 0x14:case 0x15:case 0x16:case 0x17

#define JSONCONS_EXT_CBOR_ARRAY_TAGS \
    0x40:case 0x41:case 0x42:case 0x43:case 0x44:case 0x45:case 0x46:case 0x47:case 0x48:case 0x49:case 0x4a:case 0x4b:case 0x4c:case 0x4d:case 0x4e:case 0x4f:case 0x50:case 0x51:case 0x52:case 0x53:case 0x54:case 0x55:case 0x56:case 0x57    

namespace jsoncons { namespace cbor { namespace detail {

//const uint8_t cbor_array_tags_010_mask = 0b11100000;
//const uint8_t cbor_array_tags_f_mask = 0b00010000;
//const uint8_t cbor_array_tags_s_mask = 0b00001000;
//const uint8_t cbor_array_tags_e_mask = 0b00000100;
//const uint8_t cbor_array_tags_ll_mask = 0b00000011;

const uint8_t cbor_array_tags_010_mask = 0xE0;
const uint8_t cbor_array_tags_f_mask = 0x10;
const uint8_t cbor_array_tags_s_mask = 0x08;
const uint8_t cbor_array_tags_e_mask = 0x04;
const uint8_t cbor_array_tags_ll_mask = 0x03;

const uint8_t cbor_array_tags_010_shift = 5;
const uint8_t cbor_array_tags_f_shift = 4;
const uint8_t cbor_array_tags_s_shift = 3;
const uint8_t cbor_array_tags_e_shift = 2;
const uint8_t cbor_array_tags_ll_shift = 0;

enum class cbor_major_type : uint8_t
{
    unsigned_integer = 0x00,
    negative_integer = 0x01,
    byte_string = 0x02,
    text_string = 0x03,
    array = 0x04,
    map = 0x05,   
    semantic_tag = 0x06,
    simple = 0x7
};

namespace additional_info
{
    const uint8_t indefinite_length = 0x1f;
}

inline
size_t min_length_for_stringref(uint64_t index)
{
    std::size_t n;
    if (index <= 23)
    {
        n = 3;
    }
    else if (index <= 255)
    {
        n = 4;
    }
    else if (index <= 65535)
    {
        n = 5;
    }
    else if (index <= 4294967295)
    {
        n = 7;
    }
    else 
    {
        n = 11;
    }
    return n;
}

} // namespace detail 
} // namespace cbor
} // namespace jsoncons

#endif
