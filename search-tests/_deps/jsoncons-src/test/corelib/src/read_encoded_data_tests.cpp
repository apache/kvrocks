// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_reader.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <catch/catch.hpp>

using namespace jsoncons;

template <typename Source>
typename std::enable_if<extension_traits::is_byte<typename Source::value_type>::value,std::size_t>::type
read_json(Source& source, char* buffer, std::size_t capacity, unicode_traits::encoding_kind& encoding)
{
    using value_type = typename Source::value_type;

    std::size_t count = 0;
    char* ptr = buffer;
    if (encoding != unicode_traits::encoding_kind::undetected)
    {
        count = source.read(buffer, capacity);
        auto r = unicode_traits::detect_json_encoding(buffer,capacity);
        encoding = r.encoding;
        count -= (r.ptr - buffer);
        ptr = r.ptr;
    }
    switch (encoding)
    {
        case unicode_traits::encoding_kind::utf8:
            break;
        case unicode_traits::encoding_kind::utf16le:
            break;
        case unicode_traits::encoding_kind::utf16be:
            break;
        case unicode_traits::encoding_kind::utf32le:
            break;
        case unicode_traits::encoding_kind::utf32be:
            break;
    }
}

TEST_CASE("Read utf8 encoded data")
{
    SECTION("utf8, no bom")
    {
        std::string input = "[1,2,3]";

        auto r = jsoncons::unicode_traits::detect_json_encoding(input.data(),input.size());
        CHECK(r.encoding == jsoncons::unicode_traits::encoding_kind::utf8);
        CHECK(r.ptr == input.data());
    }
}

