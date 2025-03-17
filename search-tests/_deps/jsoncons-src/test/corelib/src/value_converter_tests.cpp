// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/utility/extension_traits.hpp>
#include <jsoncons/value_converter.hpp>
#include <vector>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("convert into string")
{
    std::vector<uint8_t> bytes = {'f','o','o','b','a','r'};

    SECTION("from byte_string into string")
    {
        value_converter<byte_string_view,std::string> converter;

        std::string expected = "Zm9vYmFy";

        std::error_code ec;
        std::string s = converter.convert(byte_string_view(bytes), semantic_tag::base64url, ec);
        REQUIRE(!ec); 
        
        CHECK(expected == s);
    }
    SECTION("from byte string into wstring")
    {
        value_converter<byte_string_view,std::wstring> converter;

        std::wstring expected = L"Zm9vYmFy";

        std::error_code ec;
        std::wstring s = converter.convert(byte_string_view(bytes), semantic_tag::base64url, ec);
        REQUIRE(!ec);

        CHECK(expected == s);
    }
}

TEST_CASE("convert into list-like")
{
    SECTION("from string")
    {
        value_converter<jsoncons::string_view, std::vector<uint8_t>> converter;

        std::vector<uint8_t> expected = {'f','o','o','b','a','r'};

        std::error_code ec;
        std::vector<uint8_t> v = converter.convert(jsoncons::string_view("Zm9vYmFy"), semantic_tag::base64url, ec);
        REQUIRE(!ec); 
        
        CHECK(expected == v);
    }
}

