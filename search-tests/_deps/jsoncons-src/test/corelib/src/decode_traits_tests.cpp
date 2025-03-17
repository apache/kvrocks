// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>

#include <map>
#include <utility>
#include <vector>

#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("decode_traits primitive")
{
    SECTION("is_primitive")
    {
        CHECK(extension_traits::is_primitive<uint64_t>::value);
    }
    SECTION("uint64_t")
    {
        std::string input = R"(1000)";

        json_decoder<json> decoder;
        std::error_code ec;

        json_string_cursor cursor(input);
        auto val = decode_traits<uint64_t,char>::decode(cursor,decoder,ec);

        CHECK(val == 1000);
    }
    SECTION("vector of uint64_t")
    {
        using test_type = std::vector<uint64_t>;

        std::string input = R"([1000,1001,1002])";

        json_decoder<json> decoder;
        std::error_code ec;

        json_string_cursor cursor(input);
        auto val = decode_traits<test_type,char>::decode(cursor,decoder,ec);

        REQUIRE(val.size() == 3);
        CHECK(val[0] == 1000);
        CHECK(val[1] == 1001);
        CHECK(val[2] == 1002);
    }
}

TEST_CASE("decode_traits std::string")
{
    SECTION("is_string")
    {
        CHECK(extension_traits::is_string<std::string>::value);
    }
    SECTION("string")
    {
        std::string input = R"("Hello World")";

        json_decoder<json> decoder;
        std::error_code ec;

        json_string_cursor cursor(input);
        auto val = decode_traits<std::string,char>::decode(cursor,decoder,ec);

        CHECK((val == "Hello World"));
    }
}

TEST_CASE("decode_traits std::pair")
{
    SECTION("std::pair<std::string,std::string>")
    {
        std::string input = R"(["first","second"])";
        using test_type = std::pair<std::string,std::string>;

        json_decoder<json> decoder;
        std::error_code ec;

        json_string_cursor cursor(input);
        auto val = decode_traits<test_type,char>::decode(cursor,decoder,ec);

        CHECK((val == test_type("first","second")));
    }
    SECTION("vector of std::pair<std::string,std::string>")
    {
        std::string input = R"([["first","second"],["one","two"]])";
        using test_type = std::vector<std::pair<std::string,std::string>>;

        json_decoder<json> decoder;
        std::error_code ec;

        json_string_cursor cursor(input);
        auto val = decode_traits<test_type,char>::decode(cursor,decoder,ec);
        REQUIRE_FALSE(ec);

        REQUIRE(val.size() == 2);
        CHECK((val[0] == test_type::value_type("first","second")));
        CHECK((val[1] == test_type::value_type("one","two")));
    }
    SECTION("map of std::string-std::pair<int,double>")
    {
        std::string input = R"({"foo": [100,1.5],"bar" : [200,2.5]})";
        using test_type = std::map<std::string,std::pair<int,double>>;

        json_decoder<json> decoder;
        std::error_code ec;

        json_string_cursor cursor(input);
        auto val = decode_traits<test_type,char>::decode(cursor,decoder,ec);

        REQUIRE_FALSE(ec);

        REQUIRE(val.size() == 2);
        REQUIRE(val.count("foo") > 0);
        REQUIRE(val.count("bar") > 0);
        CHECK(val["foo"].first == 100);
        CHECK(val["foo"].second == 1.5);
        CHECK(val["bar"].first == 200);
        CHECK(val["bar"].second == 2.5);
    }
    SECTION("Conversion error")
    {
        std::string input = R"({"foo": [100,1.5,30],"bar" : [200,2.5]])";
        using test_type = std::map<std::string,std::pair<int,double>>;

        json_decoder<json> decoder;
        std::error_code ec;

        json_string_cursor cursor(input);
        auto val = decode_traits<test_type,char>::decode(cursor,decoder,ec);

        CHECK(ec == conv_errc::not_pair);
    }
}

TEST_CASE("decode_traits deserialization errors")
{
    SECTION("Expected comma or right brace")
    {
        std::string input = R"({"foo": [100,1.5],"bar" : [200,2.5]])";
        using test_type = std::map<std::string,std::pair<int,double>>;

        json_decoder<json> decoder;
        std::error_code ec;

        json_string_cursor cursor(input);
        auto val = decode_traits<test_type,char>::decode(cursor,decoder,ec);

        CHECK(ec == json_errc::expected_comma_or_rbrace);
    }
}
