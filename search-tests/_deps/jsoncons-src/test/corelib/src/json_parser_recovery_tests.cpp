
// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <jsoncons/json_reader.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("test_array_extra_comma")
{
    SECTION("using err_handler")
    {
        allow_trailing_commas err_handler;

        json expected = json::parse("[1,2,3]");

        auto options = json_options{}
        .err_handler(err_handler);
        json val = json::parse("[1,2,3,]", options);

        CHECK(expected == val);
    }
    SECTION("with option")
    {
        auto options = json_options{}
            .allow_trailing_comma(true);

        json expected = json::parse("[1,2,3]");

        json val = json::parse("[1,2,3,]", options);

        CHECK(expected == val);
    }
}

TEST_CASE("test_object_extra_comma")
{
    SECTION("using err_handler")
    {
        allow_trailing_commas err_handler;

        json expected = json::parse(R"(
    {
        "first" : 1,
        "second" : 2
    }
    )",
            err_handler);

        json val = json::parse(R"(
    {
        "first" : 1,
        "second" : 2,
    }
    )",
            err_handler);

        CHECK(expected == val);
    }

    SECTION("with option")
    {
        auto options = json_options{}
            .allow_trailing_comma(true);

        json expected = json::parse(R"(
    {
        "first" : 1,
        "second" : 2
    }
    )", options);

        json val = json::parse(R"(
    {
        "first" : 1,
        "second" : 2,
    }
    )", options);

        CHECK(expected == val);
    }
}

TEST_CASE("test json_parser error recovery")
{
    SECTION("illegal control character")
    {
        auto err_handler = [](const std::error_code& ec, const ser_context&) noexcept -> bool
            {
                return ec == json_errc::illegal_control_character;
            };
        
        std::string str;
        str.push_back('"');
        str.push_back('C');
        str.push_back(0x0e);
        str.push_back('a');
        str.push_back('t');
        str.push_back('"');
        auto j = jsoncons::json::parse(str, err_handler);
        REQUIRE(j.is_string());
        CHECK(j.as_string() == "Cat");
    }

    SECTION("\r")
    {
        auto err_handler = [](const std::error_code& ec, const ser_context&) noexcept -> bool
            {
                return ec == json_errc::illegal_character_in_string;
            };


        std::string str;
        str.push_back('C');
        str.push_back('\r');
        str.push_back('a');
        str.push_back('t');
        std::string str2;
        str2.push_back('"');
        str2.append(str);
        str2.push_back('"');
        auto j = jsoncons::json::parse(str2, err_handler);
        REQUIRE(j.is_string());
        CHECK(j.as_string() == "Cat");
    }
    SECTION("\n")
    {
        auto err_handler = [](const std::error_code& ec, const ser_context&) noexcept -> bool
            {
                return ec == json_errc::illegal_character_in_string;
            };


        std::string str;
        str.push_back('C');
        str.push_back('\n');
        str.push_back('a');
        str.push_back('t');
        std::string str2;
        str2.push_back('"');
        str2.append(str);
        str2.push_back('"');
        auto j = jsoncons::json::parse(str2, err_handler);
        REQUIRE(j.is_string());
        CHECK(j.as_string() == "Cat");
    }
}


