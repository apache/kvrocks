// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/jsonpath/json_location.hpp>

#include <jsoncons/json.hpp>

#include <iostream>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("jsonpath json_location_parser tests")
{
    SECTION("test 1")
    {
        jsonpath::detail::json_location_parser<char,std::allocator<char>> parser;

        std::error_code ec;
        std::vector<jsonpath::path_element> location = parser.parse(R"($['foo'][3]["bar"])", ec);
        REQUIRE_FALSE(ec);

        CHECK(location.size() == 3);
        CHECK(location[0].has_name());
        CHECK(location[0].name() == "foo");
        CHECK(location[1].has_index());
        CHECK(location[1].index() == 3);
        CHECK(location[2].has_name());
        CHECK(location[2].name() == "bar");
    }
    SECTION("test dot")
    {
        jsonpath::detail::json_location_parser<char, std::allocator<char>> parser;

        std::error_code ec;
        std::vector<jsonpath::path_element> location = parser.parse(R"($.'foo'.3.bar)", ec);
        REQUIRE_FALSE(ec);

        CHECK(location.size() == 3);
        CHECK(location[0].has_name());
        CHECK(location[0].name() == "foo");
        CHECK(location[1].has_index());
        CHECK(location[1].index() == 3);
        CHECK(location[2].has_name());
        CHECK(location[2].name() == "bar");
    }
    SECTION("test errors")
    {
        jsonpath::detail::json_location_parser<char, std::allocator<char>> parser;

        std::error_code ec;

        parser.parse("['foo'][3]['bar']", ec);
        CHECK(ec.value() == (int)jsonpath::jsonpath_errc::expected_root_or_current_node);

        parser.parse("$['foo'][-3]['bar']", ec);
        CHECK(ec.value() == (int)jsonpath::jsonpath_errc::expected_single_quote_or_digit);

        parser.parse("$['foo'][3a]['bar']", ec);
        CHECK(ec.value() == (int)jsonpath::jsonpath_errc::expected_rbracket);

        parser.parse("$['foo'][3]['bar'", ec);
        CHECK(ec.value() == (int)jsonpath::jsonpath_errc::unexpected_eof);
    }
}

