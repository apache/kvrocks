// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/jsonpath/path_node.hpp>
#include <jsoncons/json.hpp>

#include <iostream>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("test json_location equals")
{
    jsonpath::path_node a1{};
    jsonpath::path_node a2(&a1,"foo");
    jsonpath::path_node a3(&a2,"bar");
    jsonpath::path_node a4(&a3,0);

    jsonpath::path_node b1{};
    jsonpath::path_node b2(&b1,"foo");
    jsonpath::path_node b3(&b2,"bar");
    jsonpath::path_node b4(&b3,0);

    CHECK((a4 == b4));
    CHECK((jsonpath::to_string(a4) == std::string("$['foo']['bar'][0]")));
}

TEST_CASE("test json_location with solidus to_string")
{
    jsonpath::path_node a1{};
    jsonpath::path_node a2(&a1,"foo's");
    jsonpath::path_node a3(&a2,"bar");
    jsonpath::path_node a4(&a3,0);

    CHECK(jsonpath::to_string(a4) == std::string(R"($['foo\'s']['bar'][0])"));
}

TEST_CASE("test path_node less")
{
    SECTION("test rhs < lhs")
    {
        jsonpath::path_node a1{};
        jsonpath::path_node a2(&a1,"foo");
        jsonpath::path_node a3(&a2,"bar");
        jsonpath::path_node a4(&a3,0);

        jsonpath::path_node b1{};
        jsonpath::path_node b2(&b1,"baz");
        jsonpath::path_node b3(&b2,"bar");
        jsonpath::path_node b4(&b3,0);

        CHECK_FALSE(b4 == a4);

        CHECK(b4 < a4);
        CHECK_FALSE(a4 < b4);

        CHECK(b3 < a4);
        CHECK_FALSE(a4 < b3);

        CHECK(b2 < a4);
        CHECK_FALSE(a4 < b2);
    }

    SECTION("test rhs < lhs 2")
    {
        jsonpath::path_node a1{};
        jsonpath::path_node a2(&a1,"foo");
        jsonpath::path_node a3(&a2,"bar");
        jsonpath::path_node a4(&a3,0);

        jsonpath::path_node b1{};
        jsonpath::path_node b2(&b1,"baz");
        jsonpath::path_node b3(&b2,"g");
        jsonpath::path_node b4(&b3,0);

        CHECK_FALSE(b4 == a4);

        CHECK(b4 < a4);
        CHECK_FALSE(a4 < b4);

        CHECK(b3 < a4);
        CHECK_FALSE(a4 < b3);

        CHECK(b2 < a4);
        CHECK_FALSE(a4 < b2);
    }

    SECTION("test rhs == lhs")
    {
        jsonpath::path_node a1{};
        jsonpath::path_node a2(&a1,"foo");
        jsonpath::path_node a3(&a2,"bar");
        jsonpath::path_node a4(&a3,0);

        jsonpath::path_node b1{};
        jsonpath::path_node b2(&b1,"foo");
        jsonpath::path_node b3(&b2,"bar");
        jsonpath::path_node b4(&b3,0);

        CHECK(a1 == b1);
        CHECK(a2 == b2);
        CHECK(a3 == b3);
        CHECK(a4 == b4);
        CHECK(b1 == a1);
        CHECK(b2 == a2);
        CHECK(b3 == a3);
        CHECK(b4 == a4);

        CHECK_FALSE(b4 < a4);
        CHECK_FALSE(a4 < b4);

        CHECK(b3 < a4);
        CHECK_FALSE(a4 < b3);

        CHECK(b2 < a4);
        CHECK_FALSE(a4 < b2);
    }

}

