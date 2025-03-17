// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <map>
#include <iterator>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("json_reference array tests")
{
    json j = json::parse(R"( [1, "two", "three"] )");

    SECTION("size()")
    {
        json v(json_pointer_arg, &j);
        REQUIRE(v.is_array());
        CHECK(v.get_allocator() == j.get_allocator());
        REQUIRE(j.size() == v.size());
        v.resize(4);
        REQUIRE(4 == v.size());
        CHECK(json{} == v[3]);

        v.resize(5, json{jsoncons::null_arg});
        REQUIRE(5 == v.size());
        CHECK(json{} == v[3]);
        CHECK(json::null() == v[4]);
        CHECK(v[4].is_null());
    }

    SECTION("compare with json_const_pointer_arg")
    {
        json other{ j };
        json j1(json_pointer_arg, &other);
        json j2(json_const_pointer_arg, &other);
        
        CHECK(j1 == j2);
        CHECK(j == j1);
        CHECK(j == j2);

        j[0] = "one";

        CHECK(j1 == j2);
        CHECK_FALSE(j == j1);
        CHECK_FALSE(j == j2);
    }
    SECTION("capacity()")
    {
        json v(json_pointer_arg, &j);
        REQUIRE(v.is_array());
        CHECK(j.capacity() == v.capacity());
        v.reserve(4);
        REQUIRE(4 == v.capacity());
    }

    SECTION("empty()")
    {
        json v(json_pointer_arg, &j);
        REQUIRE(v.is_array());
        CHECK_FALSE(v.empty());
    }

    SECTION("is_int64()")
    {
        json v(json_pointer_arg, &j);
        REQUIRE(v.is_array());
        CHECK(v[0].is_int64());
        CHECK_FALSE(v[1].is_int64());
    }
    SECTION("is_number()")
    {
        json v(json_pointer_arg, &j);
        REQUIRE(v.is_array());
        CHECK(v[0].is_number());
        CHECK_FALSE(v[1].is_number());
    }
    SECTION("operator[]")
    {
        json expected = json::parse(R"( [1, "two", "four"] )");

        json v(json_pointer_arg, &j);
        CHECK(v.storage_kind() == json_storage_kind::json_reference);
        j[2] = "four";

        CHECK(expected == v);
    }
    SECTION("const operator[]")
    {
        const json v(json_pointer_arg, &j);
        CHECK(v.storage_kind() == json_storage_kind::json_reference);

        CHECK("three" == v[2]);
    }
    SECTION("at()")
    {
        json v(json_pointer_arg, &j);
        REQUIRE(v.is_array());
        REQUIRE_NOTHROW(v.at(1));
        CHECK("two" == v[1]);
    }
    SECTION("const at()")
    {
        const json v(json_pointer_arg, &j);
        REQUIRE(v.is_array());
        REQUIRE_NOTHROW(v.at(1));
        CHECK("two" == v[1]);
    }
    SECTION("copy")
    {
        json v(json_pointer_arg, &j);
        CHECK(v.storage_kind() == json_storage_kind::json_reference);

        json j2(v);
        CHECK(j2.storage_kind() == json_storage_kind::json_reference);
    }
    SECTION("assignment")
    {
        json v(json_pointer_arg, &j);
        CHECK(v.storage_kind() == json_storage_kind::json_reference);

        json j2;
        j2 = v;
        CHECK(j2.storage_kind() == json_storage_kind::json_reference);
    }
    SECTION("push_back")
    {
        json expected = json::parse(R"( [1, "two", "three", "four"] )");

        json v(json_pointer_arg, &j);
        CHECK(v.storage_kind() == json_storage_kind::json_reference);
        j.push_back("four");
        
        CHECK(expected == v);
    }
    SECTION("emplace_back")
    {
        json expected = json::parse(R"( [1, "two", "three", "four"] )");

        json v(json_pointer_arg, &j);
        CHECK(v.storage_kind() == json_storage_kind::json_reference);
        j.emplace_back("four");

        CHECK(expected == v);
    }
}

TEST_CASE("json_reference object tests")
{
    json j = json::parse(R"( {"one" : 1, "two" : 2, "three" : 3} )");

    SECTION("size()")
    {
        json v(json_pointer_arg, &j);
        REQUIRE(v.is_object());
        CHECK(v.size() == 3);
        CHECK_FALSE(v.empty());
    }

    SECTION("compare with json_const_pointer_arg")
    {
        json other{ j };
        json j1(json_pointer_arg, &other);
        json j2(json_const_pointer_arg, &other);

        CHECK(j1 == j2);
        CHECK(j == j1);
        CHECK(j == j2);

        j["one"] = 4;

        CHECK(j1 == j2);
        CHECK_FALSE(j == j1);
        CHECK_FALSE(j == j2);
    }
    SECTION("at()")
    {
        json v(json_pointer_arg, &j);
        REQUIRE(v.is_object());
        REQUIRE_NOTHROW(v.at("two"));
        CHECK(v.contains("two"));
        CHECK(v.count("two") == 1);

        CHECK(v.get_value_or<int>("three", 0) == 3);
        CHECK(v.get_value_or<int>("four", 4) == 4);
        
        v.at("one") = "first";
        CHECK("first" == v.at("one"));
    }
    SECTION("insert_or_assign()")
    {
        json expected = json::parse(R"( {"one" : 1, "two" : 2, "three" : "third", "four" : 4} )");

        json v(json_pointer_arg, &j);
        REQUIRE(v.is_object());
        REQUIRE_NOTHROW(v.at("two"));
        CHECK(v.contains("two"));
        CHECK(v.count("two") == 1);

        CHECK(v.get_value_or<int>("three", 0) == 3);
        CHECK(v.get_value_or<int>("four", 4) == 4);

        v.insert_or_assign("four", 4);
        v.insert_or_assign("three", "third");
        CHECK(expected == v);
    }
    SECTION("try_emplace()")
    {
        json expected = json::parse(R"( {"one" : 1, "two" : 2, "three" : 3, "four" : 4} )");

        json v(json_pointer_arg, &j);
        REQUIRE(v.is_object());
        REQUIRE_NOTHROW(v.at("two"));
        CHECK(v.contains("two"));
        CHECK(v.count("two") == 1);

        CHECK(v.get_value_or<int>("three", 0) == 3);
        CHECK(v.get_value_or<int>("four", 4) == 4);

        v.try_emplace("four", 4);
        v.try_emplace("three", "third"); // does nothing
        CHECK(expected == v);
    }
    SECTION("merge()")
    {
        json expected1 = json::parse(R"( {"one" : 1, "two" : 2, "three" : 3, "four" : 4} )");
        json expected2 = json::parse(R"( {"one" : 1, "two" : 2, "three" : 3, "four" : 4, "five" : 5} )");
        
        json j1 = json::parse(R"( {"three" : "third", "four" : 4} )");

        json v(json_pointer_arg, &j);
        REQUIRE(v.is_object());

        v.merge(j1);
        CHECK(expected1 == v);

        json j2 = json::parse(R"( {"five" : 5} )");
        j2.merge(v);
        CHECK(expected2 == j2);
    }
    SECTION("merge_or_update()")
    {
        json expected1 = json::parse(R"( {"one" : 1, "two" : 2, "three" : "third", "four" : 4} )");
        json expected2 = json::parse(R"( {"one" : 1, "two" : 2, "three" : "third", "four" : 4, "five" : 5} )");

        json j1 = json::parse(R"( {"three" : "third", "four" : 4} )");

        json v(json_pointer_arg, &j);
        REQUIRE(v.is_object());

        v.merge_or_update(j1);
        CHECK(expected1 == v);

        json j2 = json::parse(R"( {"five" : 5} )");
        j2.merge_or_update(v);
        CHECK(expected2 == j2);
    }
}

TEST_CASE("json_reference string tests")
{
    json j = json("Hello World");

    SECTION("is_string()")
    {
        json v(json_pointer_arg, &j);
        REQUIRE(v.is_string());
        REQUIRE(v.is_string_view());

        CHECK(v.as<std::string>() == j.as<std::string>());
    }
}

TEST_CASE("json_reference byte_string tests")
{
    std::string data = "abcdefghijk";
    json j(byte_string_arg, data);

    SECTION("is_byte_string()")
    {
        json v(json_pointer_arg, &j);
        REQUIRE(v.is_byte_string());
        REQUIRE(v.is_byte_string_view());
    }
}

TEST_CASE("json_reference bool tests")
{
    json tru(true);
    json fal(false);

    SECTION("true")
    {
        json v(json_pointer_arg, &tru);
        REQUIRE(v.is_bool());
        CHECK(v.as_bool());
    }
    SECTION("false")
    {
        json v(json_pointer_arg, &fal);
        REQUIRE(v.is_bool());
        CHECK_FALSE(v.as_bool());
    }
}

TEST_CASE("json_reference null tests")
{
    json null(jsoncons::null_arg);

    SECTION("null")
    {
        json v(json_pointer_arg, &null);
        REQUIRE(v.is_null());
    }
}

TEST_CASE("json_reference int64 tests")
{
    json j(-100);

    SECTION("is_int64()")
    {
        json v(json_pointer_arg, &j);
        REQUIRE(v.is_int64());
        CHECK(v.as<int64_t>() == -100);
    }
}

TEST_CASE("json_reference uint64 tests")
{
    json j(100);

    SECTION("is_uint64()")
    {
        json v(json_pointer_arg, &j);
        REQUIRE(v.is_uint64());
        CHECK(v.as<uint64_t>() == 100);
    }
}

TEST_CASE("json_reference half tests")
{
    json j(half_arg, 100);

    SECTION("is_half()")
    {
        json v(json_pointer_arg, &j);
        REQUIRE(v.is_half());
        CHECK(v.as<uint16_t>() == 100);
    }
}

TEST_CASE("json_reference double tests")
{
    json j(123.456);

    SECTION("is_double()")
    {
        json v(json_pointer_arg, &j);
        REQUIRE(v.is_double());

        CHECK(v.as_double() == 123.456);
    }
}

