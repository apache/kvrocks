// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>
#include <jsoncons/json.hpp>

#include <iostream>
#include <catch/catch.hpp>

using jsoncons::json;
using jsoncons::json_options;
using jsoncons::ojson;
namespace jsonpatch = jsoncons::jsonpatch;
using namespace jsoncons::literals;

template <typename Json>
void check_patch(Json& target, const Json& patch, const std::error_code& expected_ec, const Json& expected)
{
    std::error_code ec;
    jsonpatch::apply_patch(target, patch, ec);
    if (ec != expected_ec || expected != target)
    {
        std::cout << "target:\n" << target << '\n';
    }
    CHECK(ec == expected_ec); //-V521
    CHECK(expected == target); //-V521
}

TEST_CASE("testing_a_value_success")
{
    json target = R"(
        {
            "baz": "qux",
            "foo": [ "a", 2, "c" ]
        }
    )"_json;

    json patch = R"(
        [
           { "op": "test", "path": "/baz", "value": "qux" },
           { "op": "test", "path": "/foo/1", "value": 2 }
        ]
    )"_json;

    json expected = target;

    check_patch(target,patch,std::error_code(),expected);
}

TEST_CASE("testing_a_value_error")
{
    json target = R"(
        { "baz": "qux" }

    )"_json;

    json patch = R"(
        [
           { "op": "test", "path": "/baz", "value": "bar" }
        ]
    )"_json;

    json expected = target;

    check_patch(target,patch,jsonpatch::jsonpatch_errc::test_failed,expected);
}

TEST_CASE("comparing_strings_and_numbers")
{
    json target = R"(
        {
            "/": 9,
            "~1": 10
        }

    )"_json;

    json patch = R"(
        [
            {"op": "test", "path": "/~01", "value": "10"}
        ]
    )"_json;

    json expected = target;

    check_patch(target,patch,jsonpatch::jsonpatch_errc::test_failed,expected);
}

TEST_CASE("test_add_add")
{
    json target = R"(
        { "foo": "bar"}
    )"_json;

    json patch = R"(
        [
            { "op": "add", "path": "/baz", "value": "qux" },
            { "op": "add", "path": "/foo", "value": [ "bar", "baz" ] }
        ]
    )"_json;

    json expected = R"(
        { "baz":"qux", "foo": [ "bar", "baz" ]}
    )"_json;

    check_patch(target,patch,std::error_code(),expected);
}

TEST_CASE("test_diff1")
{
    json source = R"(
        {"/": 9, "~1": 10, "foo": "bar"}
    )"_json;

    json target = R"(
        { "baz":"qux", "foo": [ "bar", "baz" ]}
    )"_json;

    auto patch = jsonpatch::from_diff(source, target);

    check_patch(source,patch,std::error_code(),target);
}

TEST_CASE("test_diff2")
{
    json source = R"(
        { 
            "/": 3,
            "foo": "bar"
        }
    )"_json;

    json target = R"(
        {
            "/": 9,
            "~1": 10
        }
    )"_json;

    auto patch = jsonpatch::from_diff(source, target);

    check_patch(source,patch,std::error_code(),target);
}

TEST_CASE("add_when_new_items_in_target_array1")
{
    json source = R"(
        {"/": 9, "foo": [ "bar"]}
    )"_json;

    json target = R"(
        { "baz":"qux", "foo": [ "bar", "baz" ]}
    )"_json;

    json patch = jsoncons::jsonpatch::from_diff(source, target); 

    check_patch(source,patch,std::error_code(),target);
}

TEST_CASE("add_when_new_items_in_target_array2")
{
    json source = R"(
        {"/": 9, "foo": [ "bar", "bar"]}
    )"_json;

    json target = R"(
        { "baz":"qux", "foo": [ "bar", "baz" ]}
    )"_json;

    json patch = jsoncons::jsonpatch::from_diff(source, target); 

    check_patch(source,patch,std::error_code(),target);
}

TEST_CASE("jsonpatch - remove two items from array")
{
    json source = json::parse(R"(
{ "names" : [ "a", "b", "c", "d" ] }
    )");

    json target = json::parse(R"(
{ "names" : [ "a", "b" ] }
    )");

    json patch = jsoncons::jsonpatch::from_diff(source, target); 

    check_patch(source,patch,std::error_code(),target);
}

TEST_CASE("from diff with null and lossless number")
{
    ojson expected_patch = ojson::parse(
        R"([{"op":"replace","path":"/hello","value":null},{"op":"replace","path":"/hello2","value":"123.4"}])"
    );
    
    auto options = json_options{}
        .lossless_number(true)
        .bignum_format(jsoncons::bignum_format_kind::raw)
        .byte_string_format(jsoncons::byte_string_chars_format::base64);

    const char* json1 = "{\"hello\":123.4, \"hello2\":null}";
    const char* json2 = "{\"hello\":null,  \"hello2\":123.4 }";

    ojson j1 = ojson::parse(json1, options);
    ojson j2 = ojson::parse(json2, options);

    ojson patch = jsonpatch::from_diff(j1, j2);
    
    CHECK(expected_patch == patch);
    check_patch(j1,patch,std::error_code(),j2);
}


