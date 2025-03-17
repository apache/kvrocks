// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/jmespath/jmespath.hpp>
#include <jsoncons/json.hpp>

#include <iostream>
#include <catch/catch.hpp>

namespace jmespath = jsoncons::jmespath;

TEST_CASE("jmespath let tests")
{
    SECTION("Test 1")
    {
        auto doc = jsoncons::json::parse(R"({"foo": "bar"})");
        auto expected = jsoncons::json::parse(R"("bar")");

        std::string query = R"(let $foo = foo in $foo)";
        auto expr = jmespath::make_expression<jsoncons::json>(query);

        jsoncons::json result = expr.evaluate(doc);
        CHECK(expected == result);
    }    
    SECTION("Test 2")
    {
        auto doc = jsoncons::json::parse(R"({"foo": {"bar": "baz"}})");
        auto expected = jsoncons::json::parse(R"("baz")");

        std::string query = R"(let $foo = foo.bar in $foo)";
        auto expr = jmespath::make_expression<jsoncons::json>(query);

        jsoncons::json result = expr.evaluate(doc);
        CHECK(expected == result);
    }    
    SECTION("Test 3")
    {
        auto doc = jsoncons::json::parse(R"({"foo": "bar"})");
        auto expected = jsoncons::json::parse(R"(["bar", "bar"])");

        std::string query = R"(let $foo = foo in [$foo, $foo])";
        auto expr = jmespath::make_expression<jsoncons::json>(query);

        jsoncons::json result = expr.evaluate(doc);
        CHECK(expected == result);
    }    
    SECTION("Nested bindings")
    {
        auto doc = jsoncons::json::parse(R"({"a": "topval", "b": [{"a": "inner1"}, {"a": "inner2"}]})");
        auto expected = jsoncons::json::parse(R"( [["inner1", "topval", "shadow"], ["inner2", "topval", "shadow"]])");

        std::string query = R"(let $a = a
  in
    b[*].[a, $a, let $a = 'shadow' in $a])";
        auto expr = jmespath::make_expression<jsoncons::json>(query);

        jsoncons::json result = expr.evaluate(doc);
        CHECK(expected == result);
    }
}

TEST_CASE("jmespath let as valid identifiers")
{
    auto doc = jsoncons::json::parse(R"(
{
    "let" : 
    {
        "let" : "let-val",
        "in" : "in-val"
    }
}
    )");

    SECTION("test 1")
{
        auto expected = jsoncons::json::parse(R"(
{
    "in": {
        "in": "in-val",
        "let": "let-val"
    },
    "let": {
        "in": "in-val",
        "let": "let-val"
    }
}
        )");

        std::string query = R"(let $let = let in {let: let, in: $let})";
        auto expr = jmespath::make_expression<jsoncons::json>(query);

        jsoncons::json result = expr.evaluate(doc);
        //std::cout << pretty_print(result) << "\n";
        CHECK(expected == result);
    }    

    SECTION("test 2")
{
        auto expected = jsoncons::json::parse(R"(
{
    "in": "let",
    "let": {
        "in": "in-val",
        "let": "let-val"
    }
}
        )");

        std::string query = R"(let $let = 'let' in { let: let, in: $let })";
        auto expr = jmespath::make_expression<jsoncons::json>(query);

        jsoncons::json result = expr.evaluate(doc);
        //std::cout << pretty_print(result) << "\n";
        CHECK(expected == result);
    }    

    SECTION("test 3")
    {
        auto expected = jsoncons::json::parse(R"(
{
    "in": "let",
    "let": "let"
}
        )");

        std::string query = R"(let $let = 'let' in { let: 'let', in: $let })";
        auto expr = jmespath::make_expression<jsoncons::json>(query);

        jsoncons::json result = expr.evaluate(doc);
        //std::cout << pretty_print(result) << "\n";
        CHECK(expected == result);
    }    
}

TEST_CASE("jmespath let projection stop")
{
    auto doc = jsoncons::json::parse(R"(
{"foo" : [[0, 1], [2, 3], [4, 5]]}
    )");

    SECTION("test 1")
    {
        auto expected = jsoncons::json::parse(R"(
[0, 1]
        )");

        std::string query = R"(let $foo = foo[*] in $foo[0])";
        auto expr = jmespath::make_expression<jsoncons::json>(query);

        jsoncons::json result = expr.evaluate(doc);
        //std::cout << pretty_print(result) << "\n";
        CHECK(expected == result);
    }    
}

TEST_CASE("jmespath let motivation section")
{
    SECTION("test 1")
{
        auto doc = jsoncons::json::parse(R"(
[
  {"home_state": "WA",
   "states": [
     {"name": "WA", "cities": ["Seattle", "Bellevue", "Olympia"]},
     {"name": "CA", "cities": ["Los Angeles", "San Francisco"]},
     {"name": "NY", "cities": ["New York City", "Albany"]}
   ]
  },
  {"home_state": "NY",
   "states": [
     {"name": "WA", "cities": ["Seattle", "Bellevue", "Olympia"]},
     {"name": "CA", "cities": ["Los Angeles", "San Francisco"]},
     {"name": "NY", "cities": ["New York City", "Albany"]}
   ]
  }
]
        )");
        auto expected = jsoncons::json::parse(R"(
[
    [
        "Seattle",
        "Bellevue",
        "Olympia"
    ],
    [
        "New York City",
        "Albany"
    ]
]
        )");

        std::string query = R"([*].[let $home_state = home_state in states[? name == $home_state].cities[]][])";
        auto expr = jmespath::make_expression<jsoncons::json>(query);

        jsoncons::json result = expr.evaluate(doc);
        //std::cout << pretty_print(result) << "\n";
        CHECK(expected == result);
    }    
    
    SECTION("test 2")
{
        auto doc = jsoncons::json::parse(R"(
{"imageDetails": [
  {
    "repositoryName": "org/first-repo",
    "imageTags": ["latest", "v1.0", "v1.2"],
    "imageDigest": "sha256:abcd"
  },
  {
    "repositoryName": "org/second-repo",
    "imageTags": ["v2.0", "v2.2"],
    "imageDigest": "sha256:efgh"
  }
]}
        )");
        
        auto expected = jsoncons::json::parse(R"(
[
    ["latest","sha256:abcd","org/first-repo"],
    ["v1.0","sha256:abcd","org/first-repo"],
    ["v1.2","sha256:abcd","org/first-repo"],
    ["v2.0","sha256:efgh","org/second-repo"],
    ["v2.2","sha256:efgh","org/second-repo"]
]
        )");

        std::string query = R"(imageDetails[].[
          let $repo = repositoryName,
              $digest = imageDigest
          in
            imageTags[].[@, $digest, $repo]
        ][][])";
        auto expr = jmespath::make_expression<jsoncons::json>(query);

        jsoncons::json result = expr.evaluate(doc);
        //std::cout << pretty_print(result) << "\n";
        CHECK(expected == result);
    }    
}

TEST_CASE("jmespath let errors")
{
    jsoncons::json doc{jsoncons::json_object_arg};

    SECTION("test 1")
    {
        std::error_code ec;
        std::string query = R"($noexist)";
        auto expr = jmespath::make_expression<jsoncons::json>(query, ec);
        CHECK_FALSE(ec);

        expr.evaluate(doc, ec);
        CHECK(ec == jmespath::jmespath_errc::undefined_variable);
    }    

    SECTION("test 2")
    {
        std::error_code ec;
        std::string query = R"([let $scope = 'foo' in [$scope], $scope])";
        auto expr = jmespath::make_expression<jsoncons::json>(query, ec);
        CHECK_FALSE(ec);

        expr.evaluate(doc, ec);
        CHECK(ec == jmespath::jmespath_errc::undefined_variable);
    }

    SECTION("test 3")
    {
        std::error_code ec;
        std::string query = R"(foo.$bar)";
        auto expr = jmespath::make_expression<jsoncons::json>(query, ec);
        CHECK(ec == jmespath::jmespath_errc::expected_identifier);
    }
    SECTION("test 4")
    {
        std::error_code ec;
        std::string query = R"($noexist)";
        auto expr = jmespath::make_expression<jsoncons::json>(query, ec);
        CHECK_FALSE(ec);

        auto expected = jsoncons::json::parse(R"("foo")");
        auto result = expr.evaluate(doc, {{"noexist", "foo"}}, ec);
        CHECK_FALSE(ec);
        CHECK(expected == result);
        //std::cout << result << "\n";
    }    

    SECTION("test 5")
    {
        std::error_code ec;
        std::string query = R"([let $scope = 'foo' in [$scope], $scope])";
        auto expr = jmespath::make_expression<jsoncons::json>(query, ec);
        CHECK_FALSE(ec);

        auto expected = jsoncons::json::parse(R"([["foo"],"foo"])");
        auto result = expr.evaluate(doc, { {"scope", "foo"} }, ec);
        CHECK_FALSE(ec);
        CHECK(expected == result);
        //std::cout << result << "\n";
    }
}

TEST_CASE("jmespath let params")
{
    SECTION("test 1")
    {
        auto doc = jsoncons::json::parse(R"(
{
    "results": [
         {
              "name": "test1",
              "uuid": "33bb9554-c616-42e6-a9c6-88d3bba4221c"
          },
          {
              "name": "test2",
              "uuid": "acde070d-8c4c-4f0d-9d8a-162843c10333"
          }
    ]
}
        )");
        
        auto expr = jmespath::make_expression<jsoncons::json>("results[*].[name, uuid, $hostname]");

        auto result = expr.evaluate(doc, { {"hostname", "localhost"} });
        
        auto options = jsoncons::json_options{}
            .array_array_line_splits(jsoncons::line_split_kind::same_line);

        std::cout << pretty_print(result) << "\n";
    }
}
