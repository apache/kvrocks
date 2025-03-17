// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/jsonpath/json_query.hpp>
#include <jsoncons/json.hpp>

#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <new>
#include <unordered_set> // std::unordered_set
#include <fstream>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("test replace tests")
{
    json j;
    JSONCONS_TRY
    {
        j = json::parse(R"(
{ "store": {
    "book": [ 
      { "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      { "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      },
      { "category": "fiction",
        "author": "Herman Melville",
        "title": "Moby Dick",
        "isbn": "0-553-21311-3",
        "price": 8.99
      }
    ]
  }
}
)");
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        std::cout << e.what() << '\n';
    }

    SECTION("test 1")
    {
        jsonpath::json_replace(j,"$..book[?(@.price==12.99)].price", 30.9);

        CHECK(30.9 == Approx(j["store"]["book"][1]["price"].as<double>()).epsilon(0.001));
    }

    SECTION("test 2")
    {
        std::string expr = "$.store.book[*].price";

        // make a discount on all books
        jsonpath::json_replace(j, expr,
            [](const std::string&,json& price) { price = std::round(price.as<double>() - 1.0); });

        CHECK(8.0 == Approx(j["store"]["book"][0]["price"].as<double>()).epsilon(0.001));
        CHECK(12.0 == Approx(j["store"]["book"][1]["price"].as<double>()).epsilon(0.001));
        CHECK(8.0 == Approx(j["store"]["book"][2]["price"].as<double>()).epsilon(0.001));
    }

    SECTION("legacy test")
    {
        std::string expr = "$.store.book[*].price";

        // make a discount on all books
        jsonpath::json_replace(j, expr,
            [](const json& price) { return std::round(price.as<double>() - 1.0); });

        CHECK(8.0 == Approx(j["store"]["book"][0]["price"].as<double>()).epsilon(0.001));
        CHECK(12.0 == Approx(j["store"]["book"][1]["price"].as<double>()).epsilon(0.001));
        CHECK(8.0 == Approx(j["store"]["book"][2]["price"].as<double>()).epsilon(0.001));
    }
}

TEST_CASE("replace with binary callback tests")
{
    SECTION("test 1")
    {
        jsoncons::ojson doc = jsoncons::ojson::parse(R"({"value":"long______________enough"})");
        jsoncons::ojson rep = jsoncons::ojson::parse(R"({"value":"rew"})");
        jsoncons::ojson expected = jsoncons::ojson::parse(R"({"value":{"value":"rew"}})");

        jsoncons::jsonpath::json_replace(doc, "$..value",
            [rep](const std::string&, jsoncons::ojson& match) {
                match = rep;
        });

        CHECK(expected == doc);
    }
    SECTION("test 2")
    {
        jsoncons::ojson doc = jsoncons::ojson::parse(R"({"value":"long______________enough"})");
        jsoncons::ojson rep = jsoncons::ojson::parse(R"({"value":"rew"})");
        jsoncons::ojson expected = jsoncons::ojson::parse(R"({"value":{"value":"rew"}})");

        jsoncons::jsonpath::json_replace(doc, "$..value",
            [rep](const std::string&, jsoncons::ojson& match) {
                 match = rep;
            });

        CHECK(expected == doc);
    }
    SECTION("test 3")
    {
        jsoncons::ojson doc = jsoncons::ojson::parse(R"({"value":"long______________enough"})");
        jsoncons::ojson expected = jsoncons::ojson::parse(R"({"value":"rew"})");

        jsoncons::jsonpath::json_replace(doc, "$..value",
            [](const std::string&, jsoncons::ojson& match) {
                match = "rew";
            });

        CHECK(expected == doc);
    }
    SECTION("test 4")
    {
        jsoncons::ojson doc = jsoncons::ojson::parse(R"({"value":"long______________enough"})");
        jsoncons::ojson expected = jsoncons::ojson::parse(R"({"value":"XXX"})");

        jsoncons::jsonpath::json_replace(doc, "$..value",
            [](const jsoncons::ojson&) {
                return jsoncons::ojson{ "XXX" };
            });

        CHECK(expected == doc);
    }
    SECTION("test 5")
    {
        jsoncons::ojson doc = jsoncons::ojson::parse(R"({"value":{"value":"long______________enough"}})");
        jsoncons::ojson expected = jsoncons::ojson::parse(R"({"value":"XXX"})");

        jsoncons::jsonpath::json_replace(doc, "$..value",
            [](const jsoncons::ojson&) {
                return jsoncons::ojson{ "XXX" };
            });

        CHECK(expected == doc);
    }
}

