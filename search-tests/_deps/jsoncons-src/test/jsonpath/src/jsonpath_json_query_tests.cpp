// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/jsonpath/json_query.hpp>
#include <jsoncons/json.hpp>

#include <catch/catch.hpp>
#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <new>
#include <unordered_set> // std::unordered_set
#include <fstream>

using namespace jsoncons;

TEST_CASE("jsonpath json_query json test")
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
        auto result = jsonpath::json_query(j,"$..book[?(@.category == 'fiction')].title");
        auto expected = json::parse(R"(["Sword of Honour","Moby Dick"])");
        CHECK(expected == (result));
    }

    SECTION("test 2")
    {
        std::string expr = "$..book[?(@.category == 'fiction')].title";
        auto result = jsonpath::json_query(j,expr);
        auto expected = json::parse(R"(["Sword of Honour","Moby Dick"])");
        CHECK(expected == (result));
    }

    SECTION("test 3")
    {
        std::string expr = "$..book[?(@.title == 'Sword of Honour')].title";

        json expected("Sword of Honour");
        jsonpath::json_query(j, expr, 
            [expected](const jsoncons::string_view&, const json& title) {CHECK(expected == (title));});
    }
}

TEST_CASE("jsonpath normalized path test")
{
   const json j = json::parse(R"({"\\":0})");

   const std::string path = R"($['\\'])";
   auto paths = jsonpath::json_query(j, path, jsonpath::result_options::path);
   CHECK(paths.size() == 1);

   auto result = jsonpath::json_query(j, paths[0].as_string_view());

   CHECK(result.size() == 1);
   CHECK(result[0].as<int>() == 0);
}

TEST_CASE("jsonpath json_query wjson test")
{
    wjson j;
    JSONCONS_TRY
    {
        j = wjson::parse(LR"(
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
        auto result = jsonpath::json_query(j,L"$..book[?(@.category == 'fiction')].title");
        auto expected = wjson::parse(LR"(["Sword of Honour","Moby Dick"])");
        CHECK(expected == (result));
    }
}

