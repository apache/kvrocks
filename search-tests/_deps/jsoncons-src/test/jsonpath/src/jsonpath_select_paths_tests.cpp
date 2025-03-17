// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/jsonpath/jsonpath.hpp>

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

TEST_CASE("jsonpath.jsonpath select_paths test")
{

    std::string json_string = R"(
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
    )";

    json doc = json::parse(json_string);

    SECTION("test 1")
    {
        auto expr = jsonpath::make_expression<json>("$..book[?(@.category == 'fiction')].title");
        auto result = expr.select_paths(doc);

        for (const auto& loc : result)
        {
            std::string s = jsonpath::to_string(loc);
            std::cout << s << '\n';
        }

        std::vector<jsonpath::json_location> locations;
        jsonpath::json_location expected1;
        expected1.append("store").append("book").append(1).append("title");
        jsonpath::json_location expected2;
        expected2.append("store").append("book").append(2).append("title");

        locations.push_back(expected1);
        locations.push_back(expected2);

        CHECK((result == locations));
    }
}

