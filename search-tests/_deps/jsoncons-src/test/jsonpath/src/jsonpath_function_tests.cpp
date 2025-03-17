// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <catch/catch.hpp>

namespace jsonpath = jsoncons::jsonpath;

TEST_CASE("csv function tests")
{
    std::string store = R"(
{ 
  "books": [ 
    { "title": "Sayings of the Century"
    },
    { "title": "Sword of Honour"
    },
    { "title": "Moby Dick"
    },
    { "title": "The Lord of the Rings"
    }
  ]
}
    )";

    SECTION("length")
    {
        auto root = jsoncons::json::parse(store);

        // The number of books
        auto result1 = jsonpath::json_query(root, "length($.books)");
        REQUIRE(1 == result1.size());
        REQUIRE(result1[0].is<std::size_t>());
        CHECK(4 == result1[0].as<std::size_t>());

        auto result2 = jsonpath::json_query(root, "length($..books)");
        REQUIRE(1 == result2.size());
        REQUIRE(result2[0].is<std::size_t>());
        CHECK(4 == result2[0].as<std::size_t>());
    }
}


