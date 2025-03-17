// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/jsonpath/json_location.hpp>

#include <jsoncons/json.hpp>

#include <iostream>
#include <catch/catch.hpp>

using namespace jsoncons;
#if 0
TEST_CASE("json_location tests")
{

    SECTION("test 1")
    {
        jsonpath::json_location loc;
        loc.append("foo").append(1);

        CHECK(loc.size() == 2);
        CHECK(loc[0].has_name());
        CHECK(loc[0].name() == "foo");
        CHECK(loc[1].has_index());
        CHECK(loc[1].index() == 1);
    }
}

TEST_CASE("json_location parse tests")
{
    SECTION("test 1")
    {
        jsonpath::json_location loc;
        loc.append("foo").append(1);

        jsonpath::json_location loc2 = jsonpath::json_location::parse("$['foo'][1]");
        CHECK(loc2 == loc);
    }
}

TEST_CASE("json_location remove tests")
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

    SECTION("store book 1")
    {
        jsonpath::json_location loc;
        loc.append("store").append("book").append(1);


        CHECK(doc["store"]["book"].size() == 3);
        CHECK(doc["store"]["book"][1]["author"].as<std::string>() == "Evelyn Waugh");

        std::size_t count = jsonpath::remove(doc, loc);

        CHECK(count == 1);
        CHECK(doc["store"]["book"].size() == 2);
        CHECK(doc["store"]["book"][1]["author"].as<std::string>() == "Herman Melville");
    }

    SECTION("store book 2")
    {
        jsonpath::json_location loc;
        loc.append("store").append("book").append(2);


        CHECK(doc["store"]["book"].size() == 3);
        CHECK(doc["store"]["book"][2]["author"].as<std::string>() == "Herman Melville");

        std::size_t count = jsonpath::remove(doc, loc);

        CHECK(count == 1);
        CHECK(doc["store"]["book"].size() == 2);
        CHECK(doc["store"]["book"][1]["author"].as<std::string>() == "Evelyn Waugh");
    }

    SECTION("store book 3")
    {
        json orig = doc;

        jsonpath::json_location loc;
        loc.append("store").append("book").append(3);

        CHECK(doc["store"]["book"].size() == 3);
        CHECK(doc["store"]["book"][2]["author"].as<std::string>() == "Herman Melville");

        std::size_t count = jsonpath::remove(doc, loc);

        CHECK(count == 0);
        CHECK(doc == orig);
    }

    SECTION("store")
    {
        jsonpath::json_location loc;
        loc.append("store");

        std::size_t count = jsonpath::remove(doc, loc);
        CHECK(count == 1);
        CHECK(doc.size() == 0);
    }

    SECTION("store book")
    {
        jsonpath::json_location loc;
        loc.append("store").append("book");

        CHECK(doc["store"]["book"].size() == 3);
        std::size_t count = jsonpath::remove(doc, loc);
        CHECK(count == 1);
        CHECK(doc["store"]["book"].size() == 0);
    }

    SECTION("store lost&found")
    {
        json orig = doc;

        jsonpath::json_location loc;
        loc.append("store").append("lost&found");

        CHECK(doc["store"].size() == 1);
        std::size_t count = jsonpath::remove(doc, loc);
        CHECK(count == 0);
        CHECK(doc == orig);
    }

    SECTION("store book 2 price")
    {
        jsonpath::json_location loc;
        loc.append("store").append("book").append(2).append("price");

        CHECK(doc["store"]["book"].size() == 3);
        CHECK(doc["store"]["book"][2]["author"].as<std::string>() == "Herman Melville");
        CHECK(doc["store"]["book"][2].contains("price"));

        std::size_t count = jsonpath::remove(doc, loc);

        CHECK(count == 1);
        CHECK(doc["store"]["book"].size() == 3);
        CHECK(doc["store"]["book"][2]["author"].as<std::string>() == "Herman Melville");
        CHECK_FALSE(doc["store"]["book"][2].contains("price"));
    }

    SECTION("store 0")
    {
        json orig = doc;

        jsonpath::json_location loc;
        loc.append("store").append(0);

        CHECK(doc["store"]["book"].size() == 3);
        CHECK(doc["store"]["book"][2].contains("price"));

        std::size_t count = jsonpath::remove(doc, loc);

        CHECK(count == 0);
        CHECK(doc == orig);
    }
}

TEST_CASE("json_location select tests")
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

    SECTION("store book 1")
    {
        jsonpath::json_location loc;
        loc.append("store").append("book").append(1);

        auto result = jsonpath::get(doc, loc);

        CHECK(result.second);
        CHECK(*result.first == doc.at("store").at("book").at(1));
    }

    SECTION("store book 2")
    {
        jsonpath::json_location loc;
        loc.append("store").append("book").append(2);

        auto result = jsonpath::get(doc, loc);

        CHECK(result.second);
        CHECK(*result.first == doc.at("store").at("book").at(2));
    }

    SECTION("store book 3")
    {
        jsonpath::json_location loc;
        loc.append("store").append("book").append(3);

        auto result = jsonpath::get(doc, loc);

        CHECK(result == nullptr);
    }

    SECTION("store")
    {
        jsonpath::json_location loc;
        loc.append("store");

        auto result = jsonpath::get(doc, loc);
        CHECK(result.second);
        CHECK(*result.first == doc.at("store"));
    }

    SECTION("store book")
    {
        jsonpath::json_location loc;
        loc.append("store").append("book");

        auto result = jsonpath::get(doc, loc);
        CHECK(result.second);
        CHECK(*result.first == doc.at("store").at("book"));
    }

    SECTION("store lost&found")
    {
        jsonpath::json_location loc;
        loc.append("store").append("lost&found");

        auto result = jsonpath::get(doc, loc);
        CHECK(result == nullptr);
    }

    SECTION("store book 2 price")
    {
        jsonpath::json_location loc;
        loc.append("store").append("book").append(2).append("price");

        auto result = jsonpath::get(doc, loc);

        CHECK(result.second);
        CHECK(*result.first == doc.at("store").at("book").at(2).at("price"));
    }

    SECTION("store 0")
    {
        jsonpath::json_location loc;
        loc.append("store").append(0);

        auto result = jsonpath::get(doc, loc);

        CHECK(result == nullptr);
    }
}

TEST_CASE("test json_location from path_node")
{
    SECTION("test 1")
    {
        jsonpath::path_node a1{};
        jsonpath::path_node a2(&a1,"foo");
        jsonpath::path_node a3(&a2,"bar");
        jsonpath::path_node a4(&a3,7);

        jsonpath::json_location location;
        location.append("foo").append("bar").append(7);

        std::string jsonpath_string = "$['foo']['bar'][7]";

        CHECK((jsonpath::json_location{ a4 } == location));
        CHECK((jsonpath::to_string(location) == jsonpath_string));
    }
}
#endif

TEST_CASE("json_location replace tests")
{
    std::string json_string = R"(
{"books": [ 
    { "category": "reference",
      "author": "Nigel Rees",
      "title": "Sayings of the Century",
      "price": 8.95
    },
    { "category": "fiction",
      "author": "Evelyn Waugh",
      "title": "Sword of Honour"
    },
    { "category": "fiction",
      "author": "Herman Melville",
      "title": "Moby Dick",
      "isbn": "0-553-21311-3",
      "price": 8.99
    }
  ] 
}
    )";

    json doc = json::parse(json_string);

    SECTION("store book 1")
    {
        jsonpath::json_location loc = jsonpath::json_location::parse("$.books[0].price");
        json new_value{13.0}; 
        //std::cout << to_string(loc) << "\n";
        
        auto result1 = jsonpath::replace(doc, loc, new_value, false);
        CHECK(result1.second);
        bool test1 = result1.first == std::addressof(doc.at("books").at(0).at("price"));
        CHECK(test1);
        CHECK(doc.at("books").at(0).at("price") == new_value);

        auto result2 = jsonpath::replace(doc, loc, new_value, true);
        CHECK(result2.second);
        bool test2 = result2.first == std::addressof(doc.at("books").at(0).at("price"));
        CHECK(test2);

        //std::cout << pretty_print(doc) << "\n";
    }

    SECTION("test 2")
    {
        jsonpath::json_location loc = jsonpath::json_location::parse("$.books[1].price");
        json new_value{13.0}; 

        //std::cout << to_string(loc) << "\n";

        auto result1 = jsonpath::replace(doc, loc, new_value, false);
        CHECK_FALSE(result1.second);
        //std::cout << pretty_print(doc) << "\n";

        auto result2 = jsonpath::replace(doc, loc, new_value, true);
        CHECK(result2.second);
        bool test2 = result2.first == std::addressof(doc.at("books").at(1).at("price"));
        CHECK(test2);
        CHECK(doc.at("books").at(1).at("price") == new_value);

        //std::cout << pretty_print(doc) << "\n";
    }

    SECTION("test 3")
    {
        jsonpath::json_location loc = jsonpath::json_location::parse("$.books[1].kindle.price");
        json new_value{13.0}; 

        //std::cout << pretty_print(doc) << "\n";

        auto result1 = jsonpath::replace(doc, loc, new_value, false);
        CHECK_FALSE(result1.second);
        //std::cout << pretty_print(doc) << "\n";

        auto result2 = jsonpath::replace(doc, loc, new_value, true);
        CHECK(result2.second);
        bool test2 = result2.first == std::addressof(doc.at("books").at(1).at("kindle").at("price"));
        CHECK(test2);
        CHECK(doc.at("books").at(1).at("kindle").at("price") == new_value);

        //std::cout << pretty_print(doc) << "\n";
    }

    SECTION("test 4")
    {
        jsonpath::json_location loc = jsonpath::json_location::parse("$.books[2]");
        json new_value{}; 

        //std::cout << to_string(loc) << "\n";

        auto result1 = jsonpath::replace(doc, loc, new_value, false);
        CHECK(result1.second);
        bool test1 = result1.first == std::addressof(doc.at("books").at(2));
        CHECK(test1);
        //std::cout << pretty_print(doc) << "\n";
        CHECK(doc.at("books").at(2) == new_value);

        auto result2 = jsonpath::replace(doc, loc, new_value, true);
        CHECK(result2.second);
        bool test2 = result2.first == std::addressof(doc.at("books").at(2));
        CHECK(test2);
        CHECK(doc.at("books").at(2) == new_value);

        //std::cout << pretty_print(doc) << "\n";
    }

    SECTION("test 5")
    {
        jsonpath::json_location loc = jsonpath::json_location::parse("$.books[3]");
        json new_value{}; 

        //std::cout << to_string(loc) << "\n";

        auto result1 = jsonpath::replace(doc, loc, new_value, false);
        CHECK_FALSE(result1.second);

        auto result2 = jsonpath::replace(doc, loc, new_value, true);
        CHECK_FALSE(result2.second);

        //std::cout << pretty_print(doc) << "\n";
    }
}

