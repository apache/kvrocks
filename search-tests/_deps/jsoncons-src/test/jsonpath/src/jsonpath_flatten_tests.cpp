// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/jsonpath/flatten.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <jsoncons/json.hpp>

#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <new>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("jsonpath flatten test")
{
    json input = json::parse(R"(
    {
       "application": "hiking",
       "reputons": [
           {
               "rater": "HikingAsylum",
               "assertion": "advanced",
               "rated": "Marilyn C",
               "rating": 0.90
            },
            {
               "rater": "HikingAsylum",
               "assertion": "intermediate",
               "rated": "Hongmin",
               "rating": 0.75
            }    
        ]
    }
    )");

    SECTION("flatten")
    {
        json result = jsonpath::flatten(input);

        REQUIRE(result.is_object()); //-V521
        REQUIRE(result.size() == 9); //-V521

        //std::cout << pretty_print(result) << "\n";

        CHECK(result["$['application']"].as<std::string>() == std::string("hiking")); //-V521
        CHECK(result["$['reputons'][0]['assertion']"].as<std::string>() == std::string("advanced")); //-V521
        CHECK(result["$['reputons'][0]['rated']"].as<std::string>() == std::string("Marilyn C")); //-V521
        CHECK(result["$['reputons'][0]['rater']"].as<std::string>() == std::string("HikingAsylum")); //-V521
        CHECK(result["$['reputons'][0]['rating']"].as<double>() == Approx(0.9).epsilon(0.0000001)); //-V521
        CHECK(result["$['reputons'][1]['assertion']"].as<std::string>() == std::string("intermediate")); //-V521
        CHECK(result["$['reputons'][1]['rated']"].as<std::string>() == std::string("Hongmin")); //-V521
        CHECK(result["$['reputons'][1]['rater']"].as<std::string>() == std::string("HikingAsylum")); //-V521
        CHECK(result["$['reputons'][1]['rating']"].as<double>() == Approx(0.75).epsilon(0.0000001)); //-V521

        //std::cout << pretty_print(result) << "\n";
    }

    SECTION("unflatten")
    {
        json result = jsonpath::flatten(input);
        //std::cout << pretty_print(result) << "\n";

        json original = jsonpath::unflatten(result);
        //std::cout << pretty_print(original) << "\n";
        CHECK(original == input); //-V521
    }
}

TEST_CASE("jsonpath flatten array test")
{
    json input = json::parse(R"([1,2,3,"4\u0027s"])");

    SECTION("flatten array and unflatten")
    {
        json result = jsonpath::flatten(input);
        //std::cout << pretty_print(result) << "\n";

        json original = jsonpath::unflatten(result);
        //std::cout << pretty_print(original) << "\n";
        CHECK(original == input); //-V521
    }

}

TEST_CASE("jsonpath flatten with single quote test")
{
    json input = json::parse(R"(
    {
       "like'd": "pizza"
    }
    )");

    SECTION("flatten array and unflatten")
    {
        json result = jsonpath::flatten(input);

        json original = jsonpath::unflatten(result);
        CHECK(original == input); //-V521
    }
}

namespace {

    void compare_match(jsoncons::json& doc,
                       const std::string& path, 
                       const std::string& value)
    {
        auto result = jsoncons::jsonpath::json_query(doc, path);
        CHECK_FALSE(result.empty()); // must match //-V521
        CHECK_FALSE(result.size() > 1); // too many matches //-V521

        auto matched_value = result[0].as<std::string>();
        CHECK(value == matched_value); //-V521
    }

} // namespace

TEST_CASE("jsonpath flatten escape")
{
    std::string json
    {
        R"({)"
            R"("data":)"
            R"({)"
                R"("a\"bc": "abc",)"
                R"("d'ef": "def",)"
                R"("g.hi": "ghi",)"
                R"("j\\kl": "jkl",)"
                R"("m/no": "mno",)"
                R"("x\"y'z": "xyz")"
            R"(})"
        R"(})" };

    jsoncons::json doc = jsoncons::json::parse(json);

    auto flat_doc = jsoncons::jsonpath::flatten(doc);

    for (const auto& member : flat_doc.object_range())
    {
        const auto& path = member.key();
        const auto& value = member.value().as<std::string>();
        compare_match(doc, path, value);
    }
}
