// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>
#include <jsoncons/json.hpp>
#include <jsoncons/utility/byte_string.hpp>

#include <fstream>
#include <iostream>
#include <regex>
#include <catch/catch.hpp>

namespace jsonschema = jsoncons::jsonschema;
namespace jsonpatch = jsoncons::jsonpatch;

TEST_CASE("jsonschema custom message tests")
{
    std::string schema_str = R"(
{
    "type": "object",
    "properties": {
        "date": {
          "type": "string",
          "format": "date"
        },
        "foo": {
            "type": "array",
            "maxItems": 3,
            "items" : {
                "type" : "number"
            },
            "errorMessage" : {
                "maxItems" : "At most 3 numbers are allowed in 'foo'",
                "type" : "Only numbers are allowed in 'foo'"
            }
        },
        "bar": {
            "type": "string",
            "errorMessage" : "Type of `bar` must be string"    
        }
    },
    "errorMessage": {
        "format.date": "Date format must be YYYY-MM-DD"
    }
}
    )";
    
    auto options = jsonschema::evaluation_options{}
        .enable_custom_error_message(true)
        .require_format_validation(true);

    auto schema = jsoncons::json::parse(schema_str);    
    auto compiled = jsonschema::make_json_schema<jsoncons::json>(schema, options); 
    
    SECTION("test 1")
    {
        std::string data_str = R"(
{
    "foo": [1, 2, "three"],
    "bar": 123,        
    "date": "05-13-1955"
}        
        )";

        auto data = jsoncons::json::parse(data_str);

        std::vector<std::string> messages;
        auto reporter = [&](const jsonschema::validation_message& msg) -> jsonschema::walk_result
        {
            messages.push_back(msg.message());
            return jsonschema::walk_result::advance;
        };
        compiled.validate(data, reporter);

        REQUIRE(messages.size() == 3);
        CHECK("Type of `bar` must be string" == messages[0]);
        CHECK("Date format must be YYYY-MM-DD" == messages[1]);
        CHECK("Only numbers are allowed in 'foo'" == messages[2]);
    }

    SECTION("test 2")
    {
        std::string data_str = R"(
{
    "foo": [1, 2, "text"],
    "bar": "Bar 1"
}        
        )";

        auto data = jsoncons::json::parse(data_str);

        std::vector<std::string> messages;
        auto reporter = [&](const jsonschema::validation_message& msg) -> jsonschema::walk_result
        {
            messages.push_back(msg.message());
            return jsonschema::walk_result::advance;
        };
        compiled.validate(data, reporter);

        REQUIRE(messages.size() == 1);
        CHECK("Only numbers are allowed in 'foo'" == messages[0]);
    }

    SECTION("test 3")
    {
        std::string data_str = R"(
{
    "foo": [1, 2, "text"],
    "bar": 123
}        
        )";

        auto data = jsoncons::json::parse(data_str);

        std::vector<std::string> messages;
        auto reporter = [&](const jsonschema::validation_message& msg) -> jsonschema::walk_result
        {
            messages.push_back(msg.message());
            return jsonschema::walk_result::advance;
        };
        compiled.validate(data, reporter);
        
        REQUIRE(messages.size() == 2);
        CHECK("Type of `bar` must be string" == messages[0]);
        CHECK("Only numbers are allowed in 'foo'" == messages[1]);
    }

    SECTION("test 4")
    {
        std::string data_str = R"(
{
            "foo": [1, 2, "text", 3],
            "bar": 123
}        
        )";

        auto data = jsoncons::json::parse(data_str);

        std::vector<std::string> messages;
        auto reporter = [&](const jsonschema::validation_message& msg) -> jsonschema::walk_result
        {
            messages.push_back(msg.message());
            return jsonschema::walk_result::advance;
        };
        compiled.validate(data, reporter);

        REQUIRE(messages.size() == 3);
        CHECK("Type of `bar` must be string" == messages[0]);
        CHECK("Only numbers are allowed in 'foo'" == messages[1]);
        CHECK("At most 3 numbers are allowed in 'foo'" == messages[2]);
    }
}

TEST_CASE("jsonschema custom message with format keyword")
{
    std::string schema_str = R"(
{
  "type": "object",
  "properties": {
    "date": {
      "type": "string",
      "format": "date"
    },
    "date-time": {
      "type": "string",
      "format": "date-time",
      "errorMessage": "Date-time format must be YYYY-MM-DDThh:mmTZD"
    }
  },
  "errorMessage": {
    "format.date": "Date format must be YYYY-MM-DD"
  }
}
    )";
    
    auto options = jsonschema::evaluation_options{}
        .enable_custom_error_message(true)
        .require_format_validation(true);

    auto schema = jsoncons::json::parse(schema_str);    
    auto compiled = jsonschema::make_json_schema<jsoncons::json>(schema, options); 
    
    SECTION("test 1")
    {
        std::string data_str = R"(
{
    "date": "05-13-1955",
    "date-time": "1955-05-13"
}
        )";

        auto data = jsoncons::json::parse(data_str);

        std::vector<std::string> messages;
        auto reporter = [&](const jsonschema::validation_message& msg) -> jsonschema::walk_result
        {
            messages.push_back(msg.message());
            return jsonschema::walk_result::advance;
        };
        compiled.validate(data, reporter);

        REQUIRE(messages.size() == 2);
        CHECK("Date format must be YYYY-MM-DD" == messages[0]);
        CHECK("Date-time format must be YYYY-MM-DDThh:mmTZD" == messages[1]);
    }
}

