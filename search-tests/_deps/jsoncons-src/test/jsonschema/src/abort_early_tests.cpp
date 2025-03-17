// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <jsoncons/json.hpp>

#include <catch/catch.hpp>
#include <iostream>

using jsoncons::ojson;
namespace jsonschema = jsoncons::jsonschema;

TEST_CASE("jsonschema stop early tests")
{
    std::string schema_str = R"(
{
  "$id": "https://example.com/arrays.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "description": "A representation of a person, company, organization, or place",
  "type": "object",
  "properties": {
    "fruits": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "vegetables": {
      "type": "array",
      "items": { "$ref": "#/$defs/veggie" }
    }
  },
  "$defs": {
    "veggie": {
      "type": "object",
      "required": [ "veggieName", "veggieLike" ],
      "properties": {
        "veggieName": {
          "type": "string",
          "description": "The name of the vegetable."
        },
        "veggieLike": {
          "type": "boolean",
          "description": "Do I like this vegetable?"
        }
      }
    }
  }
}
  )";

    std::string data_str = R"(
{
  "fruits": [ "apple", "orange", "pear" ],
  "vegetables": [
    {
      "veggieName": "potato",
      "veggieLike": true
    },
    {
      "veggieName": "broccoli",
      "veggieLike": "false"
    },
    {
      "veggieName": "carrot",
      "veggieLike": false
    },
    {
      "veggieName": "Swiss Chard"
    }
  ]
}
    )";

    ojson schema = ojson::parse(schema_str);
    jsonschema::json_schema<ojson> compiled = jsonschema::make_json_schema(std::move(schema));
    ojson data = ojson::parse(data_str);

    SECTION("test 1")
    {
        std::string expected_str = R"(
{
    "/vegetables/1/veggieLike": "Expected boolean, found string",
    "/vegetables/3": "Required property 'veggieLike' not found."
}
        )";

        ojson expected = ojson::parse(expected_str);
        ojson results{ jsoncons::json_object_arg };
        auto reporter = [&](const jsonschema::validation_message& message) -> jsonschema::walk_result
            {
                results.try_emplace(message.instance_location().string(), message.message());
                return jsonschema::walk_result::advance;
            };
        compiled.validate(data, reporter);
        CHECK(expected == results);
        //std::cout << pretty_print(results) << "\n";
    }

    SECTION("test 2")
    {
        std::string expected_str = R"(
{
    "/vegetables/1/veggieLike": "Expected boolean, found string"
}
        )";

        ojson expected = ojson::parse(expected_str);
        ojson results{jsoncons::json_object_arg};
        auto reporter = [&](const jsonschema::validation_message& message) -> jsonschema::walk_result
            {
                results.try_emplace(message.instance_location().string(), message.message());
                return jsonschema::walk_result::abort;
            };
        compiled.validate(data, reporter);
        CHECK(expected == results);
        //std::cout << pretty_print(results) << "\n";
    }
}

