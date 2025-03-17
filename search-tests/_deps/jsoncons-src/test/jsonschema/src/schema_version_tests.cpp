// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <jsoncons/json.hpp>

#include <fstream>
#include <iostream>
#include <regex>
#include <catch/catch.hpp>

using jsoncons::json;
namespace jsonschema = jsoncons::jsonschema;

TEST_CASE("jsonschema version tests")
{
    json schema_03 = json::parse(R"(
{
    "$schema": "http://json-schema.org/draft-03/schema#",
    "description": "A product from Acme's catalog",
    "properties": {
      "id": {
        "description": "The unique identifier for a product",
        "type": "integer"
      },
      "name": {
        "description": "Name of the product",
        "type": "string"
      },
      "price": {
        "exclusiveMinimum": true,
        "minimum": 0,
        "type": "number"
      },
      "tags": {
        "items": {
          "type": "string"
        },
        "minItems": 1,
        "type": "array",
        "uniqueItems": true
      }
    },
    "required": ["id", "name", "price"],
    "title": "Product",
    "type": "object"
  }
      )");

    json schema_07 = json::parse(R"(
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "description": "A product from Acme's catalog",
    "properties": {
      "id": {
        "description": "The unique identifier for a product",
        "type": "integer"
      },
      "name": {
        "description": "Name of the product",
        "type": "string"
      },
      "price": {
        "exclusiveMinimum": true,
        "minimum": 0,
        "type": "number"
      },
      "tags": {
        "items": {
          "type": "string"
        },
        "minItems": 1,
        "type": "array",
        "uniqueItems": true
      }
    },
    "required": ["id", "name", "price"],
    "title": "Product",
    "type": "object"
  }
      )");

    SECTION("test 3")
    {
        REQUIRE_THROWS_WITH(jsonschema::make_json_schema(schema_03), "Unsupported schema version http://json-schema.org/draft-03/schema#");
    }

    SECTION("test 7")
    {
        REQUIRE_THROWS_WITH(jsonschema::make_json_schema(schema_07), "https://jsoncons.com#/properties/price/exclusiveMinimum: exclusiveMinimum must be a number value");
    }
}

