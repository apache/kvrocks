// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <jsoncons/json.hpp>
#include <jsoncons/utility/byte_string.hpp>

#include <fstream>
#include <iostream>
#include <regex>
#include <catch/catch.hpp>

using jsoncons::json;
namespace jsonschema = jsoncons::jsonschema;

TEST_CASE("jsonschema validator tests")
{
    SECTION("Basic")
    {
        json schema = json::parse(R"(
{
  "$id": "https://example.com/polygon",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$defs": {
    "point": {
      "type": "object",
      "properties": {
        "x": { "type": "number" },
        "y": { "type": "number" }
      },
      "additionalProperties": false,
      "required": [ "x", "y" ]
    }
  },
  "type": "array",
  "items": { "$ref": "#/$defs/point" },
  "minItems": 3,
  "maxItems": 1
}

        )");

        json instance = json::parse(R"(
[
  {
    "x": 2.5,
    "y": 1.3
  },
  {
    "x": 1,
    "z": 6.7
  }
]
        )");

        jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema);

        CHECK_FALSE(compiled.is_valid(instance));
    }
}

/*
: Expected minimum item count: 3, found: 2
/1: Required key "y" not found
/1: Validation failed for additional property "z". False schema always fails
*/
