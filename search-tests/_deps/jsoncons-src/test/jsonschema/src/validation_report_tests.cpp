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
using jsoncons::ojson;
namespace jsonschema = jsoncons::jsonschema;

TEST_CASE("jsonschema validation report tests")
{
    json schema = json::parse(R"(
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://test.com/schema",
    "$defs": {
        "integer": {
            "type": "integer"
        },      
        "minimum": {
            "minimum": 5
        }      
    },
    "type" : "object",
    "properties" : {
        "passes" : true,
        "fails" : false,
        "refs" : {"$ref" : "#/$defs/integer"},
        "multi" : {
            "allOf" : [{"$ref" : "#/$defs/integer"},{"$ref" : "#/$defs/minimum"}]
        }
    }
}
    )");

    SECTION("Test 1")
    {
        ojson expected = ojson::parse(R"(
[
    {
        "valid": false,
        "evaluationPath": "/properties/fails",
        "schemaLocation": "https://test.com/schema#/properties/fails",
        "instanceLocation": "/fails",
        "error": "False schema always fails"
    }
]
        )");

        jsoncons::json_decoder<ojson> decoder;    
        jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema);
    
        json data = json::parse(R"({"fails":"value"})");
    
        compiled.validate(data, decoder);
        
        ojson output = decoder.get_result();
        CHECK(expected == output);
        //std::cout << pretty_print(output) << "\n";
    }
    SECTION("Test 2")
    {
        ojson expected = ojson::parse(R"(
[
    {
        "valid": false,
        "evaluationPath": "/properties/multi/allOf",
        "schemaLocation": "https://test.com/schema#/properties/multi/allOf",
        "instanceLocation": "/multi",
        "error": "Must be valid against all schemas, but found unmatched schemas",
        "details": [
            {
                "valid": false,
                "evaluationPath": "/properties/multi/allOf/0/$ref/type",
                "schemaLocation": "https://test.com/schema#/$defs/integer",
                "instanceLocation": "/multi",
                "error": "Expected integer, found number"
            },
            {
                "valid": false,
                "evaluationPath": "/properties/multi/allOf/1/$ref/minimum",
                "schemaLocation": "https://test.com/schema#/$defs/minimum/minimum",
                "instanceLocation": "/multi",
                "error": "Minimum value is 5 but found 3.5"
            }
        ]
    }
]
        )");

        jsoncons::json_decoder<ojson> decoder;    
        jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema);

        json data = json::parse(R"({"multi":3.5})");

        compiled.validate(data, decoder);

        ojson output = decoder.get_result();
        CHECK(expected == output);
        //std::cout << pretty_print(output) << "\n";
    }
}

TEST_CASE("jsonschema prefixItems report tests")
{
    json schema = json::parse(R"(
{
  "type": "array",
  "prefixItems": [
    { "type": "number" },
    { "type": "string" },
    { "enum": ["Street", "Avenue", "Boulevard"] },
    { "enum": ["NW", "NE", "SW", "SE"] }
  ],
  "items": false
}
    )");

    SECTION("Test 1")
    {
        ojson expected = ojson::parse(R"(
[
    {
        "valid": false,
        "evaluationPath": "/items",
        "schemaLocation": "https://jsoncons.com#/items",
        "instanceLocation": "/4",
        "error": "Extra item at index '4' but the schema does not allow extra items."
    }
]
        )");

        jsoncons::json_decoder<ojson> decoder;    
        jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema);
    
        json data = json::parse(R"(
            [1600, "Pennsylvania", "Avenue", "NW", "Washington"]
        )");
    
        compiled.validate(data, decoder);
        
        ojson output = decoder.get_result();
        CHECK(expected == output);
        //std::cout << pretty_print(output) << "\n";
    }
}

TEST_CASE("jsonschema items-additionalItems report tests")
{
    json schema = json::parse(R"(
{
  "$schema": "https://json-schema.org/draft/2019-09/schema",
  "type": "array",
  "items": [
    { "type": "number" },
    { "type": "string" },
    { "enum": ["Street", "Avenue", "Boulevard"] },
    { "enum": ["NW", "NE", "SW", "SE"] }
  ],
  "additionalItems": false
}
    )");

    SECTION("Test 1")
    {
        ojson expected = ojson::parse(R"(
[
    {
        "valid": false,
        "evaluationPath": "/additionalItems",
        "schemaLocation": "https://jsoncons.com#/additionalItems",
        "instanceLocation": "/4",
        "error": "Extra item at index '4' but the schema does not allow extra items."
    }
]
        )");

        jsoncons::json_decoder<ojson> decoder;    
        jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema);
    
        json data = json::parse(R"(
            [1600, "Pennsylvania", "Avenue", "NW", "Washington"]
        )");
    
        compiled.validate(data, decoder);
        
        ojson output = decoder.get_result();
        CHECK(expected == output);
        //std::cout << pretty_print(output) << "\n";
    }
}

TEST_CASE("jsonschema additionalProperties output tests")
{
    json schema = json::parse(R"(
{
  "type": "object",
  "properties": {
    "number": { "type": "number" },
    "street_name": { "type": "string" },
    "street_type": { "enum": ["Street", "Avenue", "Boulevard"] }
  },
  "additionalProperties": false
}
    )");

    SECTION("Test 1")
    {
        ojson expected = ojson::parse(R"(
[
    {
        "valid": false,
        "evaluationPath": "/additionalProperties/direction",
        "schemaLocation": "https://jsoncons.com#/additionalProperties",
        "instanceLocation": "/direction",
        "error": "Additional property 'direction' not allowed by schema."
    }
]
        )");

        jsoncons::json_decoder<ojson> decoder;    
        jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema);
    
        json data = json::parse(R"(
{ "number": 1600, "street_name": "Pennsylvania", "street_type": "Avenue", "direction": "NW" }
)");
    
        compiled.validate(data, decoder);
        
        ojson output = decoder.get_result();
        CHECK(expected == output);
        //std::cout << pretty_print(output) << "\n";
    }
}

TEST_CASE("jsonschema unevaluatedProperties output tests")
{
    json schema = json::parse(R"(
{
  "allOf": [
    {
      "type": "object",
      "properties": {
        "street_address": { "type": "string" },
        "city": { "type": "string" },
        "state": { "type": "string" }
      },
      "required": ["street_address", "city", "state"]
    }
  ],

  "properties": {
    "type": { "enum": ["residential", "business"] }
  },
  "required": ["type"],
  "unevaluatedProperties": false
}    
)");

    SECTION("Test 1")
    {
        ojson expected = ojson::parse(R"(
[
    {
        "valid": false,
        "evaluationPath": "/unevaluatedProperties/something that doesn't belong",
        "schemaLocation": "https://jsoncons.com",
        "instanceLocation": "/something that doesn't belong",
        "error": "Unevaluated property 'something that doesn't belong' but the schema does not allow unevaluated properties."
    }
]
        )");

        jsoncons::json_decoder<ojson> decoder;    
        jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema);
    
        json data = json::parse(R"(
{
  "street_address": "1600 Pennsylvania Avenue NW",
  "city": "Washington",
  "state": "DC",
  "type": "business",
  "something that doesn't belong": "hi!"
}
        )");
    
        compiled.validate(data, decoder);
        
        ojson output = decoder.get_result();
        CHECK(expected == output);
    }
}

TEST_CASE("jsonschema unevaluatedItems output tests")
{
    json schema = json::parse(R"(
{
  "prefixItems": [
    { "type": "string" }, { "type": "number" }
  ],
  "unevaluatedItems": false
}
)");

    SECTION("Test 1")
    {
        ojson expected = ojson::parse(R"(
[
    {
        "valid": false,
        "evaluationPath": "/unevaluatedProperties/2",
        "schemaLocation": "https://jsoncons.com",
        "instanceLocation": "/2",
        "error": "Unevaluated item at index '2' but the schema does not allow unevaluated items."
    }
]
        )");

        jsoncons::json_decoder<ojson> decoder;    
        jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema);
    
        json data = json::parse(R"(
["foo", 42, null]
        )");
    
        compiled.validate(data, decoder);
        
        ojson output = decoder.get_result();
        CHECK(expected == output);
        //std::cout << pretty_print(output) << "\n";
    }
}

TEST_CASE("jsonschema items output tests")
{
   std::string schema_str = R"(
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "definitions": {
    "path": {
      "description": "A JSON Pointer path.",
      "type": "string"
    }
  },
  "id": "https://json.schemastore.org/json-patch.json",
  "items": {
    "oneOf": [
      {
        "additionalProperties": false,
        "required": ["value", "op", "path"],
        "properties": {
          "path": {
            "$ref": "#/definitions/path"
          },
          "op": {
            "description": "The operation to perform.",
            "type": "string",
            "enum": ["add", "replace", "test"]
          },
          "value": {
            "description": "The value to add, replace or test."
          }
        }
      },
      {
        "additionalProperties": false,
        "required": ["op", "path"],
        "properties": {
          "path": {
            "$ref": "#/definitions/path"
          },
          "op": {
            "description": "The operation to perform.",
            "type": "string",
            "enum": ["remove"]
          }
        }
      },
      {
        "additionalProperties": false,
        "required": ["from", "op", "path"],
        "properties": {
          "path": {
            "$ref": "#/definitions/path"
          },
          "op": {
            "description": "The operation to perform.",
            "type": "string",
            "enum": ["move", "copy"]
          },
          "from": {
            "$ref": "#/definitions/path",
            "description": "A JSON Pointer path pointing to the location to move/copy from."
          }
        }
      }
    ]
  },
  "title": "JSON schema for JSONPatch files",
  "type": "array"
}
)";
    SECTION("Test 1")
    {
        ojson expected = ojson::parse(R"(
[
    {
        "valid": false,
        "evaluationPath": "/items/oneOf",
        "schemaLocation": "https://json.schemastore.org/json-patch.json#/items/oneOf",
        "instanceLocation": "/0",
        "error": "Must be valid against exactly one schema, but found no matching schemas",
        "details": [
            {
                "valid": false,
                "evaluationPath": "/items/oneOf/0/properties/op/enum",
                "schemaLocation": "https://json.schemastore.org/json-patch.json#/items/oneOf/0/properties/op/enum",
                "instanceLocation": "/0/op",
                "error": "'invalid_op' is not a valid enum value."
            },
            {
                "valid": false,
                "evaluationPath": "/items/oneOf/1/properties/op/enum",
                "schemaLocation": "https://json.schemastore.org/json-patch.json#/items/oneOf/1/properties/op/enum",
                "instanceLocation": "/0/op",
                "error": "'invalid_op' is not a valid enum value."
            },
            {
                "valid": false,
                "evaluationPath": "/items/oneOf/1/additionalProperties/value",
                "schemaLocation": "https://json.schemastore.org/json-patch.json#/items/oneOf/1/additionalProperties",
                "instanceLocation": "/0/value",
                "error": "Additional property 'value' not allowed by schema."
            },
            {
                "valid": false,
                "evaluationPath": "/items/oneOf/2/required",
                "schemaLocation": "https://json.schemastore.org/json-patch.json#/items/oneOf/2/required",
                "instanceLocation": "/0",
                "error": "Required property 'from' not found."
            },
            {
                "valid": false,
                "evaluationPath": "/items/oneOf/2/properties/op/enum",
                "schemaLocation": "https://json.schemastore.org/json-patch.json#/items/oneOf/2/properties/op/enum",
                "instanceLocation": "/0/op",
                "error": "'invalid_op' is not a valid enum value."
            },
            {
                "valid": false,
                "evaluationPath": "/items/oneOf/2/additionalProperties/value",
                "schemaLocation": "https://json.schemastore.org/json-patch.json#/items/oneOf/2/additionalProperties",
                "instanceLocation": "/0/value",
                "error": "Additional property 'value' not allowed by schema."
            }
        ]
    }
]
        )");

        std::string data_str = R"(
[
    {
        "op": "invalid_op",
        "path": "/biscuits/1",
        "value":{"name":"Ginger Nut" }
    }
]
        )";   
        auto schema_ = jsoncons::ojson::parse(schema_str);
        auto data_ = jsoncons::ojson::parse(data_str);
        auto compiled = jsoncons::jsonschema::make_json_schema(schema_);
        jsoncons::json_decoder<jsoncons::ojson> decoder;
        compiled.validate(data_, decoder);
        auto output = decoder.get_result();        
        CHECK(expected == output);
        //std::cout << pretty_print(output) << "\n";
    }
}
TEST_CASE("jsonschema more output tests 2")
{
    json schema = json::parse(R"(
{
  "$id":"http://schemarepo.org/schemas/user.json",
  "$schema":"http://json-schema.org/draft-07/schema#",
  "type":"object",
  "definitions":{
    "min18":{
      "type":"integer",
      "minimum":18
    },
    "username":{
      "type":"string",
      "minLength":8
    },
    "member":{
      "type":"object",
      "properties":{
        "age":{"$ref":"#/definitions/min18"},
        "username":{"$ref":"#/definitions/username"}
      }
    },
    "membershipTypes":{"enum":["admin","user"]}
  },
  "oneOf":[
    {
      "properties":{
        "member":{"$ref":"#/definitions/member"},
        "membershipType":{"$ref":"#/definitions/membershipTypes"}
      }
    },
    {
      "properties":{
        "membershipType":{"const":"guest"},
        "firstName":{"type":"string"},
        "lastName":{"type":"string"}
      },
      "additionalProperties":false
    }
  ]
}
        )");

    SECTION("With ref")
    {
        json data = json::parse(R"(
{
  "member":{
      "age":5,  // doesn't meet minimum
      "username":"aName"  // doesn't meet minLength
  },
  "membershipType":"user"
}
        )");
            
        ojson expected = ojson::parse(R"(
[
    {
        "valid": false,
        "evaluationPath": "/oneOf",
        "schemaLocation": "http://schemarepo.org/schemas/user.json#/oneOf",
        "instanceLocation": "",
        "error": "Must be valid against exactly one schema, but found no matching schemas",
        "details": [
            {
                "valid": false,
                "evaluationPath": "/oneOf/0/properties/member/$ref/properties/age/$ref/minimum",
                "schemaLocation": "http://schemarepo.org/schemas/user.json#/definitions/min18/minimum",
                "instanceLocation": "/member/age",
                "error": "Minimum value is 18 but found 5"
            },
            {
                "valid": false,
                "evaluationPath": "/oneOf/0/properties/member/$ref/properties/username/$ref/minLength",
                "schemaLocation": "http://schemarepo.org/schemas/user.json#/definitions/username/minLength",
                "instanceLocation": "/member/username",
                "error": "Number of characters must be at least 8"
            },
            {
                "valid": false,
                "evaluationPath": "/oneOf/1/properties/membershipType/const",
                "schemaLocation": "http://schemarepo.org/schemas/user.json#/oneOf/1/properties/membershipType/const",
                "instanceLocation": "/membershipType",
                "error": "Instance is not const"
            },
            {
                "valid": false,
                "evaluationPath": "/oneOf/1/additionalProperties/member",
                "schemaLocation": "http://schemarepo.org/schemas/user.json#/oneOf/1/additionalProperties",
                "instanceLocation": "/member",
                "error": "Additional property 'member' not allowed by schema."
            }
        ]
    }
]
        )");           

        auto compiled = jsoncons::jsonschema::make_json_schema(schema);
        jsoncons::json_decoder<jsoncons::ojson> decoder;
        compiled.validate(data, decoder);
        auto output = decoder.get_result();        
        CHECK(expected == output);
        //std::cout << pretty_print(output) << "\n";
    }
}

TEST_CASE("jsonschema more output tests 3")
{
    json schema = json::parse(R"(
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "example-schema",
  "type": "object",
  "title": "foo object schema",
  "properties": {
    "foo": {
      "title": "foo's title",
      "description": "foo's description",
      "type": "string",
      "pattern": "^foo ",
      "minLength": 10
    }
  },
  "required": [ "foo" ],
  "additionalProperties": false
}
    )");

    SECTION("With ref")
    {
        json data = json::parse(R"(
{
  "baz": 42
}
        )");
            
        ojson expected = ojson::parse(R"(
[
    {
        "valid": false,
        "evaluationPath": "/additionalProperties/baz",
        "schemaLocation": "https://jsoncons.com/example-schema#/additionalProperties",
        "instanceLocation": "/baz",
        "error": "Additional property 'baz' not allowed by schema."
    },
    {
        "valid": false,
        "evaluationPath": "/required",
        "schemaLocation": "https://jsoncons.com/example-schema#/required",
        "instanceLocation": "",
        "error": "Required property 'foo' not found."
    }
]
        )");           

        auto compiled = jsoncons::jsonschema::make_json_schema(schema);
        jsoncons::json_decoder<jsoncons::ojson> decoder;
        compiled.validate(data, decoder);
        auto output = decoder.get_result();        
        CHECK(expected == output);
        //std::cout << pretty_print(output) << "\n";
    }
}

TEST_CASE("jsonschema more output tests")
{
    json schema = json::parse(R"(
{
  "$id": "https://example.com/polygon",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
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
  "minItems": 3
}
    )");
    
    SECTION("Test 1")
    {
        ojson expected = ojson::parse(R"(
[
    {
        "valid": false,
        "evaluationPath": "/items/$ref/additionalProperties/z",
        "schemaLocation": "https://example.com/polygon#/$defs/point/additionalProperties",
        "instanceLocation": "/1/z",
        "error": "Additional property 'z' not allowed by schema."
    },
    {
        "valid": false,
        "evaluationPath": "/items/$ref/required",
        "schemaLocation": "https://example.com/polygon#/$defs/point/required",
        "instanceLocation": "/1",
        "error": "Required property 'y' not found."
    },
    {
        "valid": false,
        "evaluationPath": "/minItems",
        "schemaLocation": "https://example.com/polygon#/minItems",
        "instanceLocation": "",
        "error": "Minimum number of items is 3 but found 2"
    }
]
        )");


        json data = json::parse(R"(
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
            
        auto compiled = jsoncons::jsonschema::make_json_schema(schema);
        jsoncons::json_decoder<jsoncons::ojson> decoder;
        compiled.validate(data, decoder);
        auto output = decoder.get_result();        
        CHECK(expected == output);
        //std::cout << pretty_print(output) << "\n";
    }
}

TEST_CASE("jsonschema additionalProperties with 'not true' output tests")
{
    json schema = json::parse(R"(
{
  "type": "object",
  "properties": {
    "number": { "type": "number" },
    "street_name": { "type": "string" },
    "street_type": { "enum": ["Street", "Avenue", "Boulevard"] }
  },
  "additionalProperties": {"not" : true}
}
    )");

    SECTION("Test 1")
    {
        ojson expected = ojson::parse(R"(
[
    {
        "valid": false,
        "evaluationPath": "/additionalProperties/direction",
        "schemaLocation": "https://jsoncons.com#/additionalProperties",
        "instanceLocation": "/direction",
        "error": "Additional property 'direction' not allowed by schema."
    }
]
        )");

        jsoncons::json_decoder<ojson> decoder;    
        jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema);
    
        json data = json::parse(R"(
{ "number": 1600, "street_name": "Pennsylvania", "street_type": "Avenue", "direction": "NW" }
)");
    
        compiled.validate(data, decoder);
        
        ojson output = decoder.get_result();
        CHECK(expected == output);
        //std::cout << pretty_print(output) << "\n";
    }
}

TEST_CASE("jsonschema additionalProperties with 'not {}' output tests")
{
    json schema = json::parse(R"(
{
  "type": "object",
  "properties": {
    "number": { "type": "number" },
    "street_name": { "type": "string" },
    "street_type": { "enum": ["Street", "Avenue", "Boulevard"] }
  },
  "additionalProperties": {"not" : {}}
}
    )");

    SECTION("Test 1")
    {
        ojson expected = ojson::parse(R"(
[
    {
        "valid": false,
        "evaluationPath": "/additionalProperties/direction",
        "schemaLocation": "https://jsoncons.com#/additionalProperties",
        "instanceLocation": "/direction",
        "error": "Additional property 'direction' not allowed by schema."
    }
]
        )");

        jsoncons::json_decoder<ojson> decoder;    
        jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema);
    
        json data = json::parse(R"(
{ "number": 1600, "street_name": "Pennsylvania", "street_type": "Avenue", "direction": "NW" }
)");
    
        compiled.validate(data, decoder);
        
        ojson output = decoder.get_result();
        CHECK(expected == output);
        //std::cout << pretty_print(output) << "\n";
    }
}

TEST_CASE("jsonschema with 'oneOf' output tests")
{
    json schema = json::parse(R"(
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "/test_schema",
  "type": "object",
  "properties": {
    "One": {
      "type": "string"
    },
    "Two" : {
      "type": "string"
    }
  },
  "oneOf" : [
    {
      "required" : ["One"]
    },
    {
      "required" : ["Two"]
    }
  ],
  "unevaluatedProperties" : false
}
    )");

    SECTION("Test 1")
    {
        ojson expected = ojson::parse(R"(
[
    {
        "valid": false,
        "evaluationPath": "/oneOf",
        "schemaLocation": "https://jsoncons.com/test_schema#/oneOf",
        "instanceLocation": "",
        "error": "Must be valid against exactly one schema, but found 2 matching schemas at indices 0,1"
    }
]
        )");

        jsoncons::json_decoder<ojson> decoder;    
        jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema);
    
        json data = json::parse(R"(
{
    "One" : "test",
    "Two" : "test"
}
)");
    
        compiled.validate(data, decoder);
        
        ojson output = decoder.get_result();
        CHECK(expected == output);
        //std::cout << pretty_print(output) << "\n";
    }
}
