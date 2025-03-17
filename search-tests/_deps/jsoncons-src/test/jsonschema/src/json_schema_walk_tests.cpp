// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>
#include <jsoncons/json.hpp>

#include <jsoncons/utility/byte_string.hpp>

#include <catch/catch.hpp>
#include <fstream>
#include <iostream>
#include <regex>

using jsoncons::json;
using jsoncons::ojson;
namespace jsonschema = jsoncons::jsonschema;

TEST_CASE("jsonschema walk tests")
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
      "items": {
        "$ref": "#/$defs/veggie"
      }
    }
  },
  "$defs": {
    "veggie": {
      "type": "object",
      "required": [
        "veggieName",
        "veggieLike"
      ],
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

    ojson schema = ojson::parse(schema_str);
    jsonschema::json_schema<ojson> compiled = jsonschema::make_json_schema(std::move(schema)); 

    SECTION("walk")
    {
        std::string data_str = R"(
{
  "fruits": [
    "apple",
    "orange",
    "pear"
  ],
  "vegetables": [
    {
      "veggieName": "potato",
      "veggieLike": true
    },
    {
      "veggieName": "broccoli",
      "veggieLike": false
    }
  ]
}
        )";

        // Data
        ojson data = ojson::parse(data_str);
 
        ojson expected = ojson::parse(R"(      
{
    "/fruits/0": "string",
    "/fruits/1": "string",
    "/fruits/2": "string",
    "/fruits": "array",
    "/vegetables/0/veggieName": "string",
    "/vegetables/0/veggieLike": "boolean",
    "/vegetables/0": "object",
    "/vegetables/1/veggieName": "string",
    "/vegetables/1/veggieLike": "boolean",
    "/vegetables/1": "object",
    "/vegetables": "array",
    "": "object"
}
        )");

        ojson result(jsoncons::json_object_arg);
        auto reporter = [&](const std::string& keyword,
            const ojson& schema, const jsoncons::uri& /*schema_location*/,
            const ojson& /*instance*/, const jsoncons::jsonpointer::json_pointer& instance_location) -> jsonschema::walk_result
        {
            if (keyword == "type" && schema.is_object())
            {
                auto it = schema.find("type");
                if (it != schema.object_range().end())
                {
                    result.try_emplace(instance_location.string(), it->value());
                    //std::cout << instance_location.string() << ": " << it->value() << "\n";
                }
            }
            return jsonschema::walk_result::advance;
        };
        compiled.walk(data, reporter);
        CHECK(expected == result);
        //std::cout << pretty_print(result) << "\n";
    }
} 

TEST_CASE("jsonschema with $dynamicRef walk test")
{
    std::string schema_str = R"(
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://test.json-schema.org/dynamic-ref-leaving-dynamic-scope/main",
    "if": {
        "$id": "first_scope",
        "$defs": {
            "thingy": {
                "$comment": "this is first_scope#thingy",
                "$dynamicAnchor": "thingy",
                "type": "number"
            }
        }
    },
    "then": {
        "$id": "second_scope",
        "$ref": "start",
        "$defs": {
            "thingy": {
                "$comment": "this is second_scope#thingy, the final destination of the $dynamicRef",
                "$dynamicAnchor": "thingy",
                "type": "null"
            }
        }
    },
    "$defs": {
        "start": {
            "$comment": "this is the landing spot from $ref",
            "$id": "start",
            "$dynamicRef": "inner_scope#thingy"
        },
        "thingy": {
            "$comment": "this is the first stop for the $dynamicRef",
            "$id": "inner_scope",
            "$dynamicAnchor": "thingy",
            "type": "string"
        }
    }
}
    )";

    ojson schema = ojson::parse(schema_str);
    jsonschema::json_schema<ojson> compiled = jsonschema::make_json_schema(std::move(schema)); 

    SECTION("walk")
    {
        std::string data_str = R"(null)";

        try
        {
            // Data
            ojson data = ojson::parse(data_str);

            ojson expected = ojson::parse(R"(      
{
    "" : "null"
}
            )");

            ojson result(jsoncons::json_object_arg);
            auto reporter = [&](const std::string& keyword,
                const ojson& schema, const jsoncons::uri& /*schema_location*/,
                const ojson& /*instance*/, const jsoncons::jsonpointer::json_pointer& instance_location) -> jsonschema::walk_result
            {
                if (keyword == "type" && schema.is_object())
                {
                    auto it = schema.find("type");
                    if (it != schema.object_range().end())
                    {
                        result.try_emplace(instance_location.string(), it->value());
                        //std::cout << instance_location.string() << ": " << it->value() << "\n";
                    }
                }
                return jsonschema::walk_result::advance;
            };
            compiled.walk(data, reporter);
            CHECK(expected == result);
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << "\n";
        }
    }
}

TEST_CASE("jsonschema walk keyword test")
{
    SECTION("prefixItems")
    {
        try
        {
            ojson schema = ojson::parse(R"(
    {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "prefixItems": [
            {"type": "integer"},
            {"type": "string"}
        ]
    }        
            )");
            jsonschema::json_schema<ojson> compiled = jsonschema::make_json_schema(std::move(schema)); 

            ojson data = ojson::parse(R"(
[ 1, "foo" ]
            )");

            ojson expected = ojson::parse(R"(      
{
    "/0": "integer",
    "/1": "string"
}
            )");

            ojson result(jsoncons::json_object_arg);
            auto reporter = [&](const std::string& keyword,
                const ojson& schema, const jsoncons::uri& /*schema_location*/,
                const ojson& /*instance*/, const jsoncons::jsonpointer::json_pointer& instance_location) -> jsonschema::walk_result
            {
                if (keyword == "type")
                {
                    REQUIRE(schema.is_object());
                    auto it = schema.find("type");
                    if (it != schema.object_range().end())
                    {
                        result.try_emplace(instance_location.string(), it->value());
                    }
                }
                return jsonschema::walk_result::advance;
            };
            compiled.walk(data, reporter);
            CHECK(expected == result);
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << "\n";
        }
    }
    SECTION("dependentRequired")
    {
        try
        {
            ojson schema = ojson::parse(R"(
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {"bar": ["foo"]}
}
            )");
            jsonschema::json_schema<ojson> compiled = jsonschema::make_json_schema(std::move(schema)); 

            ojson data = ojson::parse(R"(
{"foo": 1, "bar": 2}
            )");

            ojson expected = ojson::parse(R"(      
{"":{"bar":["foo"]}}
            )");

            ojson result(jsoncons::json_object_arg);
            auto reporter = [&](const std::string& keyword,
                const ojson& schema, const jsoncons::uri& /*schema_location*/,
                const ojson& /*instance*/, const jsoncons::jsonpointer::json_pointer& instance_location) -> jsonschema::walk_result
            {
                //std::cout << "keyword: " << keyword << "\n";
                if (keyword == "dependentRequired")
                {
                    REQUIRE(schema.is_object());
                    auto it = schema.find("dependentRequired");
                    if (it != schema.object_range().end())
                    {
                        result.try_emplace(instance_location.string(), it->value());
                    }
                }
                return jsonschema::walk_result::advance;
            };
            compiled.walk(data, reporter);
            CHECK(expected == result);
            //std::cout << result << "\n";
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << "\n";
        }
    }
    SECTION("dependentSchemas")
    {
        try
        {
            ojson schema = ojson::parse(R"(
{
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "dependentSchemas": {
                "bar": {
                    "properties": {
                        "foo": {"type": "integer"},
                        "bar": {"type": "integer"}
                    }
                }
            }
        }
            )");
            jsonschema::json_schema<ojson> compiled = jsonschema::make_json_schema(std::move(schema)); 

            ojson data = ojson::parse(R"(
{"foo": 1, "bar": 2}
            )");

            ojson expected = ojson::parse(R"(      
{"/bar/foo":"integer","/bar/bar":"integer"}
            )");

            ojson result(jsoncons::json_object_arg);
            auto reporter = [&](const std::string& keyword,
                const ojson& schema, const jsoncons::uri& /*schema_location*/,
                const ojson& /*instance*/, const jsoncons::jsonpointer::json_pointer& instance_location) -> jsonschema::walk_result
            {
                //std::cout << "keyword: " << keyword << "\n";
                if (keyword == "type")
                {
                    REQUIRE(schema.is_object());
                    auto it = schema.find("type");
                    if (it != schema.object_range().end())
                    {
                        result.try_emplace(instance_location.string(), it->value());
                    }
                }
                return jsonschema::walk_result::advance;
            };
            compiled.walk(data, reporter);
            CHECK(expected == result);
            //std::cout << result << "\n";
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << "\n";
        }
    }
    SECTION("propertyNames")
    {
        try
        {
            ojson schema = ojson::parse(R"(
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "propertyNames": {"maxLength": 3}
}
            )");
            jsonschema::json_schema<ojson> compiled = jsonschema::make_json_schema(std::move(schema)); 

            ojson data = ojson::parse(R"(
{
    "f": {},
    "foo": {}
}
            )");

            ojson expected = ojson::parse(R"(      
{"/f":3,"/foo":3}
            )");

            ojson result(jsoncons::json_object_arg);
            auto reporter = [&](const std::string& keyword,
                const ojson& schema, const jsoncons::uri& /*schema_location*/,
                const ojson& /*instance*/, const jsoncons::jsonpointer::json_pointer& instance_location) -> jsonschema::walk_result
            {
                //std::cout << "keyword: " << keyword << "\n";
                if (keyword == "maxLength")
                {
                    REQUIRE(schema.is_object());
                    auto it = schema.find("maxLength");
                    if (it != schema.object_range().end())
                    {
                        result.try_emplace(instance_location.string(), it->value());
                    }
                }
                return jsonschema::walk_result::advance;
            };
            compiled.walk(data, reporter);
            CHECK(expected == result);
            //std::cout << result << "\n";
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << "\n";
        }
    }
    SECTION("contains")
    {
        try
        {
            ojson schema = ojson::parse(R"(
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {"minimum": 5}
}
            )");
            jsonschema::json_schema<ojson> compiled = jsonschema::make_json_schema(std::move(schema)); 

            ojson data = ojson::parse(R"(
[3, 4, 5]
            )");

            ojson expected = ojson::parse(R"(      
{"/0":5,"/1":5,"/2":5}
            )");

            ojson result(jsoncons::json_object_arg);
            auto reporter = [&](const std::string& keyword,
                const ojson& schema, const jsoncons::uri& /*schema_location*/,
                const ojson& /*instance*/, const jsoncons::jsonpointer::json_pointer& instance_location) -> jsonschema::walk_result
            {
                //std::cout << "keyword: " << keyword << "\n";
                if (keyword == "minimum")
                {
                    REQUIRE(schema.is_object());
                    auto it = schema.find("minimum");
                    if (it != schema.object_range().end())
                    {
                        result.try_emplace(instance_location.string(), it->value());
                    }
                }
                return jsonschema::walk_result::advance;
            };
            compiled.walk(data, reporter);
            CHECK(expected == result);
            //std::cout << result << "\n";
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << "\n";
        }
    }
    SECTION("patternProperties")
    {
        try
        {
            ojson schema = ojson::parse(R"(
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "patternProperties": {
        "f.*o": {"type": "integer"}
    }
}
            )");
            jsonschema::json_schema<ojson> compiled = jsonschema::make_json_schema(std::move(schema)); 

            ojson data = ojson::parse(R"(
{"foo": 1, "foooooo" : 2}
            )");

            ojson expected = ojson::parse(R"(      
{"/foo":"integer","/foooooo":"integer"}
            )");

            ojson result(jsoncons::json_object_arg);
            auto reporter = [&](const std::string& keyword,
                const ojson& schema, const jsoncons::uri& /*schema_location*/,
                const ojson& /*instance*/, const jsoncons::jsonpointer::json_pointer& instance_location) -> jsonschema::walk_result
            {
                //std::cout << "keyword: " << keyword << "\n";
                if (keyword == "type")
                {
                    REQUIRE(schema.is_object());
                    auto it = schema.find("type");
                    if (it != schema.object_range().end())
                    {
                        result.try_emplace(instance_location.string(), it->value());
                    }
                }
                return jsonschema::walk_result::advance;
            };
            compiled.walk(data, reporter);
            CHECK(expected == result);
            //std::cout << result << "\n";
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << "\n";
        }
    }
    SECTION("additionalProperties")
    {
        try
        {
            ojson schema = ojson::parse(R"(
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "additionalProperties": {"type": "boolean"}
}
            )");
            jsonschema::json_schema<ojson> compiled = jsonschema::make_json_schema(std::move(schema)); 

            ojson data = ojson::parse(R"(
{"foo" : true}
            )");

            ojson expected = ojson::parse(R"(      
{"/foo":"boolean"}
            )");

            ojson result(jsoncons::json_object_arg);
            auto reporter = [&](const std::string& keyword,
                const ojson& schema, const jsoncons::uri& /*schema_location*/,
                const ojson& /*instance*/, const jsoncons::jsonpointer::json_pointer& instance_location) -> jsonschema::walk_result
            {
                //std::cout << "keyword: " << keyword << "\n";
                if (keyword == "type")
                {
                    REQUIRE(schema.is_object());
                    auto it = schema.find("type");
                    if (it != schema.object_range().end())
                    {
                        result.try_emplace(instance_location.string(), it->value());
                    }
                }
                return jsonschema::walk_result::advance;
            };
            compiled.walk(data, reporter);
            CHECK(expected == result);
            //std::cout << result << "\n";
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << "\n";
        }
    }
    SECTION("additionalItems")
    {
        try
        {
            ojson schema = ojson::parse(R"(
{
    "$schema": "https://json-schema.org/draft/2019-09/schema",
    "items": [{}],
    "additionalItems": {"type": "integer"}
}
            )");
            jsonschema::json_schema<ojson> compiled = jsonschema::make_json_schema(std::move(schema)); 

            ojson data = ojson::parse(R"(
[ null, 2, 3, 4 ]
            )");

            ojson expected = ojson::parse(R"(      
{"/1":"integer","/2":"integer","/3":"integer"}
            )");

            ojson result(jsoncons::json_object_arg);
            auto reporter = [&](const std::string& keyword,
                const ojson& schema, const jsoncons::uri& /*schema_location*/,
                const ojson& /*instance*/, const jsoncons::jsonpointer::json_pointer& instance_location) -> jsonschema::walk_result
            {
                //std::cout << "keyword: " << keyword << "\n";
                if (keyword == "type")
                {
                    REQUIRE(schema.is_object());
                    auto it = schema.find("type");
                    if (it != schema.object_range().end())
                    {
                        result.try_emplace(instance_location.string(), it->value());
                    }
                }
                return jsonschema::walk_result::advance;
            };
            compiled.walk(data, reporter);
            CHECK(expected == result);
            //std::cout << result << "\n";
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << "\n";
        }
    }    
    SECTION("oneOf")
    {
        try
        {
            ojson schema = ojson::parse(R"(
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "oneOf": [
        {
            "type": "integer"
        },
        {
            "minimum": 2
        }
    ]
}
                    )");
            jsonschema::json_schema<ojson> compiled = jsonschema::make_json_schema(std::move(schema)); 

            ojson data = ojson::parse(R"(
1
            )");

            ojson expected = ojson::parse(R"(      
{"":"integer"}
            )");

            ojson result(jsoncons::json_object_arg);
            auto reporter = [&](const std::string& keyword,
                const ojson& schema, const jsoncons::uri& /*schema_location*/,
                const ojson& /*instance*/, const jsoncons::jsonpointer::json_pointer& instance_location) -> jsonschema::walk_result
            {
                //std::cout << "keyword: " << keyword << "\n";
                if (keyword == "type")
                {
                    REQUIRE(schema.is_object());
                    auto it = schema.find("type");
                    if (it != schema.object_range().end())
                    {
                        result.try_emplace(instance_location.string(), it->value());
                    }
                }
                return jsonschema::walk_result::advance;
            };
            compiled.walk(data, reporter);
            CHECK(expected == result);
            //std::cout << result << "\n";
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << "\n";
        }
    }
    SECTION("anyOf")
    {
        try
        {
            ojson schema = ojson::parse(R"(
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "anyOf": [
        {
            "type": "integer"
        },
        {
            "minimum": 2
        }
    ]
}
                    )");
            jsonschema::json_schema<ojson> compiled = jsonschema::make_json_schema(std::move(schema)); 

            ojson data = ojson::parse(R"(
1
            )");

            ojson expected = ojson::parse(R"(      
{"":"integer"}
            )");

            ojson result(jsoncons::json_object_arg);
            auto reporter = [&](const std::string& keyword,
                const ojson& schema, const jsoncons::uri& /*schema_location*/,
                const ojson& /*instance*/, const jsoncons::jsonpointer::json_pointer& instance_location) -> jsonschema::walk_result
            {
                //std::cout << "keyword: " << keyword << "\n";
                if (keyword == "type")
                {
                    REQUIRE(schema.is_object());
                    auto it = schema.find("type");
                    if (it != schema.object_range().end())
                    {
                        result.try_emplace(instance_location.string(), it->value());
                    }
                }
                return jsonschema::walk_result::advance;
            };
            compiled.walk(data, reporter);
            CHECK(expected == result);
            //std::cout << result << "\n";
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << "\n";
        }
    }
    SECTION("allOf")
    {
        try
        {
            ojson schema = ojson::parse(R"(
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "anyOf": [
        {
            "type": "integer"
        },
        {
            "minimum": 2
        }
    ]
}
                    )");
            jsonschema::json_schema<ojson> compiled = jsonschema::make_json_schema(std::move(schema)); 

            ojson data = ojson::parse(R"(
1
            )");

            ojson expected = ojson::parse(R"(      
{"":"integer"}
            )");

            ojson result(jsoncons::json_object_arg);
            auto reporter = [&](const std::string& keyword,
                const ojson& schema, const jsoncons::uri& /*schema_location*/,
                const ojson& /*instance*/, const jsoncons::jsonpointer::json_pointer& instance_location) -> jsonschema::walk_result
            {
                //std::cout << "keyword: " << keyword << "\n";
                if (keyword == "type")
                {
                    REQUIRE(schema.is_object());
                    auto it = schema.find("type");
                    if (it != schema.object_range().end())
                    {
                        result.try_emplace(instance_location.string(), it->value());
                    }
                }
                return jsonschema::walk_result::advance;
            };
            compiled.walk(data, reporter);
            CHECK(expected == result);
            //std::cout << result << "\n";
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << "\n";
        }
    }
}

