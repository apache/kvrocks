// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>

#include <jsoncons/json.hpp>

#include <iostream>
#include <fstream>
#include <string>

// for brevity
using jsoncons::json;
using jsoncons::ojson;
namespace jsonschema = jsoncons::jsonschema;
namespace jsonpatch = jsoncons::jsonpatch; 

void validate_three_ways() 
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
        
    std::cout << "\n(1) Validate using exceptions\n";
    try
    {
        compiled.validate(data);
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << "\n";
    }
    
    std::cout << "\n(2) Validate using reporter callback\n";
    auto reporter = [](const jsonschema::validation_message& msg) -> jsonschema::walk_result
        {
            std::cout << msg.instance_location().string() << ": " << msg.message() << "\n";
            return jsonschema::walk_result::advance;
        };
    compiled.validate(data, reporter);
    
    std::cout << "\n(3) Validate outputting to a json decoder\n";
    jsoncons::json_decoder<ojson> decoder;
    compiled.validate(data, decoder);
    ojson output = decoder.get_result();
    std::cout << pretty_print(output) << "\n";
}

// Until 0.174.0, throw a `schema_error` instead of returning json::null() 

void resolve_uri_example()
{ 
    std::string main_schema = R"(
{
    "$id" : "https://www.example.com/main",
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "http://localhost:1234/draft2020-12/object",
    "type": "object",
    "properties": {
        "name": {"$ref": "/name-defs.json#/$defs/orNull"}
    }
}
    )";
    
    std::string root_dir = "./input/jsonschema";
    auto resolver = [&](const jsoncons::uri& uri) -> json
        {
            std::cout << "Requested URI: " << uri.string() << "\n";
            std::cout << "base: " << uri.base().string() << ", path: " << uri.path() << "\n\n";

            std::string pathname = root_dir + uri.path();

            std::fstream is(pathname.c_str());
            if (!is)
            {
                return json::null();
            }

            return json::parse(is);
        };
    
    json schema = json::parse(main_schema);    

    // Data
    json data = json::parse(R"(
{
    "name": {
        "name": null
    }
}
    )");

    try
    {
        // Throws schema_error if JSON Schema compilation fails
        jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema, resolver);

        auto report = [](const jsonschema::validation_message& msg) -> jsonschema::walk_result
            {
                std::cout << msg.instance_location().string() << ": " << msg.message() << "\n";
                for (const auto& detail : msg.details())
                {
                    std::cout << "    "  << detail.message() << "\n";
                }
                return jsonschema::walk_result::advance;
            };

        // Will call report function object for each schema violation
        compiled.validate(data, report);
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << '\n';
    }
}

void defaults_example() 
{
    json schema = json::parse(R"(
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "properties": {
        "bar": {
            "type": "string",
            "minLength": 4,
            "default": "bad"
        }
    }
}
)");

    try
    {
        // Data
        json data = json::parse("{}");

        // will throw schema_error if JSON Schema compilation fails 
        jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema); 

        // will throw a validation_error when a schema violation happens 
        json patch;
        compiled.validate(data, patch); 

        std::cout << "Patch: " << patch << "\n";

        std::cout << "Original data: " << data << "\n";

        jsonpatch::apply_patch(data, patch);

        std::cout << "Patched data: " << data << "\n\n";
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << "\n";
    }
}

#if defined(JSONCONS_HAS_STD_VARIANT)

#include <variant>

namespace ns {

    struct os_properties {
        std::string command;
    };

    struct db_properties {
        std::string query;
    };

    struct api_properties {
        std::string target;
    };

    struct job_properties {
        std::string name;
        std::variant<os_properties,db_properties,api_properties> run;
    };

} // namespace ns

    JSONCONS_N_MEMBER_TRAITS(ns::os_properties, 1, command)
    JSONCONS_N_MEMBER_TRAITS(ns::db_properties, 1, query)
    JSONCONS_N_MEMBER_TRAITS(ns::api_properties, 1, target)
    JSONCONS_N_MEMBER_TRAITS(ns::job_properties, 2, name, run)


void validate_before_decode_example() 
{
    std::string schema_str = R"(
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "job",
  "description": "job properties json schema",
  "$defs": {
    "os_properties": {
      "type": "object",
      "properties": {
        "command": {
          "description": "this is the OS command to run",
          "type": "string",
          "minLength": 1
        }
      },
      "required": [ "command" ],
      "additionalProperties": false
    },
    "db_properties": {
      "type": "object",
      "properties": {
        "query": {
          "description": "this is db query to run",
          "type": "string",
          "minLength": 1
        }
      },
      "required": [ "query" ],
      "additionalProperties": false
    },

    "api_properties": {
      "type": "object",
      "properties": {
        "target": {
          "description": "this is api target to run",
          "type": "string",
          "minLength": 1
        }
      },
      "required": [ "target" ],
      "additionalProperties": false
    }
  },

  "type": "object",
  "properties": {
    "name": {
      "description": "name of the flow",
      "type": "string",
      "minLength": 1
    },
    "run": {
      "description": "job run properties",
      "type": "object",
      "oneOf": [

        { "$ref": "#/$defs/os_properties" },
        { "$ref": "#/$defs/db_properties" },
        { "$ref": "#/$defs/api_properties" }

      ]
    }
  },
  "required": [ "name", "run" ],
  "additionalProperties":  false
}
    )";
    
    std::string data_str = R"(
{
    "name": "testing flow", 
    "run" : {
        "command": "some command"    
    }
}
    
    )";
    try
    {
        json schema = json::parse(schema_str);

        // Throws schema_error if JSON Schema compilation fails
        jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(std::move(schema));

        // Test that input is valid before attempting to decode
        json data = json::parse(data_str);
        if (compiled.is_valid(data))
        {
            const ns::job_properties v = data.as<ns::job_properties>(); 

            std::string output;
            jsoncons::encode_json_pretty(v, output);
            std::cout << output << '\n';

            // Verify that output is valid
            json test = json::parse(output);
            assert(compiled.is_valid(test));
        }
        else
        {
            std::cout << "Invalid input\n";
        }
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << '\n';
    }
}

#endif // JSONCONS_HAS_STD_VARIANT

void draft_201212_example()
{
    json schema = json::parse(R"(
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://test.json-schema.org/typical-dynamic-resolution/root",
    "$ref": "list",
    "$defs": {
        "foo": {
            "$dynamicAnchor": "items",
            "type": "string"
        },
        "list": {
            "$id": "list",
            "type": "array",
            "items": { "$dynamicRef": "#items" },
            "$defs": {
              "items": {
                  "$comment": "This is only needed to satisfy the bookending requirement",
                  "$dynamicAnchor": "items"
              }
            }
        }
    }
}
)");

    jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema);

    json data = json::parse(R"(["foo", 42])");

    jsoncons::json_decoder<ojson> decoder;
    compiled.validate(data, decoder);
    ojson output = decoder.get_result();
    std::cout << pretty_print(output) << "\n\n";
}

void draft_201909_example()
{
    json schema = json::parse(R"(
{
    "$schema": "https://json-schema.org/draft/2019-09/schema",
    "type": "object",
    "properties": {
        "foo": { "type": "string" }
    },
    "allOf": [
        {
            "properties": {
                "bar": { "type": "string" }
            }
        }
    ],
    "unevaluatedProperties": false
}
)");

    jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema);

    json data = json::parse(R"({"foo": "foo","bar": "bar","baz": "baz"})");

    jsoncons::json_decoder<ojson> decoder;
    compiled.validate(data, decoder);
    ojson output = decoder.get_result();
    std::cout << pretty_print(output) << "\n\n";
}

void draft_07_example()
{
    json schema = json::parse(R"(
{
    "items": [{}],
    "additionalItems": {"type": "integer"}
}
)");

    // Need to supply default version because schema does not have $schema keyword  
    auto options = jsonschema::evaluation_options{}
        .default_version(jsonschema::schema_version::draft7());
    auto compiled = jsonschema::make_json_schema(schema, options);

    json data = json::parse(R"([ null, 2, 3, "foo" ])");

    jsoncons::json_decoder<ojson> decoder;
    compiled.validate(data, decoder);
    ojson output = decoder.get_result();
    std::cout << pretty_print(output) << "\n\n";
}

void cross_schema_example()
{
    json schema = json::parse(R"(
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://example.com/schema",
    "$defs": {
        "foo": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "schema/foo",
            "definitions" : {
                "bar" : {
                    "type" : "string"
                }               
            }
        }       
    },
    "properties" : {
        "thing" : {
            "$ref" : "schema/foo#/definitions/bar"
        }
    }
}
)");
    jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema);

    json data = json::parse(R"({"thing" : 10})");

    jsoncons::json_decoder<ojson> decoder;
    compiled.validate(data, decoder);
    ojson output = decoder.get_result();
    std::cout << pretty_print(output) << "\n\n";
}

void optional_format_example()
{
    json schema = json::parse(R"(
{
    "$id": "/schema_str",
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "properties": {
        "Date": {
            "format": "date-time",
            "type": "string"
        }
    },
    "required": [
        "Date"
    ],
    "type": "object",
    "unevaluatedProperties": false
}
    )");

    auto compiled = jsoncons::jsonschema::make_json_schema(schema,
        jsonschema::evaluation_options{}.require_format_validation(true));

    json data = json::parse(R"(
{ "Date" : "2024-03-19T26:34:56Z" }
    )");

    jsoncons::json_decoder<ojson> decoder;
    compiled.validate(data, decoder);
    ojson output = decoder.get_result();
    std::cout << pretty_print(output) << "\n";
}

void walk_example() // since 0.175.0
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

    auto reporter = [](const std::string& keyword, 
        const ojson& schema, 
        const jsoncons::uri& /*schema_location*/,
        const ojson& /*instance*/, 
        const jsoncons::jsonpointer::json_pointer& instance_location) -> jsonschema::walk_result
        {
            if (keyword == "type")
            {
                assert(schema.is_object());
                auto it = schema.find("type");
                if (it != schema.object_range().end())
                {
                    std::cout << instance_location.string() << ": " << it->value() << "\n";
                }
            }
            return jsonschema::walk_result::advance;
        };
    compiled.walk(data, reporter);
}

int main()
{
    std::cout << "\nJSON Schema Examples\n\n";
    validate_three_ways();
    std::cout << "\n";

#if defined(JSONCONS_HAS_STD_VARIANT)
    validate_before_decode_example();
#endif
    defaults_example();
    optional_format_example();

    draft_201212_example();
    draft_201909_example();
    draft_07_example();
    
    cross_schema_example();
    
    walk_example();
    
    resolve_uri_example();
    
    std::cout << "\n";
}

