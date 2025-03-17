### jsoncons::jsonschema::json_schema<Json>::is_valid

```cpp
bool is_valid(const Json& instance) const;
```

Validates input JSON against a JSON Schema and returns false upon the 
first schema violation.

#### Parameters

<table>
  <tr>
    <td>instance</td>
    <td>Input Json</td> 
  </tr>
</table>

#### Return value
 
`true` if the instance is valid, otherwise `false`.

#### Exceptions

None.

### Examples

#### Validate before decode

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <iostream>
#include <cassert>
#include <variant>

using jsoncons::json;
namespace jsonschema = jsoncons::jsonschema;

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
        std::variant<os_properties, db_properties, api_properties> run;
    };

} // namespace ns

JSONCONS_N_MEMBER_TRAITS(ns::os_properties, 1, command)
JSONCONS_N_MEMBER_TRAITS(ns::db_properties, 1, query)
JSONCONS_N_MEMBER_TRAITS(ns::api_properties, 1, target)
JSONCONS_N_MEMBER_TRAITS(ns::job_properties, 2, name, run)

int main()
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
```

Output:
```
{
    "name": "testing flow",
    "run": {
        "command": "some command"
    }
}
```
