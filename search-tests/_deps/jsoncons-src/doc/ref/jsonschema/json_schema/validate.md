### jsoncons::jsonschema::json_schema<Json>::validate

```cpp
void validate(const Json& instance) const;  (1)

void validate(const Json& instance, Json& patch) const;  (2)

template <typename MsgReporter>
void validate(const Json& instance, const MsgReporter& reporter) const;  (3)

template <typename MsgReporter>
void validate(const Json& instance, const MsgReporter& reporter, Json& patch) const;  (4)

void validate(const Json& instance, json_visitor<Json>& visitor) const;  (5)
```

(1) Validates input JSON against a JSON Schema with a default error reporter
that throws upon the first schema violation.

(2) Validates input JSON against a JSON Schema with a default error reporter
that throws upon the first schema violation. Writes a JSONPatch document to the output
parameter.

(3) Validates input JSON against a JSON Schema with a provided error reporter
that is called for each schema violation. 

(4) Validates input JSON against a JSON Schema with a provided error reporter
that is called for each schema violation. Writes a JSONPatch document to the output
parameter.

(5) Validates input JSON against a JSON Schema and writes the validation messages
to a [json_visitor](../../corelib/basic_json_visitor.md).

#### Parameters

<table>
  <tr>
    <td>instance</td>
    <td>Input Json</td> 
  </tr>
  <tr>
    <td>reporter</td>
    <td>A function object with signature equivalent to 
    <pre>
           <a href="../walk_result.md">walk_result</a> fun(const <a href="../validation_message.md">validation_message</a>& msg);</pre>
</td> 
  </tr>
  <tr>
    <td>patch</td>
    <td>A JSONPatch document that may be applied to the input JSON
to fill in missing properties that have "default" values in the
schema.</td> 
  </tr>
  <tr>
    <td>visitor</td>
    <td>A [json_visitor](../../corelib/basic_json_visitor.md) that receives JSON events 
    corresponding to an array of validation messages.</td> 
  </tr>
</table>

#### Return value
 
None.

#### Exceptions

(1) - (2) Throws a [validation_error](../validation_error.md) for the first schema violation.

### Examples

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <iostream>

using jsoncons::ojson;
namespace jsonschema = jsoncons::jsonschema;

int main()
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
            std::cout << message.instance_location().string() << ": " << message.message() << "\n";
            return walk_result::advance;
        };
    compiled.validate(data, reporter);
    
    std::cout << "\n(3) Validate outputting to a json decoder\n";
    jsoncons::json_decoder<ojson> decoder;
    compiled.validate(data, decoder);
    ojson output = decoder.get_result();
    std::cout << pretty_print(output) << "\n";
}
```
Output:
```
(1) Validate using exceptions
/vegetables/1/veggieLike: Expected boolean, found string

(2) Validate using reporter callback
/vegetables/1/veggieLike: Expected boolean, found string
/vegetables/3: Required property 'veggieLike' not found.

(3) Validate outputting to a json decoder
[
    {
        "valid": false,
        "evaluationPath": "/properties/vegetables/items/$ref/properties/veggieLike/type",
        "schemaLocation": "https://example.com/arrays.schema.json#/$defs/veggie/properties/veggieLike",
        "instanceLocation": "/vegetables/1/veggieLike",
        "error": "Expected boolean, found string"
    },
    {
        "valid": false,
        "evaluationPath": "/properties/vegetables/items/$ref/required",
        "schemaLocation": "https://example.com/arrays.schema.json#/$defs/veggie/required",
        "instanceLocation": "/vegetables/3",
        "error": "Required property 'veggieLike' not found."
    }
]
```
