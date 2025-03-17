### jsoncons::jsonschema::make_json_schema

```cpp
#include <jsoncons_ext/jsonschema/jsonschema.hpp>

template <typename Json>
json_schema<Json> make_json_schema(const Json& sch,                 (until 0.175.0)
    evaluation_options options = evaluation_options{});        
                                                               (1)
template <typename Json>
json_schema<Json> make_json_schema(Json sch,                        (since 0.175.0)
    evaluation_options options = evaluation_options{});        

template <typename Json,class SchemaResolver>
json_schema<Json> make_json_schema(const Json& sch,                 (until 0.175.0)
    const SchemaResolver& resolver,                               
    evaluation_options options = evaluation_options{});        (2)

template <typename Json,class SchemaResolver>
json_schema<Json> make_json_schema(Json sch,                        (since 0.175.0)
    const SchemaResolver& resolver,                               
    evaluation_options options = evaluation_options{});        

template <typename Json>
json_schema<Json> make_json_schema(const Json& sch,                 (until 0.175.0)
    const std::string& retrieval_uri,                          
    evaluation_options options = evaluation_options{});         
                                                               (3)
template <typename Json>
json_schema<Json> make_json_schema(Json sch,                        (since 0.175.0) 
    const std::string& retrieval_uri,                          
    evaluation_options options = evaluation_options{});         

template <typename Json,class SchemaResolver>
json_schema<Json> make_json_schema(const Json& sch,                 (until 0.175.0)
    const std::string& retrieval_uri,                          
    const SchemaResolver& resolver, 
    evaluation_options options = evaluation_options{});         
                                                               (4)
template <typename Json,class SchemaResolver>
json_schema<Json> make_json_schema(Json sch,                        (since 0.175.0) 
    const std::string& retrieval_uri,                          
    const SchemaResolver& resolver, 
    evaluation_options options = evaluation_options{});         
```

Returns a [json_schema<Json>](json_schema.md) that represents a compiled JSON Schema document.

#### Parameters

<table>
  <tr>
    <td>schema</td>
    <td>JSON Schema</td> 
  </tr>
  <tr>
    <td>resolver</td>
    <td>A function object with the signature of <code>resolver</code> being equivalent to 
    <pre>
    Json fun(const <a href="../corelib/utility/uri.md">jsoncons::uri</a>& uri);</pre>
    If unable to resolve the resource, it should return <code>Json::null()</code>.
    </td>   
  </tr>
  <tr>
    <td>retrieval_uri</td>
    <td>Optional retrieval URI</td> 
  </tr>
  <tr>
    <td><a href="evaluation_options.md">options</a></td>
    <td>Evaluation options</td> 
  </tr>
</table>

#### Return value

A [json_schema<Json>](json_schema.md) that represents a compiled JSON Schema document.

#### Exceptions

(1)-(4) Throws a [schema_error](schema_error.md) if JSON Schema compilation fails.

### Examples

#### Draft 2020-12 example (from the JSON Schema Test Suite)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <iostream>

using jsoncons::json;
namespace jsonschema = jsoncons::jsonschema;

int main()
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
```

Output:
```json
[
    {
        "valid": false,
        "evaluationPath": "/$ref/items/$dynamicRef/type",
        "schemaLocation": "https://test.json-schema.org/typical-dynamic-resolution/root#items",
        "instanceLocation": "/1",
        "error": "Expected string, found integer"
    }
]
```

#### Draft 2019-09 example (from the JSON Schema Test Suite)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <iostream>

using jsoncons::json;
namespace jsonschema = jsoncons::jsonschema;

int main()
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
```

Output:
```json
[
    {
        "valid": false,
        "evaluationPath": "/unevaluatedProperties/baz",
        "schemaLocation": "#",
        "instanceLocation": "/baz",
        "error": "Unevaluated property 'baz' but the schema does not allow unevaluated properties."
    }
]
```

#### Draft 07 example (from the JSON Schema Test Suite)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <iostream>

using jsoncons::json;
namespace jsonschema = jsoncons::jsonschema;

int main()
{
    json schema = json::parse(R"(
{
    "items": [{}],
    "additionalItems": {"type": "integer"}
}
)");

    // Need to supply default version because schema does not have $schema keyword  
    jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(schema,
        jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft7()));

    json data = json::parse(R"([ null, 2, 3, "foo" ])");

    jsoncons::json_decoder<ojson> decoder;
    compiled.validate(data, decoder);
    ojson output = decoder.get_result();
    std::cout << pretty_print(output) << "\n\n";
}
```
Output:
```json
[
    {
        "valid": false,
        "evaluationPath": "/items/type",
        "schemaLocation": "#/additionalItems",
        "instanceLocation": "/3",
        "error": "Expected integer, found string"
    }
]
```

#### Cross draft example

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <iostream>

using jsoncons::json;
namespace jsonschema = jsoncons::jsonschema;

int main()
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
```
Output:
```json
[
    {
        "valid": false,
        "evaluationPath": "/properties/thing/$ref/type",
        "schemaLocation": "https://example.com/schema/foo#/definitions/bar",
        "instanceLocation": "/thing",
        "error": "Expected string, found integer"
    }
]
```

