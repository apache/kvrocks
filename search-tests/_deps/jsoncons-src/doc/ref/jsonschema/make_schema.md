### jsoncons::jsonschema::make_schema: 
(deprecated since 0.174.0) 
(removed in 1.0.0)

```cpp
#include <jsoncons_ext/jsonschema/jsonschema.hpp>

template <typename Json>
std::shared_ptr<json_schema<Json>> make_schema(const Json& schema);  (1)

template <typename Json>
std::shared_ptr<json_schema<Json>> make_schema(const Json& schema,
    const std::string& retrieval_uri);                               (2) (since 0.173.0)

template <typename Json,class SchemaResolver>
std::shared_ptr<json_schema<Json>> make_schema(const Json& schema,
    const std::string& retrieval_uri, const SchemaResolver& resolver);    (3) (since 0.173.0)

template <typename Json,class SchemaResolver>
std::shared_ptr<json_schema<Json>> make_schema(const Json& schema, 
    const SchemaResolver& resolver);                                      (4)
```

Returns a `shared_ptr` to a `json_schema<Json>`.

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
    Json fun(const <a href="../corelib/utility/uri.md">jsoncons::uri</a>& uri);</pre></td>   
  </tr>
</table>

#### Return value

Returns a `shared_ptr` to a `json_schema<Json>`.

#### Exceptions

(1)-(2) Throws a [schema_error](schema_error.md) if JSON Schema compilation fails.


