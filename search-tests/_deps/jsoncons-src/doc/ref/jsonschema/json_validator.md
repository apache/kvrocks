### jsoncons::jsonschema::json_validator: deprecated (since 0.174.0)

```cpp
#include <jsoncons_ext/jsonschema/jsonschema.hpp>

template <typename Json>
class json_validator
```

#### Constructor

    json_validator(std::shared_ptr<json_schema<Json>> schema);

#### Member functions

    bool is_valid(const Json& instance) const;  (1)

    Json validate(const Json& instance) const;  (2)

    template <typename MsgReporter>
    Json validate(const Json& instance, const MsgReporter& reporter) const;  (3)

(1) Validates input JSON against a JSON Schema and returns false upon the 
first schema violation.

(2) Validates input JSON against a JSON Schema with a default error reporter
that throws upon the first schema violation.

(3) Validates input JSON against a JSON Schema with a provided error reporter
that is called for each schema violation.

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
           void fun(const validation_output& msg);</pre>
which accepts an argument of type <a href="validation_output.md">validation_output</a>.</td> 
  </tr>
</table>

#### Return value
 
(1) `true` if the instance is valid, otherwise `false` 

(2) - (3) A JSONPatch document that may be applied to the input JSON
to fill in missing properties that have "default" values in the
schema.

#### Exceptions

(2) Throws a [validation_error](validation_error.md) for the first schema violation.

(3) `reporter` is called for each schema violation

