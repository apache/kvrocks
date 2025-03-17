### jsoncons::jsonschema::validation_output

```cpp
#include <jsoncons_ext/jsonschema/jsonschema_error.hpp>
```

<br>

`jsoncons::jsonschema::validation_output` defines a message type for reporting failures in jsonschema operations.

#### Member functions

    const std::string& instance_location() const;    
The location of the JSON value within the instance being validated,
expressed as a URI fragment-encoded JSON Pointer.

    const std::string& message() const;
An error message that is produced by the validation.

    const std::string& schema_path() const;  
The absolute, dereferenced location of the validating keyword,
expressed as an absolute URI using the canonical URI of the 
relevant schema.

    const std::vector<validation_output>& nested_errors() const
Returns a list of nested errors

