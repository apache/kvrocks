### jsoncons::jsonschema::schema_error

```cpp
#include <jsoncons_ext/jsonschema/schema_error.hpp>
```

<br>

`jsoncons::jsonschema::schema_error` defines an exception type for reporting failures in jsonschema operations.

![schema_error](./diagrams/schema_error.png)

#### Constructors

    schema_error(const std::string& message);

#### Member functions

    const char* what() const noexcept
Returns an error message


