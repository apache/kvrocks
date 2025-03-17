### JSON Parsing Error Handling

```cpp
#include <jsoncons/json_parser.hpp>     // (until 0.171.0)
#include <jsoncons/json_options.hpp>    // (since 0.171.0)

std::function<bool(json_errc,const ser_context&)>
```

<br>

JSON parsing error handling is defined by a function object that receives arguments 
`std::error_code` and const `ser_context&`, and returns a `bool`. The JSON parser will report all errors
through the function object. If the function object returns `true`, the parser
will make an attempt to recover from recoverable errors. If the error is non-recoverable of if the function object
returns `false`, the parser will stop. 

The jsoncons library comes with three built-in function objects for JSON parsing error handling:

- `default_json_parsing`, which returns `true` if the error code indicates a comment, otherwise `false`

- `strict_json_parsing`, which always returns `false`

- `allow_trailing_commas`, which returns `true` if the error code indicates a comment or an extra comma, otherwise `false`



    

