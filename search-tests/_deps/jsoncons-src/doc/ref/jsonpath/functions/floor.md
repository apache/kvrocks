### floor

```
integer floor(number value)
```

Returns the largest integer value not greater than the given number.

It is a type error if the provided argument is not a number.

### Examples

```cpp
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

// for brevity
using jsoncons::json; 
namespace jsonpath = jsoncons::jsonpath;

int main() 
{
        std::string data = R"(
        [
          {
            "number" : 8.95
          },
          {
            "number" : -8.95
          }
        ]        
        )";

        json j = json::parse(data);

        json result1 = jsonpath::json_query(j, "$[?(floor(@.number*100) == 895)]");
        std::cout << "(1) " << result1 << "\n\n";
        json result2 = jsonpath::json_query(j, "$[?(floor(@.number*100) == 894)]"); // (since 0.164.0)
        std::cout << "(2) " << result2 << "\n\n";
        json result3 = jsonpath::json_query(j, "$[?(floor(@.number*100) == -895)]"); // (since 0.164.0)
        std::cout << "(3) " << result3 << "\n\n";
}
```
Output:
```
(1) []

(2) [{"number":8.95}]

(3) [{"number":-8.95}]
```

(Note that the representable floating point number closest to 8.95*100 is strictly less than 895.0)
