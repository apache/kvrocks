### ceil

```
integer ceil(number value)
```

Returns the smallest integer value not less than the provided number.

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
    {
        "books":
        [
            {
                "title" : "A Wild Sheep Chase",
                "author" : "Haruki Murakami",
                "price" : 22.72
            },
            {
                "title" : "The Night Watch",
                "author" : "Sergei Lukyanenko",
                "price" : 23.58
            }            
        ]
    }
    )";

    json j = json::parse(data);

    json result1 = jsonpath::json_query(j, "$.books[?(ceil(@.price) == 23.0)]");
    std::cout << "(1) " << result1 << "\n\n";
    json result2 = jsonpath::json_query(j, "$.books[?(ceil(@.price*100) == 2358.0)]"); // (since 0.164.0)
    std::cout << "(2) " << result2 << "\n\n";
```
Output:
```
(1) [{"author":"Haruki Murakami","price":22.72,"title":"A Wild Sheep Chase"}]

(2) [{"author":"Sergei Lukyanenko","price":23.58,"title":"The Night Watch"}]
```

