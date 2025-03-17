### length

```
integer|null length(array|object|string value)
```

Returns the length of an array, object or string.

If array, returns the number of items in the array
If object, returns the number of key-value pairs in the object
If string, returns the number of codepoints in the string
Otherwise, returns null.

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
        },
        {
            "title" : "The Comedians",
            "author" : "Graham Greene",
            "price" : 21.99
        },
        {
            "title" : "The Night Watch",
            "author" : "Phillips, David Atlee"
        }
    ]
}
    )";

    json j = json::parse(data);

    json result1 = jsonpath::json_query(j, "length($.books[*])");
    std::cout << "(1) " << result1 << "\n\n";

    json result2 = jsonpath::json_query(j, "length($.books[*].price)");
    std::cout << "(2) "  << result2 << "\n\n";
}
```
Output:
```
(1) [4]

(2) [3]
```


