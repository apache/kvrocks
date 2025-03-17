### avg

```
number|null avg(array[number] value)
```

Returns the average of the items in an array of numbers, or null if the array is empty

It is a type error if 

- the provided value is not an array

- the array contains items that are not numbers

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

    // All titles whose price is greater than the average price
    std::string expr = R"($.books[?(@.price > avg($.books[*].price))].title)";

    json result = jsonpath::json_query(j, expr);
    std::cout << result << "\n\n";
}
```
Output:
```
["The Night Watch"]
```

