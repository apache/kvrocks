### jsoncons::jsonpath::flatten

```cpp
#include <jsoncons_ext/jsonpath/filter.hpp>

template <typename Json>
Json flatten(const Json& value); (1)

template <typename Json>
Json unflatten(const Json& value); (2)
```
Flattens a json object or array to a single depth object of key-value pairs, and unflattens that object back to the original json.
The keys in the flattened object are normalized json paths.
The values are primitive (string, number, boolean, or null), empty object (`{}`) or empty array (`[]`).

#### Return value

(1) A flattened json object of JSONPath-value pairs

(2) An unflattened json object

### Examples

#### Flatten and unflatten

```cpp
#include <iostream>
#include <cassert>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

// for brevity
using jsoncons::json; 
namespace jsonpath = jsoncons::jsonpath;

int main()
{
    json input = json::parse(R"(
    {
       "application": "hiking",
       "reputons": [
           {
               "rater": "HikingAsylum",
               "assertion": "advanced",
               "rated": "Marilyn C",
               "rating": 0.90
            },
            {
               "rater": "HikingAsylum",
               "assertion": "intermediate",
               "rated": "Hongmin",
               "rating": 0.75
            }    
        ]
    }
    )");

    json flattened = jsonpath::flatten(input);

    std::cout << pretty_print(flattened) << "\n";

    json original = jsonpath::unflatten(flattened);
    assert(original == input);
}
```
Output:
```
{
    "$['application']": "hiking",
    "$['reputons'][0]['assertion']": "advanced",
    "$['reputons'][0]['rated']": "Marilyn C",
    "$['reputons'][0]['rater']": "HikingAsylum",
    "$['reputons'][0]['rating']": 0.9,
    "$['reputons'][1]['assertion']": "intermediate",
    "$['reputons'][1]['rated']": "Hongmin",
    "$['reputons'][1]['rater']": "HikingAsylum",
    "$['reputons'][1]['rating']": 0.75
}
```

### See also

[jsoncons::jsonpointer::flatten](../jsonpointer/flatten.md)
