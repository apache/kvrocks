### jsoncons::jsonpointer::flatten

```cpp
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

template <typename Json>
Json flatten(const Json& value); (1)

template <typename Json>
Json unflatten(const Json& value, unflatten_options options = unflatten_options::none); (2) (since 0.150.0)
```

(1) flattens a json object or array into a single depth object of JSON Pointer-value pairs.

- The keys in the flattened object are JSONPointer's.  

- (until 0.160.0) The values are primitive (string, number, boolean, or null). Empty objects or arrays become null.   

- (since 0.160.0) The values can be string, number, boolean, null, empty object (`{}`) or empty array (`[]`). 

(2) unflattens a json object of JSON Pointer-value pairs. There is no unique solution,
an integer appearing in a path could be an array index or it could be an object key.
The default is to attempt to preserve arrays. [unflatten_options](unflatten_options.md) 
provides additonal options.

#### Return value

(1) A flattened json object of JSON Pointer-value pairs

(2) An unflattened json object

### Examples

#### Flatten and unflatten a json object with non-numeric keys

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

// for brevity
using jsoncons::json; 
namespace jsonpointer = jsoncons::jsonpointer;

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

    json flattened = jsonpointer::flatten(input);

    std::cout << pretty_print(flattened) << "\n\n";

    json unflattened = jsonpointer::unflatten(flattened);

    assert(unflattened == input);
}
```
Output:
```
{
    "/application": "hiking",
    "/reputons/0/assertion": "advanced",
    "/reputons/0/rated": "Marilyn C",
    "/reputons/0/rater": "HikingAsylum",
    "/reputons/0/rating": 0.9,
    "/reputons/1/assertion": "intermediate",
    "/reputons/1/rated": "Hongmin",
    "/reputons/1/rater": "HikingAsylum",
    "/reputons/1/rating": 0.75
}
```

#### Flatten and unflatten a json object with numberlike keys

```cpp
int main()
{
    json input = json::parse(R"(
    {
        "discards": {
            "1000": "Record does not exist",
            "1004": "Queue limit exceeded",
            "1010": "Discarding timed-out partial msg"
        },
        "warnings": {
            "0": "Phone number missing country code",
            "1": "State code missing",
            "2": "Zip code missing"
        }
    }
    )");

    json flattened = jsonpointer::flatten(input);
    std::cout << "(1)\n" << pretty_print(flattened) << "\n";

    json unflattened1 = jsonpointer::unflatten(flattened);
    std::cout << "(2)\n" << pretty_print(unflattened1) << "\n";

    json unflattened2 = jsonpointer::unflatten(flattened,
        jsonpointer::unflatten_options::assume_object);
    std::cout << "(3)\n" << pretty_print(unflattened2) << "\n";
}
```
Output:
```
(1)
{
    "/discards/1000": "Record does not exist",
    "/discards/1004": "Queue limit exceeded",
    "/discards/1010": "Discarding timed-out partial msg",
    "/warnings/0": "Phone number missing country code",
    "/warnings/1": "State code missing",
    "/warnings/2": "Zip code missing"
}
(2)
{
    "discards": {
        "1000": "Record does not exist",
        "1004": "Queue limit exceeded",
        "1010": "Discarding timed-out partial msg"
    },
    "warnings": ["Phone number missing country code", "State code missing", "Zip code missing"]
}
(3)
{
    "discards": {
        "1000": "Record does not exist",
        "1004": "Queue limit exceeded",
        "1010": "Discarding timed-out partial msg"
    },
    "warnings": {
        "0": "Phone number missing country code",
        "1": "State code missing",
        "2": "Zip code missing"
    }
}
```
### See also

[jsoncons::jsonpath::flatten](../jsonpath/flatten.md)
