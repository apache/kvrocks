### jsoncons::jsonpath::replace

```cpp
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

template <typename Json>
std::pair<Json*,bool> replace(Json& root, 
    const basic_json_location<Json::char_type>& location, 
    const Json& new_value,
    bool create_if_missing=false)
```

Replace a JSON value in a JSON document at a specified location. 

If `create_if_missing` is `false`, the target location must exist 
for the replacement to succeed. If `create_if_missing` is `true`, 
and if the target location specifies object members that do not
already exist, the missing objects and members are added.  

#### Parameters
<table>
  <tr>
    <td>root</td>
    <td>Root JSON value</td> 
  </tr>
  <tr>
    <td>location</td>
    <td>A <a href="basic_json_location.md">basic_json_location</a></td> 
  </tr>
  <tr>
    <td>new_value</td>
    <td>Replacement value</td> 
  </tr>
  <tr>
    <td><code>create_if_missing</code></td>
    <td>Create key-object pairs when object key is missing</td> 
  </tr>
</table>

#### Return value

Returns a `std::pair<Json*,bool>`. If the replacement succeeded, the bool component is `true`, and
the `Json*` component points to the new value in the `root`. If the replacement failed, the bool component is `false`.

#### Exceptions

None

### Examples

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <cassert>
#include <iostream>

using jsoncons::json;
namespace jsonpath = jsoncons::jsonpath;

int main()
{
    std::string json_string = R"(
{"books": [ 
    { "category": "reference",
      "author": "Nigel Rees",
      "title": "Sayings of the Century",
      "price": 8.95
    },
    { "category": "fiction",
      "author": "Evelyn Waugh",
      "title": "Sword of Honour"
    },
    { "category": "fiction",
      "author": "Herman Melville",
      "title": "Moby Dick",
      "isbn": "0-553-21311-3"
    }
  ] 
}
    )";

    json doc = json::parse(json_string);

    json new_value{13.0}; 

    jsonpath::json_location loc0 = jsonpath::json_location::parse("$.books[0].price");
    auto result0 = jsonpath::replace(doc, loc0, new_value);
    assert(result0.second);
    assert(result0.first == std::addressof(doc.at("books").at(0).at("price")));
    assert(doc.at("books").at(0).at("price") == new_value);
    
    jsonpath::json_location loc1 = jsonpath::json_location::parse("$.books[1].price");
    auto result1 = jsonpath::replace(doc, loc1, new_value);
    assert(!result1.second);
    
    // create_if_missing is true
    result1 = jsonpath::replace(doc, loc1, new_value, true);
    assert(result1.second);
    assert(result1.first == std::addressof(doc.at("books").at(1).at("price")));
    assert(doc.at("books").at(1).at("price") == new_value);

    jsonpath::json_location loc2 = jsonpath::json_location::parse("$.books[2].kindle.price");
    auto result2 = jsonpath::replace(doc, loc2, new_value, true);
    assert(result2.second);
    assert(result2.first == std::addressof(doc.at("books").at(2).at("kindle").at("price")));
    assert(doc.at("books").at(2).at("kindle").at("price") == new_value);
    
    std::cout << pretty_print(doc) << "\n\n";
}
```
Output:
```json
{
    "books": [
        {
            "author": "Nigel Rees",
            "category": "reference",
            "price": 13.0,
            "title": "Sayings of the Century"
        },
        {
            "author": "Evelyn Waugh",
            "category": "fiction",
            "price": 13.0,
            "title": "Sword of Honour"
        },
        {
            "author": "Herman Melville",
            "category": "fiction",
            "isbn": "0-553-21311-3",
            "kindle": {
                "price": 13.0
            },
            "title": "Moby Dick"
        }
    ]
}
```

