### jsoncons::jsonpath::remove

```cpp
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

template <typename Json>
std::size_t remove(Json& root, const basic_json_location<Json::char_type>& location);            (1)

template <typename Json>
std::size_t remove(Json& root, const jsoncons::basic_string_view<Json::char_type>& path_string); (2)
```

(1) Removes a single node at the specified location. Returns the number of nodes removed (0 or 1).

(2) Finds all the nodes that match the given JSONPath expression and removes them one by one. Returns the number of nodes removed.

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
</table>

#### Return value

The number of items removed.

### Exceptions

None

### Examples

#### Remove nodes one by one at a specified location

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

using jsoncons::json;
namespace jsonpath = jsoncons::jsonpath;

int main()
{
    std::string json_string = R"(
{
    "books":
    [
        {
            "category": "fiction",
            "title" : "A Wild Sheep Chase",
            "author" : "Haruki Murakami",
            "price" : 22.72
        },
        {
            "category": "fiction",
            "title" : "The Night Watch",
            "author" : "Sergei Lukyanenko",
            "price" : 23.58
        },
        {
            "category": "fiction",
            "title" : "The Comedians",
            "author" : "Graham Greene",
            "price" : 21.99
        },
        {
            "category": "memoir",
            "title" : "The Night Watch",
            "author" : "Phillips, David Atlee"
        }
    ]
}
    )";

    json doc = json::parse(json_string);

    auto expr = jsonpath::make_expression<json>("$.books[?(@.category == 'fiction')]");
    std::vector<jsonpath::json_location> locations = expr.select_paths(doc, 
        jsonpath::result_options::sort_descending | jsonpath::result_options::sort_descending);

    for (const auto& location : locations)
    {
        std::cout << jsonpath::to_string(location) << "\n";
    }
    std::cout << "\n";

    for (const auto& location : locations)
    {
        jsonpath::remove(doc, location);
    }

    std::cout << jsoncons::pretty_print(doc) << "\n\n";
}
```
Output:
```
$['books'][2]
$['books'][1]
$['books'][0]

{
    "books": [
        {
            "author": "Phillips, David Atlee",
            "category": "memoir",
            "title": "The Night Watch"
        }
    ]
}
```

#### Remove nodes in a single step


```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

using jsoncons::json;
namespace jsonpath = jsoncons::jsonpath;

int main()
{
    std::string json_string = R"(
{
    "books":
    [
        {
            "category": "fiction",
            "title" : "A Wild Sheep Chase",
            "author" : "Haruki Murakami",
            "price" : 22.72
        },
        {
            "category": "fiction",
            "title" : "The Night Watch",
            "author" : "Sergei Lukyanenko",
            "price" : 23.58
        },
        {
            "category": "fiction",
            "title" : "The Comedians",
            "author" : "Graham Greene",
            "price" : 21.99
        },
        {
            "category": "memoir",
            "title" : "The Night Watch",
            "author" : "Phillips, David Atlee"
        }
    ]
}
    )";

    json doc = json::parse(json_string);

    std::size_t n = jsonpath::remove(doc, "$.books[?(@.category == 'fiction')]");

    std::cout << "Number of nodes removed: " << n << "\n\n";

    std::cout << jsoncons::pretty_print(doc) << "\n\n";
}
```
Output:
```
Number of nodes removed: 3

{
    "books": [
        {
            "author": "Phillips, David Atlee",
            "category": "memoir",
            "title": "The Night Watch"
        }
    ]
}
```

