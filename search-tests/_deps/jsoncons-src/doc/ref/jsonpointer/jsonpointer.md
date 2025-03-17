### jsonpointer extension

[JSON Pointer](https://tools.ietf.org/html/rfc6901) defines a string syntax to locate a specific value in a JSON document. 
A JSON Pointer is a string of zero or more tokens, each token prefixed by `/` characters. 
These tokens denote keys in JSON objects or indexes in JSON arrays. An empty string with zero tokens points 
to the root of a json document.

The characters `~` and `/` have special meanings in JSON Pointer, 
so if a key in a JSON object has these characters, `~` needs to be escaped as `~0`,
and `/` needs to be escaped as `~1`. 

When applied to a JSON array, the character `-` indicates one past the last element in the array.

### Classes
<table border="0">
  <tr>
    <td><a href="basic_json_pointer.md">basic_json_pointer</a></td>
    <td>Objects of type <code>basic_json_pointer</code> represent a JSON Pointer.</td> 
  </tr>
</table>

### Functions

<table border="0">
  <tr>
    <td><a href="contains.md">contains</a></td>
    <td>Returns <code>true</code> if the json document contains the given JSON Pointer</td> 
  </tr>
  <tr>
    <td><a href="get.md">get</a></td>
    <td>Get a value from a JSON document using JSON Pointer path notation.</td> 
  </tr>
  <tr>
    <td><a href="add.md">add</a></td>
    <td>Inserts a value in a JSON document using JSON Pointer path notation, or if the path specifies an object member that already has the same key, assigns the new value to that member.</td> 
  </tr>
  <tr>
    <td><code>insert</code> (until 0.162.0)</td>
    <td>Same as <a href="add_if_absent.md">add_if_absent</a></td>
  </tr>
  <tr>
    <td><a href="add_if_absent.md">add_if_absent</a> (since 0.162.0))</td>
    <td>Inserts a value in a JSON document using JSON Pointer path notation, if the path doesn't specify an object member that already has the same key.</td> 
  </tr>
  <tr>
    <td><a href="remove.md">remove</a></td>
    <td>Removes a value from a JSON document using JSON Pointer path notation.</td> 
  </tr>
  <tr>
    <td><a href="replace.md">replace</a></td>
    <td>Replaces a value in a JSON document using JSON Pointer path notation.</td> 
  </tr>
  <tr>
    <td><a href="flatten.md">flatten<br>unflatten</a></td>
    <td>Flattens a json object or array into a single depth object of JSON Pointer-value pairs.</td> 
  </tr>
</table>

### Examples

#### Select author from second book

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

// for brevity
using jsoncons::json; 
namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    auto j = json::parse(R"(
    [
      { "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      { "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      }
    ]
    )");

    // Using exceptions to report errors
    try
    {
        json result = jsonpointer::get(j, "/1/author");
        std::cout << "(1) " << result << '\n';
    }
    catch (const jsonpointer::jsonpointer_error& e)
    {
        std::cout << e.what() << '\n';
    }

    // Using error codes to report errors
    std::error_code ec;
    const json& result = jsonpointer::get(j, "/0/title", ec);

    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << "(2) " << result << '\n';
    }
}
```
Output:
```json
(1) "Evelyn Waugh"
(2) "Sayings of the Century"
```

#### Using jsonpointer::json_pointer with jsonpointer::get 

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

using jsoncons::json; 
namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    auto j = json::parse(R"(
       {
          "a/b": ["bar", "baz"],
          "m~n": ["foo", "qux"]
       }
    )");

    jsonpointer::json_pointer ptr; // (since 0.159.0)
    // jsonpointer::json_ptr ptr; // (until 0.159.0)

    ptr /= "m~n";
    ptr /= 1; // (since 0.159.0)
    //ptr /= "1"; // (until 0.159.0)

    std::cout << "(1) " << ptr << "\n\n";

    std::cout << "(2)\n";
    for (const auto& item : ptr)
    {
        std::cout << item << "\n";
    }
    std::cout << "\n";

    json item = jsonpointer::get(j, ptr);
    std::cout << "(3) " << item << "\n";
}
```
Output:
```
(1) /m~0n/1

(2)
m~n
1

(3) "qux"
```

#### Add a value to a location after creating objects when missing object keys (since 0.162.0)

```cpp
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

using jsoncons::json;
namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    std::vector<std::string> keys = {"foo","bar","baz"};

    jsonpointer::json_pointer location;
    for (const auto& key : keys)
    {
        location /= key;
    }

    json doc;
    jsonpointer::add(doc, location, "str", true);

    std::cout << pretty_print(doc) << "\n\n";
}
```
Output:
```json
{
    "foo": {
        "bar": {
            "baz": "str"
        }
    }
}
```

