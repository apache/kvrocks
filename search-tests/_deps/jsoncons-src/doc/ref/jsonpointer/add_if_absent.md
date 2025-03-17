### jsoncons::jsonpointer::add_if_absent

Adds a value to an object or inserts it into an array at the target location,
if a value does not already exist at that location.

```cpp
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

template <typename Json,typename T>
void add_if_absent(Json& target, 
                    const basic_json_pointer<Json::char_type>& location, 
                    T&& value, 
                    bool create_if_missing = false);                     (1) 

template <typename Json,typename T>
void add_if_absent(Json& target, 
                   const basic_json_pointer<Json::char_type>& location, 
                   T&& value, 
                   std::error_code& ec);                                 (2) 

template <typename Json,typename T>
void add_if_absent(Json& target, 
                   const basic_json_pointer<Json::char_type>& location, 
                   T&& value, 
                   bool create_if_missing, 
                   std::error_code& ec);                                 (3) 

template <typename Json,typename StringSource,typename T>
void add_if_absent(Json& target, 
                    const StringSource& location_str, 
                    T&& value, 
                    bool create_if_missing = false);                     (4) 

template <typename Json,typename StringSource,typename T>
void add_if_absent(Json& target, 
                   const StringSource& location_str, 
                   T&& value, 
                   std::error_code& ec);                                 (5) 

template <typename Json,typename StringSource,typename T>
void add_if_absent(Json& target, 
                   const StringSource& location_str, 
                   T&& value, 
                   bool create_if_missing, 
                   std::error_code& ec);                                 (6) 
```

Inserts a value into the target at the specified location, if the location doesn't specify an object member that already has the same key.

- If `location` specifies an array index, a new value is inserted into the array at the specified index.

- If `location` specifies an object member that does not already exist, a new member is added to the object.

#### Parameters
<table>
  <tr>
    <td>target</td>
    <td>JSON value</td> 
  </tr>
  <tr>
    <td>location</td>
    <td>A <a href="basic_json_pointer.md">basic_json_pointer</a></td> 
  </tr>
  <tr>
    <td>location_str</td>
    <td>A JSON Pointer provided as a string, string view, or C-string</td> 
  </tr>
  <tr>
    <td>value</td>
    <td>New value</td> 
  </tr>
  <tr>
    <td><code>create_if_missing</code> (since 0.162.0)</td>
    <td>Create key-object pairs when object key is missing</td> 
  </tr>
  <tr>
    <td><code>ec</code></td>
    <td>out-parameter for reporting errors in the non-throwing overload</td> 
  </tr>
</table>

#### Return value

None

### Exceptions

(1) Throws a [jsonpointer_error](jsonpointer_error.md) if `add_if_absent` fails.
 
(2) Sets the out-parameter `ec` to the [jsonpointer_error_category](jsonpointer_errc.md) if `add_if_absent` fails. 

### Examples

#### Add a member to a target location that does not already exist

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

// for brevity
using jsoncons::json;
namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    auto target = json::parse(R"(
        { "foo": "bar"}
    )");

    std::error_code ec;
    jsonpointer::add_if_absent(target, "/baz", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << target << '\n';
    }
}
```
Output:
```json
{"baz":"qux","foo":"bar"}
```

#### Add an element to the second position in an array

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

using jsoncons::json;
namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    auto target = json::parse(R"(
        { "foo": [ "bar", "baz" ] }
    )");

    std::error_code ec;
    jsonpointer::add_if_absent(target, "/foo/1", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << target << '\n';
    }
}
```
Output:
```json
{"foo":["bar","qux","baz"]}
```

#### Add a value at the end of an array

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

using jsoncons::json;
namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    auto target = json::parse(R"(
        { "foo": [ "bar", "baz" ] }
    )");

    std::error_code ec;
    jsonpointer::add_if_absent(target, "/foo/-", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << target << '\n';
    }
}
```
Output:
```json
{"foo":["bar","baz","qux"]}
```

#### Try to add an object member at a location that already exists

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

using jsoncons::json;
namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    auto target = json::parse(R"(
        { "foo": "bar", "baz" : "abc"}
    )");

    std::error_code ec;
    jsonpointer::add_if_absent(target, "/baz", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << target << '\n';
    }
}
```
Output:
```
Key already exists
```

#### Try to add a value to a location in an array that exceeds the size of the array

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

using jsoncons::json;
namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    auto target = json::parse(R"(
    { "foo": [ "bar", "baz" ] }
    )");

    std::error_code ec;
    jsonpointer::add(target, "/foo/3", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << target << '\n';
    }
}
```
Output:
```
Index exceeds array size
```

#### Add a value to a location after creating objects when missing object keys

```cpp
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

using jsoncons::json;
namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    std::vector<std::string> keys = { "foo","bar","baz" };

    jsonpointer::json_pointer location;
    for (const auto& key : keys)
    {
        location /= key;
    }

    json doc;
    jsonpointer::add_if_absent(doc, location, "str", true);

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

