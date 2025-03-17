### jsoncons::jsonpointer::replace

```cpp
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

template <typename Json,typename T>
void replace(Json& target, 
             const basic_json_pointer<Json::char_type>& location, 
             T&& value, 
             bool create_if_missing = false);                      (1)

template <typename Json,typename T>
void replace(Json& target, 
             const basic_json_pointer<Json::char_type>& location, 
             T&& value, 
             std::error_code& ec);                                 (2)

template <typename Json,typename T>
void replace(Json& target, 
             const basic_json_pointer<Json::char_type>& location, 
             T&& value, 
             bool create_if_missing, 
             std::error_code& ec);                                 (3)

template <typename Json,typename StringSource,typename T>
void replace(Json& target, 
             const StringSource& location_str, 
             T&& value, 
             bool create_if_missing = false);                      (4)

template <typename Json,typename StringSource,typename T>
void replace(Json& target, 
             const StringSource& location_str, 
             T&& value, 
             std::error_code& ec);                                 (5)

template <typename Json,typename StringSource,typename T>
void replace(Json& target, 
             const StringSource& location_str, 
             T&& value, 
             bool create_if_missing, 
             std::error_code& ec);                                 (6)
```

Replaces the value at the location specified by `location` with a new value. 

If `create_if_missing` is `false`, the target location must exist 
for the replacement to succeed. If `create_if_missing` is `true`, 
and if the target location specifies object members that do not
already exist, the missing objects and members are added.  

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
    <td>Replacement value</td> 
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

#### Exceptions

(1) Throws a [jsonpointer_error](jsonpointer_error.md) if `replace` fails.
 
(2) Sets the out-parameter `ec` to the [jsonpointer_error_category](jsonpointer_errc.md) if `replace` fails. 

### Examples

#### Replace an object value

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

using jsoncons::json;
namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    auto target = json::parse(R"(
        {
          "baz": "qux",
          "foo": "bar"
        }
    )");

    std::error_code ec;
    jsonpointer::replace(target, "/baz", json("boo"), ec);
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
{
    "baz": "boo",
    "foo": "bar"
}
```

#### Replace an array value

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
    jsonpointer::replace(target, "/foo/1", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << pretty_print(target) << '\n';
    }
}
```
Output:
```json
{
    "foo": ["bar","qux"]
}
```

#### Replace a value at a location after creating objects when missing object keys

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
    jsonpointer::replace(doc, location, "str", true);

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

