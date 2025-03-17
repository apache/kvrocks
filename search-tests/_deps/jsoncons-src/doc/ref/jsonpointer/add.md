### jsoncons::jsonpointer::add

Adds a value to an object or inserts it into an array at the target location.
If a value already exists at the target location, that value is replaced.

```cpp
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

template <typename Json,typename T>
void add(Json& target, 
         const basic_json_pointer<Json::char_type>& location, 
         T&& value, 
         bool create_if_missing = false);                      (1) (since 0.167.0)

template <typename Json,typename T>
void add(Json& target, 
         const basic_json_pointer<Json::char_type>& location, 
         T&& value, 
         std::error_code& ec);                                 (2) (since 0.167.0)

template <typename Json,typename T>
void add(Json& target, 
         const basic_json_pointer<Json::char_type>& location, 
         T&& value, 
         bool create_if_missing, 
         std::error_code& ec);                                 (3) (since 0.167.0)

template <typename Json,typename StringSource,typename T>
void add(Json& target, 
         const StringSource& location_str, 
         T&& value, 
         bool create_if_missing = false);                      (4)

template <typename Json,typename StringSource,typename T>
void add(Json& target, 
         const StringSource& location_str, 
         T&& value, 
         std::error_code& ec);                                 (5)

template <typename Json,typename StringSource,typename T>
void add(Json& target, 
         const StringSource& location_str, 
         T&& value, 
         bool create_if_missing, 
         std::error_code& ec);                                 (6) (since 0.162.0)
```

Inserts a value into the target at the specified location, or if the location specifies an object member that already has the same key, assigns the new value to that member

- If `location` specifies an array index, a new value is inserted into the array at the specified index.

- If `location` specifies an object member that does not already exist, a new member is added to the object.

- If `location` specifies an object member that does exist, that member's value is replaced.

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
    <td>New or replacement value</td> 
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

(1) Throws a [jsonpointer_error](jsonpointer_error.md) if `add` fails.
 
(2)-(3) Sets the out-parameter `ec` to the [jsonpointer_error_category](jsonpointer_errc.md) if `add` fails. 
 
### Examples

#### Insert or assign an object member at a location_str that already exists

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
    jsonpointer::add(target, "/baz", json("qux"), ec);
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

#### Add a value to a location_str after creating objects when missing object keys

```cpp
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

using jsoncons::json;
namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    std::vector<std::string> keys = {"foo","bar","baz"};

    jsonpointer::json_pointer location_str;
    for (const auto& key : keys)
    {
        location_str /= key;
    }

    json doc;
    jsonpointer::add(doc, location_str, "str", true);

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


