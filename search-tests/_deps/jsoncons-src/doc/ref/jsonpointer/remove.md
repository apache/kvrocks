### jsoncons::jsonpointer::remove

Removes a `json` element.

```cpp
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

template <typename Json>
void remove(Json& target, 
            const basic_json_pointer<Json::char_type>& location); (1)

template <typename Json>
void remove(Json& target, 
            const basic_json_pointer<Json::char_type>& location, 
            std::error_code& ec);                                 (2)

template <typename Json,typename StringSource>
void remove(Json& target, 
            const StringSource& location_str);                    (3)

template <typename Json,typename StringSource>
void remove(Json& target, 
            const StringSource& location_str, 
            std::error_code& ec);                                 (4)
```

Removes the value at the location specifed by `location`.

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
    <td><code>ec</code></td>
    <td>out-parameter for reporting errors in the non-throwing overload</td> 
  </tr>
</table>

#### Return value

None

### Exceptions

(1) Throws a [jsonpointer_error](jsonpointer_error.md) if `remove` fails.
 
(2) Sets the out-parameter `ec` to the [jsonpointer_error_category](jsonpointer_errc.md) if `remove` fails. 

### Examples

#### Remove an object member

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    auto target = json::parse(R"(
        { "foo": "bar", "baz" : "qux"}
    )");

    std::error_code ec;
    jsonpointer::remove(target, "/baz", ec);
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
{"foo":"bar"}
```

#### Remove an array element

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

using jsoncons::json;
namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    auto target = json::parse(R"(
        { "foo": [ "bar", "qux", "baz" ] }
    )");

    std::error_code ec;
    jsonpointer::remove(target, "/foo/1", ec);
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
{"foo":["bar","baz"]}
```


