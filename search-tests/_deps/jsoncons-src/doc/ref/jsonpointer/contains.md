### jsoncons::jsonpointer::contains

```cpp
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

template <typename Json>
bool contains(const Json& root, const basic_json_pointer<Json::char_type>& location);

template <typename Json,typename StringSource>
bool contains(const Json& root, const StringSource& location_str);
```
#### Parameters
<table>
  <tr>
    <td>root</td>
    <td>Root JSON value</td> 
  </tr>
  <tr>
    <td>location</td>
    <td>A <a href="basic_json_pointer.md">basic_json_pointer</a></td> 
  </tr>
  <tr>
    <td>location_str</td>
    <td>A JSON Pointer provided as a string, string view, or C-string</td> 
  </tr>
</table>

#### Return value

Returns `true` if the json doc contains the given JSON Pointer, otherwise `false'

### Examples

#### Examples from [RFC6901](https://tools.ietf.org/html/rfc6901)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    // Example from RFC 6901
    auto j = jsoncons::json::parse(R"(
       {
          "foo": ["bar", "baz"],
          "": 0,
          "a/b": 1,
          "c%d": 2,
          "e^f": 3,
          "g|h": 4,
          "i\\j": 5,
          "k\"l": 6,
          " ": 7,
          "m~n": 8
       }
    )");

    std::cout << "(1) " << jsonpointer::contains(j, "/foo/0") << '\n';
    std::cout << "(2) " << jsonpointer::contains(j, "e^g") << '\n';
}
```
Output:
```json
(1) true
(2) false
```

