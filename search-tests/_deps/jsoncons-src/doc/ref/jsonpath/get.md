### jsoncons::jsonpath::get

```cpp
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

template <typename Json>
Json* get(Json& root, const basic_json_location<Json::char_type>& location);                   (until 0.175.0)

template <typename Json>
std::pair<Json*,bool> get(Json& root, const basic_json_location<Json::char_type>& location);   (since 0.175.0)
```

Gets a pointer to a JSON value in a JSON document at a specified location.

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

Until 0.175.0, returns a pointer to the selected item, or null if not found. 

Since 0.175.0, returns a `std::pair<Json*,bool>`. If the get operation succeeded, the bool component is `true`, and
the `Json*` component points to the value in the `root`. If the get operation failed, the bool component is `false`.


### Exceptions

None.


