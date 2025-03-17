### jsoncons::jsonpath::jsonpath_expression::select

```cpp
std::vector<basic_json_location<char_type>> select_paths(const_reference root,
    result_options options = result_options::nodups | result_options::sort);                                                 (1) (since 0.172.0)
```

(1) Evaluates the root value against the compiled JSONPath expression and returns the
locations of the selected values in the root value.

#### Parameters

<table>
  <tr>
    <td>root</td>
    <td>Root JSON value</td> 
  </tr>
  <tr>
    <td><code>options</code> (since 0.161.0)</td>
    <td>Result options, a bitmask of type <a href="../result_options.md">result_options</></td> 
  </tr>
</table>

### Examples

#### Return locations of selected values 

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

using json = jsoncons::json;
namespace jsonpath = jsoncons::jsonpath;

int main()
{
    auto expr = jsoncons::jsonpath::make_expression<json>("$.books[*]");

    std::ifstream is(/*path_to_books_file*/);
    json doc = json::parse(is);

    std::vector<jsonpath::json_location> paths = expr.select_paths(doc);
    for (const auto& path : paths)
    {
        std::cout << jsonpath::to_string(path) << "\n";
    }
}
```
Output:
```
[
  $['books'][0]
  $['books'][1]
  $['books'][2]
  $['books'][3]
]
```

