### jsoncons::jsonpatch::from_diff

```cpp
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>

template <typename Json>
Json from_diff(const Json& source, const Json& target)
```

Create a JSON Patch from a diff of two json documents.

#### Return value

Returns a JSON Patch.  

### Examples

#### Create a JSON Patch

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>

using jsoncons::json;
namespace jsonpatch = jsoncons::jsonpatch;

int main()
{
    json source = json::parse(R"(
        {"/": 9, "foo": "bar"}
    )");

    json target = json::parse(R"(
        { "baz":"qux", "foo": [ "bar", "baz" ]}
    )");

    auto patch = jsonpatch::from_diff(source, target);

    std::error_code ec;
    jsonpatch::apply_patch(source, patch, ec);

    std::cout << "(1) " << pretty_print(patch) << '\n';
    std::cout << "(2) " << pretty_print(source) << '\n';
}
```
Output:
```
(1) 
[
    {
        "op": "remove",
        "path": "/~1"
    },
    {
        "op": "replace",
        "path": "/foo",
        "value": ["bar","baz"]
    },
    {
        "op": "add",
        "path": "/baz",
        "value": "qux"
    }
]
(2) 
{
    "baz": "qux",
    "foo": ["bar","baz"]
}
```

