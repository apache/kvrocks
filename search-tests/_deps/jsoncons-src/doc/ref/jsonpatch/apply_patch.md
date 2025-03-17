### jsoncons::jsonpatch::apply_patch

```cpp
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>

template <typename Json>
void apply_patch(Json& target, const Json& patch); (1)

template <typename Json>
void apply_patch(Json& target, const Json& patch, std::error_code& ec); (2)
```

Applies a patch to a `json` document.

#### Return value

None

#### Exceptions

(1) Throws a [jsonpatch_error](jsonpatch_error.md) if `apply_patch` fails.
  
(2) Sets the out-parameter `ec` to the [jsonpatch_error_category](jsonpatch_errc.md) if `apply_patch` fails. 

### Examples

#### Apply a JSON Patch with two add operations

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>

using jsoncons::json;
namespace jsonpatch = jsoncons::jsonpatch;

int main()
{
    json doc = json::parse(R"(
        { "foo": "bar"}
    )");

    json patch = json::parse(R"(
        [
            { "op": "add", "path": "/baz", "value": "qux" },
            { "op": "add", "path": "/foo", "value": [ "bar", "baz" ] }
        ]
    )");

    std::error_code ec;
    jsonpatch::apply_patch(target,patch,ec);

    std::cout << pretty_print(target) << '\n';
}
```
Output:
```
{
    "baz": "qux",
    "foo": ["bar","baz"]
}
```

#### Apply a JSON Patch with three add operations, the last one fails

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>

using jsoncons::json;
namespace jsonpatch = jsoncons::jsonpatch;

int main()
{
    json target = json::parse(R"(
        { "foo": "bar"}
    )");

    json patch = json::parse(R"(
        [
            { "op": "add", "path": "/baz", "value": "qux" },
            { "op": "add", "path": "/foo", "value": [ "bar", "baz" ] },
            { "op": "add", "path": "/baz/bat", "value": "qux" } // nonexistent target
        ]
    )");

    std::error_code ec;
    jsonpatch::apply_patch(target, patch, ec);

    std::cout << "(1) " << ec.message() << '\n';
    std::cout << "(2) " << target << '\n';
}
```
Output:
```
(1) JSON Patch add operation failed
(2) {"foo":"bar"}
```
Note that all JSON Patch operations have been rolled back, and target is in its original state.

