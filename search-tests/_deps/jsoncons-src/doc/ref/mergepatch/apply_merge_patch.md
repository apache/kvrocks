### jsoncons::mergepatch::apply_merge_patch

```cpp
#include <jsoncons_ext/mergepatch/mergepatch.hpp>

template <typename Json>
void apply_merge_patch(Json& target, const Json& patch); 
```

Applies a merge patch to a `json` document.

#### Return value

None

#### Exceptions

None expected.

### Examples

#### Apply a JSON Merge Patch 

This example is from [RFC 7386](https://datatracker.ietf.org/doc/html/rfc7386#section-3).

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/mergepatch/mergepatch.hpp>

using jsoncons::json;
namespace mergepatch = jsoncons::mergepatch;

int main()
{
    json doc = json::parse(R"(
{
         "title": "Goodbye!",
         "author" : {
       "givenName" : "John",
       "familyName" : "Doe"
         },
         "tags":[ "example", "sample" ],
         "content": "This will be unchanged"
}
    )");

    json doc2 = doc;

    json patch = json::parse(R"(
{
         "title": "Hello!",
         "phoneNumber": "+01-123-456-7890",
         "author": {
       "familyName": null
         },
         "tags": [ "example" ]
}
    )");

    mergepatch::apply_merge_patch(doc, patch);

    std::cout << "(1)\n" << pretty_print(doc) << '\n';

    // Create a JSON Patch

    auto patch2 = mergepatch::from_diff(doc2,doc);

    std::cout << "(2)\n" << pretty_print(patch2) << '\n';

    mergepatch::apply_merge_patch(doc2,patch2);

    std::cout << "(3)\n" << pretty_print(doc2) << '\n';
}
```
Output:
```
(1)
{
    "author": {
        "familyName": null
    },
    "phoneNumber": "+01-123-456-7890",
    "tags": ["example"],
    "title": "Hello!"
}
(2)
{
    "author": {
        "givenName": "John"
    },
    "content": "This will be unchanged",
    "phoneNumber": "+01-123-456-7890",
    "tags": ["example"],
    "title": "Hello!"
}
```

