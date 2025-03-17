### jsoncons::wojson

```cpp
#include <jsoncons/json.hpp>

typedef basic_json<wchar_t,
                   Policy = order_preserving_policy,
                   Allocator = std::allocator<wchar_t>> wojson
```
The `wojson` class is an instantiation of the [basic_json](basic_json.md) class template that uses `wchar_t` as the character type. The original insertion order of an object's name/value pairs is preserved. 

`wojson` behaves similarly to [wjson](wjson.md), with these particularities:

- `wojson`, like `wjson`, supports object member `insert_or_assign` methods that take an `object_iterator` as the first parameter. But while with `wjson` that parameter is just a hint that allows optimization, with `wojson` it is the actual location where to insert the member.

- In `wojson`, the `insert_or_assign` members that just take a name and a value always insert the member at the end.

### See also

[wjson](wjson.md) constructs a wide character json value that sorts name-value members alphabetically  

[json](json.md) constructs a utf8 character json value that sorts name-value members alphabetically  

[ojson](ojson.md) constructs a utf8 character json value that preserves the original insertion order of an object's name/value pairs  

