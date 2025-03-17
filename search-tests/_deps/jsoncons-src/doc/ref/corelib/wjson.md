### jsoncons::wjson

```cpp
#include <jsoncons/json.hpp>

typedef basic_json<wchar_t,
                   Policy = sorted_policy,
                   Allocator = std::allocator<wchar_t>> wjson
```

The `wjson` class is an instantiation of the [basic_json](basic_json.md) class template that uses `wchar_t` as the character type. The order of an object's name/value pairs is not preserved, they are sorted alphabetically by name. If you want to preserve the original insertion order, use [wojson](wojson.md) instead.

### See also

[wojson](wojson.md) constructs a wide character json value that preserves the original insertion order of an object's name/value pairs  

[json](json.md) constructs a utf8 character json value that sorts name-value members alphabetically  

[ojson](ojson.md) constructs a utf8 character json value that preserves the original insertion order of an object's name/value pairs  

