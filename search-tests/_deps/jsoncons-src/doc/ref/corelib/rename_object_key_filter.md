### jsoncons::rename_object_key_filter

```cpp
#include <jsoncons/json_filter.hpp>

using rename_object_key_filter = basic_rename_object_key_filter<char>;
```
The `rename_object_key_filter` class is an instantiation of the `basic_rename_object_key_filter` class template that uses `char` as the character type. 

Renames object member names. 

![rename_object_key_filter](./diagrams/rename_object_key_filter.png)

#### Constructors

    rename_object_key_filter(const std::string& name,
                             const std::string& new_name,
                             json_visitor& visitor)

    rename_object_key_filter(const std::string& name,
                             const std::string& new_name,
                             json_visitor& visitor)

### Examples

#### Rename object member names

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons/json_filter.hpp>

using namespace jsoncons;

int main()
{
    ojson j = ojson::parse(R"({"first":1,"second":2,"fourth":3})");

    json_stream_encoder encoder(std::cout);

    rename_object_key_filter filter("fourth","third",encoder);
    j.dump(filter);
}
```
Output:
```json
{"first":1,"second":2,"third":3}
```

### See also

[basic_json_filter](basic_json_filter.md)

