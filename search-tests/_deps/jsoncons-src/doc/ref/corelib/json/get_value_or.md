### jsoncons::basic_json::get_value_or

```cpp
template <typename T,class U>
T get_value_or(const string_view_type& name, U&& default_value) const; 
```

Returns a value in a basic_json object
if `name` matches the name of a member, 
otherwise returns a default value.
Throws `std::domain_error` if not an object or null value.

### Type requirements

- `T` must meet the requirements of [CopyConstructible](https://en.cppreference.com/w/cpp/named_req/CopyConstructible) 
- `U` must be convertible to `T`

### Examples

#### Return a value if available, a default value if not 

```cpp
#include <jsoncons/json.hpp>

int main()
{
    json j(json_object_arg, {{"author","Evelyn Waugh"},{"title","Sword of Honour"}});

    std::cout << j.get_value_or<std::string>("author","unknown") << "\n";
    std::cout << j.get_value_or<std::string>("category","fiction") << "\n";
}
```
Output:
```
Evelyn Waugh
fiction
```
