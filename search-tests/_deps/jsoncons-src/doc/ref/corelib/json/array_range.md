### jsoncons::basic_json::array_range

```cpp
array_range_type array_range();
const_array_range_type array_range() const;
```
Returns a [range](range.md) that supports a range-based for loop over the elements of a `json` array      
Throws `std::domain_error` if not an array.

### Examples

#### Range-based for loop

```cpp
json j(json_array_arg);
j.push_back("Montreal");
j.push_back("Toronto");
j.push_back("Vancouver");

for (const auto& val : j.array_range())
{
    std::cout << val.as<std::string>() << '\n';
}
```
Output:
```json
Montreal
Toronto
Vancouver 
```

#### Array iterator
```cpp
json j(json_array_arg, {"Montreal", "Toronto", "Vancouver"});

for (auto it = j.array_range().begin(); it != j.array_range().end(); ++it)
{
    std::cout << it->as<std::string>() << '\n';
}
```
Output:
```json
Montreal
Toronto
Vancouver 
```

#### Reverse array iterator
```cpp
json j(json_array_arg, {"Montreal", "Toronto", "Vancouver"});

for (auto it = j.array_range().rbegin(); it != j.array_range().rend(); ++it)
{
    std::cout << it->as<std::string>() << '\n';
}
```
Output:
```json
Vancouver
Toronto
Montreal
```


