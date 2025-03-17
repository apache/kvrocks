### jsoncons::basic_json::object_range

```c++
object_range_type object_range();
const_object_range_type object_range() const;
```
Returns a [range](range.md) that supports a range-based for loop over the key-value pairs of a `basic_json` object      
Throws `std::domain_error` if not an object.

### Examples

#### Range-based for loop over key-value pairs of an object

```c++
#include <jsoncons/json.hpp>

int main()
{
    json j = json::parse(R"(
{
    "category" : "Fiction",
    "title" : "Pulp",
    "author" : "Charles Bukowski",
    "date" : "2004-07-08",
    "price" : 22.48,
    "isbn" : "1852272007"  
}
)");

    for (const auto& member : j.object_range())
    {
        std::cout << member.key() << " => " << member.value().as<std::string>() << '\n';
    }
}
```
Output:
```json
author => Charles Bukowski
category => Fiction
date => 2004-07-08
isbn => 1852272007
price => 22.48
title => Pulp
```

#### Reverse object iterator
```c++
ojson j;
j["city"] = "Toronto";
j["province"] = "Ontario";
j["country"] = "Canada";

for (auto it = j.object_range().crbegin(); it != j.object_range().crend(); ++it)
{
    std::cout << it->key() << " => " << it->value().as<std::string>() << '\n';
}
```
Output:
```c++
country => Canada
province => Ontario
city => Toronto
```

