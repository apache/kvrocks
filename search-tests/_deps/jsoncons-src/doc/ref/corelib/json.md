### jsoncons::json

```cpp
#include <jsoncons/json.hpp>

typedef basic_json<char,
                   Policy = sorted_policy,
                   Allocator = std::allocator<char>> json
```
The class `json` is an instantiation of the [basic_json](basic_json.md) class template that uses `char` as the character type. 
The order of an object's name/value pairs is not preserved, they are sorted alphabetically by name. 
If you want to preserve the original insertion order, use [ojson](ojson.md) instead.

### See also

[ojson](ojson.md) constructs a utf8 character json value that preserves the original insertion order of an object's name/value pairs  

[wjson](wjson.md) constructs a wide character json value that sorts name-value members alphabetically  

[wojson](wojson.md) constructs a wide character json value that preserves the original insertion order of an object's name/value pairs  


### Examples
  
#### Accessors and defaults
```cpp
json j;

j["field1"] = 1;
j["field3"] = "Toronto";

double x1 = obj.contains("field1") ? j["field1"].as<double>() : 10.0;
double x2 = obj.contains("field2") ? j["field2"].as<double>() : 20.0;

std::string x3 = obj.get_value_or<std::string>("field3","Montreal");
std::string x4 = obj.get_value_or<std::string>("field4","San Francisco");

std::cout << "x1=" << x1 << '\n';
std::cout << "x2=" << x2 << '\n';
std::cout << "x3=" << x3 << '\n';
std::cout << "x4=" << x4 << '\n';
```
Output:
```cpp
x1=1
x2=20
x3=Toronto
x4=San Francisco
```
#### Nulls
```cpp
json j;
j["field1"] = json::null();
std::cout << j << '\n';
```
Output: 
```json
{"field1":null}
```
#### Constructing json structures
```cpp
json doc;

doc["persons"] = json(json_array_arg);

json person;
person["first_name"] = "John";
person["last_name"] = "Smith";
person["birth_date"] = "1972-01-30";

json address;
address["city"] = "Toronto";
address["country"] = "Canada";

person["address"] = std::move(address);

doc["persons"].push_back(std::move(person));

std::cout << pretty_print(doc) << '\n';
```
Output:
```cpp
{
    "persons":
    [
        {
            "address":
            {
                "city":"Toronto",
                "country":"Canada"
            },
            "birth_date":"1972-01-30",
            "first_name":"John",
            "last_name":"Smith"
        }
    ]
}
```

