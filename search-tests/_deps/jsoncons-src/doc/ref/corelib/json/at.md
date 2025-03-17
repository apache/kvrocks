### jsoncons::basic_json::at

```cpp
basic_json& at(const string_view_type& name); (1)

const basic_json& at(const string_view_type& name) const; (2)

const basic_json& at_or_null(const string_view_type& name) const; (3)

basic_json& at(std::size_t i); (4)

const basic_json& at(std::size_t i) const; (5)
```

(1)-(2) return a reference to the value with the specifed name in a 
`basic_json` object. If not an object, an exception of type
`std::domain_error` is thrown. if the object does not have a 
member with the specified name, an exception of type
`std::out_of_range` is thrown. 

(3) returns a const reference to the value in a basic_json object
if `name` matches the name of a member, 
otherwise returns a const reference to a null `basic_json` value.
Throws `std::domain_error` if not an object or null value.

(4)-(5) return a reference to the element at index `i` in a 
basic_json array. If not an array, an exception of type
`std::domain_error` is thrown. if the index is outside the 
bounds of the array, an exception of type `std::out_of_range`
is thrown.  

### Examples

#### Return a value if available, a null if not 

```cpp
#include <jsoncons/json.hpp>

int main()
{
    json j(json_object_arg, {{"author","Evelyn Waugh"},{"title","Sword of Honour"}});

    std::cout << "(1) " << j.at_or_null("author").as<std::string>() << "\n";
    std::cout << "(2) " << j.at_or_null("title").as<std::string>() << "\n";
    std::cout << "(3) " << j.at_or_null("category").as<std::string>() << "\n";
}
```
Output:
```
(1) Evelyn Waugh
(2) Sword of Honour
(3) null
```
