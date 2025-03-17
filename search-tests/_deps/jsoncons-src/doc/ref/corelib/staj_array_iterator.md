### jsoncons::staj_array_iterator

```cpp
#include <jsoncons/staj_iterator.hpp>

template<
    class T,
    class Json
    >
class staj_array_iterator
```

A `staj_array_iterator` is an [InputIterator](https://en.cppreference.com/w/cpp/named_req/InputIterator) that
accesses the individual stream events from a [staj_cursor](staj_cursor.md) and, provided that when it is constructed
the current stream event has type `staj_event_type::begin_array`, it retrieves the elements of the JSON array
as items of type `T`. If when it is constructed the current stream event does not have type `staj_event_type::begin_array`,
it becomes equal to the default-constructed iterator.

#### Member types

Member type                         |Definition
------------------------------------|------------------------------
`char_type`|Json::char_type
`value_type`|`T`
`difference_type`|`std::ptrdiff_t`
`pointer`|`value_type*`
`reference`|`value_type&`
`iterator_category`|[std::input_iterator_tag](https://en.cppreference.com/w/cpp/iterator/iterator_tags)

#### Constructors

    staj_array_iterator() noexcept; (1)

    staj_array_iterator(staj_array_view<T, Json>& view); (2)

    staj_array_iterator(staj_array_view<T, Json>& view,
                        std::error_code& ec); (3)

(1) Constructs the end iterator

(2) Constructs a `staj_array_iterator` that refers to the first element of the array
    following the current stream event `begin_array`. If there is no such element,
    returns the end iterator. If a parsing error is encountered, throws a 
    [ser_error](ser_error.md).

(3) Constructs a `staj_array_iterator` that refers to the first member of the array
    following the current stream event `begin_array`. If there is no such element,
    returns the end iterator. If a parsing error is encountered, returns the end iterator 
    and sets `ec`.

#### Member functions

    const T& operator*() const

    const T* operator->() const

    staj_array_iterator& operator++()
    staj_array_iterator& increment(std::error_code& ec)
    staj_array_iterator operator++(int) 
Advances the iterator to the next array element.

#### Non-member functions

    template <typename class T,typename Json>
    bool operator==(const staj_array_iterator<T, Json>& a, const staj_array_iterator<T, Json>& b);

    template <typename Json,typename T>
    bool operator!=(const staj_array_iterator<T, Json>& a, const staj_array_iterator<T, Json>& b);

### Examples

#### Iterate over a JSON array, returning json values  

```cpp
const std::string example = R"(
[ 
  { 
      "employeeNo" : "101",
      "name" : "Tommy Cochrane",
      "title" : "Supervisor"
  },
  { 
      "employeeNo" : "102",
      "name" : "Bill Skeleton",
      "title" : "Line manager"
  }
]
)";

int main()
{
    std::istringstream is(example);

    json_stream_cursor cursor(is);

    auto view = staj_array<json>(cursor);

    for (const auto& j : view)
    {
        std::cout << pretty_print(j) << "\n";
    }
    std::cout << "\n\n";
}
```
Output:
```
{
    "employeeNo": "101",
    "name": "Tommy Cochrane",
    "title": "Supervisor"
}
{
    "employeeNo": "102",
    "name": "Bill Skeleton",
    "title": "Line manager"
}
```

#### Iterate over the JSON array, returning employee values 

```cpp
namespace ns {

    struct employee
    {
        std::string employeeNo;
        std::string name;
        std::string title;
    };

} // namespace ns

JSONCONS_ALL_MEMBER_TRAITS(ns::employee, employeeNo, name, title)
      
int main()
{
    std::istringstream is(example);

    json_stream_cursor cursor(is);

    auto view = staj_array<ns::employee>(cursor);

    for (const auto& val : view)
    {
        std::cout << val.employeeNo << ", " << val.name << ", " << val.title << "\n";
    }
    std::cout << "\n\n";
}
```
Output:
```
101, Tommy Cochrane, Supervisor
102, Bill Skeleton, Line manager
```

