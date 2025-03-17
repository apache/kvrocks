### jsoncons::staj_object_iterator

```cpp
#include <jsoncons/staj_iterator.hpp>

template<
    class Key,
    class T,
    class Json
    > class staj_object_iterator
```

A `staj_object_iterator` is an [InputIterator](https://en.cppreference.com/w/cpp/named_req/InputIterator) that
accesses the individual stream events from a [staj_cursor](staj_cursor.md) and, provided that when it is constructed
the current stream event has type `begin_object`, it returns the elements
of the JSON object as items of type `std::pair<string_type,T>`. If when it is constructed the current stream event
does not have type `begin_object`, it becomes equal to the default-constructed iterator.

#### Member types

Member type                         |Definition
------------------------------------|------------------------------
`char_type`|`Json::char_type`
`key_type`|`Key`
`value_type`|`std::pair<Key,T>`
`difference_type`|`std::ptrdiff_t`
`pointer`|`value_type*`
`reference`|`value_type&`
`iterator_category`|[std::input_iterator_tag](https://en.cppreference.com/w/cpp/iterator/iterator_tags)

#### Constructors

    staj_object_iterator() noexcept; (1)

    staj_object_iterator(staj_object_view<Key, T, Json>& view); (2)

    staj_object_iterator(staj_object_view<Key, T, Json>& view,
                         std::error_code& ec);  (3)

(1) Constructs the end iterator

(2) Constructs a `staj_object_iterator` that refers to the first member of the object
    following the current stream event `begin_object`. If there is no such member,
    returns the end iterator. If a parsing error is encountered, throws a 
    [ser_error](ser_error.md).

(3) Constructs a `staj_object_iterator` that refers to the first member of the object
    following the current stream event `begin_object`. If there is no such member,
    returns the end iterator. If a parsing error is encountered, returns the end iterator 
    and sets `ec`.

#### Member functions

    const key_value_type& operator*() const

    const key_value_type* operator->() const

    staj_object_iterator& operator++();
    staj_object_iterator operator++(int); 
    staj_object_iterator& increment(std::error_code& ec);
Advances the iterator to the next object member.

#### Non-member functions

    template <typename Key,typename T,typename Json>
    bool operator==(const staj_object_iterator<Key, T, Json>& a, const staj_object_iterator<Key, T, Json>& b)

    template <typename Json,typename T>
    bool operator!=(const staj_object_iterator<Key, T, Json>& a, const staj_object_iterator<Key, T, Json>& b)

### Examples

#### Iterate over a JSON object, returning key-json value pairs

```cpp
const std::string example = R"(
{
   "application": "hiking",
   "reputons": [
   {
       "rater": "HikingAsylum.array_example.com",
       "assertion": "advanced",
       "rated": "Marilyn C",
       "rating": 0.90
     }
   ]
}
)";

int main()
{
    json_string_cursor cursor(example);

    auto view = staj_object<std::string,json>(cursor);

    for (const auto& kv : view)
    {
        std::cout << kv.first << ":\n" << pretty_print(kv.second) << "\n";
    }
    std::cout << "\n\n";
}
```
Output:
```
application:
"hiking"
reputons:
[
    {
        "assertion": "advanced",
        "rated": "Marilyn C",
        "rater": "HikingAsylum.array_example.com",
        "rating": 0.90
    }
]
```

