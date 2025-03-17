### `jsoncons::basic_json::basic_json`

```cpp
basic_json(); (1)

basic_json(const basic_json& other); (2)

basic_json(const basic_json& other, const allocator_type& alloc); (3)

basic_json(basic_json&& other) noexcept; (4)

basic_json(basic_json&& other, const allocator_type& alloc) noexcept; (5)

template <typename T>
basic_json(const T& val); 
                                                                  (6)
template <typename T>
basic_json(const T& val, const Allocator& alloc = Allocator()); 

template <typename Unsigned>
basic_json(Unsigned val, semantic_tag tag); (7)

template <typename Signed>
basic_json(Signed val, semantic_tag tag); (8)

basic_json(half_arg_t, uint16_t value, semantic_tag tag = semantic_tag::none); (9)

basic_json(double val, semantic_tag tag); (10)

explicit basic_json(json_array_arg_t, 
    const Allocator& alloc = Allocator()); (11)

basic_json(json_array_arg_t, 
    semantic_tag tag, 
    const Allocator& alloc = Allocator()); (12)

basic_json(json_array_arg_t, 
    std::size_t count, const basic_json& value, 
    semantic_tag tag = semantic_tag::none, 
    const Allocator& alloc = Allocator());                         (13) (since 0.178.0)  

template <typename InputIt>
basic_json(json_array_arg_t, 
    InputIt first, InputIt last, semantic_tag tag = semantic_tag::none, 
    const Allocator& alloc = Allocator());                         (14) 

basic_json(json_array_arg_t, 
    std::initializer_list<basic_json> init, semantic_tag tag = semantic_tag::none, 
    const Allocator& alloc = Allocator());                         (15)

explicit basic_json(json_object_arg_t, 
    const Allocator& alloc = Allocator());                         (16) 

basic_json(json_object_arg_t, 
    semantic_tag tag, 
    const Allocator& alloc = Allocator());                         (17) 

template <typename InputIt>
basic_json(json_object_arg_t, 
    InputIt first, InputIt last, 
    semantic_tag tag = semantic_tag::none,
    const Allocator& alloc = Allocator());                         (18) 

basic_json(json_object_arg_t, 
    std::initializer_list<std::pair<std::basic_string<char_type>,basic_json>> init, 
    semantic_tag tag = semantic_tag::none, 
    const Allocator& alloc = Allocator());                         (19)

template <typename T>
basic_json(const T& val); 
                                                                   (20)
template <typename T>
basic_json(const T& val, const allocator_type& alloc); 

basic_json(const char_type* val); (21)

basic_json(const char_type* val, const allocator_type& alloc);     (22)

template <typename Source>
basic_json(byte_string_arg_t, 
    const Source& source, semantic_tag tag = semantic_tag::none,
    const Allocator& alloc = Allocator());                         (23)

template <typename Source>
basic_json(byte_string_arg_t, 
    const Source& source, uint64_t ext_tag,
    const Allocator& alloc = Allocator());                         (24) 

basic_json(json_const_pointer_arg, const basic_json* ptr);         (25) (since 0.156.0)

basic_json(json_pointer_arg, basic_json* ptr);                     (26) (since 1.0.0)
```

(1) Constructs an empty json object. 

(2) Constructs a copy of val

(3) Copy with allocator

(4) Acquires the contents of val, leaving val a `null` value

(5) Move with allocator

(6) Constructs a `basic_json` value for types supported in [json_type_traits](json_type_traits.md).

(7) Constructs a `basic_json` value from an unsigned integer and a [semantic_tag](../semantic_tag.md). This overload only participates in overload resolution if `Unsigned` is an unsigned integral type.

(8) Constructs a `basic_json` value from a signed integer and a [semantic_tag](../semantic_tag.md). This overload only participates in overload resolution if `Signed` is a signed integral type.

(9) Constructs a `basic_json` value for a half precision floating point number.
Uses [half_arg_t](../half_arg_t.md) as first argument to disambiguate overloads that construct half precision floating point numbers.

(10) Constructs a `basic_json` value from a double and a [semantic_tag](../semantic_tag.md).

(11)-(15) use [json_array_arg_t](../json_aray_arg_t.md) as first argument to disambiguate overloads that construct json arrays.

(11) Constructs a json array with the provided allocator.

(12) Constructs a json array with the provided [semantic_tag](../semantic_tag.md) and allocator.

(13) Constructs a json array with `count` copies of elements with value `value`.

(14) Constructs a json array with the contents of the range `[first,last]`.
`std::iterator_traits<InputIt>::value_type` must be convertible to `basic_json`. 

(15) Constructs a json array with the contents of the initializer list `init`.

(16)-(19) use [json_object_arg_t](../json_object_arg_t.md) as first argument to disambiguate overloads that construct json objects.

(16) Constructs a json object with the provided allocator.

(17) Constructs a json object with the provided [semantic_tag](../semantic_tag.md) and allocator.

(18) Constructs a json object with the contents of the range `[first,last]`.

(19) Constructs a json object with the contents of the initializer list `init`.

(20) Constructs a `basic_json` value for types supported in [json_type_traits](json_type_traits.md) with allocator.

(21) Constructs a `basic_json` value from a text string.

(22) Constructs a `basic_json` value from a text string with supplied allocator.

(23) Constructs a `basic_json` value for a byte string from a contiguous byte sequence provided by `source`
with a generic tag.
Type `Source` must be a contiguous container that has member functions `data()` and `size()`, and member type `value_type` 
with width of exactly 8 bits (since 0.152.0.)
Any of the values types `int8_t`, `uint8_t`, `char`, `unsigned char` and `std::byte` (since C++17) are allowed.

Uses [byte_string_arg_t](../byte_string_arg_t.md) as first argument to disambiguate overloads that construct byte strings.

(24) Constructs a `basic_json` value for a byte string from a contiguous byte sequence provided by `source`
with a format specific tag.
Type `Source` must be a contiguous container that has member functions `data()` and `size()`, and member type `value_type` 
with width of exactly 8 bits (since 0.152.0.)
Any of the values types `int8_t`, `uint8_t`, `char`, `unsigned char` and `std::byte` (since C++17) are allowed.

Uses [byte_string_arg_t](../byte_string_arg_t.md) as first argument to disambiguate overloads that construct byte strings.

(25) Constructs a `basic_json` value that provides a non-owning view of
another `basic_json` value. If second argument `ptr` is null,
constructs a `null` value.

(26) Constructs a `basic_json` value that provides a non-owning view of
another `basic_json` value. If second argument `ptr` is null,
constructs a `null` value. 

### Helpers

Helper                |Definition
--------------------|------------------------------
[json_object_arg][../json_object_arg.md] |    
[json_object_arg_t][../json_object_arg_t.md] | json object construction tag
[json_array_arg][../json_array_arg.md] |
[json_array_arg_t][../json_array_arg_t.md] | json array construction tag
[byte_string_arg][../byte_string_arg.md] |
[byte_string_arg_t][../byte_string_arg_t.md] | byte string construction tag
[half_arg][../half_arg.md] |
[half_arg_t][../half_arg_t.md] | half precision floating point number construction tag

### Examples

#### Basic examples

```cpp
#include <stdexcept>
#include <string>
#include <vector>
#include <map>
#include <jsoncons/json.hpp>

using namespace jsoncons;
int main()
{
    json j1; // An empty object
    std::cout << "(1) " << j1 << '\n';

    json j2(json_object_arg, {{"baz", "qux"}, {"foo", "bar"}}); // An object 
    std::cout << "(2) " << j2 << '\n';

    json j3(json_array_arg, {"bar", "baz"}); // An array 
    std::cout << "(3) " << j3 << '\n';
  
    json j4(json::null()); // A null value
    std::cout << "(4) " << j4 << '\n';
    
    json j5(true); // A boolean value
    std::cout << "(5) " << j5 << '\n';

    double x = 1.0/7.0;

    json j6(x); // A double value
    std::cout << "(6) " << j6 << '\n';

    json j8("Hello"); // A text string
    std::cout << "(8) " << j8 << '\n';

    std::vector<int> v = {10,20,30};
    json j9 = v; // From a sequence container
    std::cout << "(9) " << j9 << '\n';

    std::map<std::string, int> m{ {"one", 1}, {"two", 2}, {"three", 3} };
    json j10 = m; // From an associative container
    std::cout << "(10) " << j10 << '\n';

    std::vector<uint8_t> bytes = {'H','e','l','l','o'};
    json j11(byte_string_arg, bytes); // A byte string
    std::cout << "(11) " << j11 << '\n';

    json j12(half_arg, 0x3bff);
    std::cout << "(12) " << j12.as_double() << '\n';
}
```
Output:
```
(1) {}
(2) {"baz":"qux","foo":"bar"}
(3) ["bar","baz"]
(4) null
(5) true
(6) 0.14285714285714285
(8) "Hello"
(9) [10,20,30]
(10) {"one":1,"three":3,"two":2}
(11) "SGVsbG8"
(12) 0.999512
```

#### A json array that contains non-owning references to other json values

```cpp
#include <jsoncons/json.hpp>
#include <iostream>

int main()
{
    std::string input = R"(
    {
      "machines": [
        {"id": 1, "state": "running"},
        {"id": 2, "state": "stopped"},
        {"id": 3, "state": "running"}
      ]
    }        
    )";

    json j = json::parse(input);

    json j_v(json_array_arg);
    for (const auto& item : j.at("machines").array_range())
    {
        if (item.at("state").as<std::string>() == "running")
        {
            j_v.emplace_back(json_const_pointer_arg, &item);
        }
    }

    std::cout << "\n(1)\n" << pretty_print(j_v) << "\n\n";

    for (const auto& item : j_v.array_range())
    {
        std::cout << "json type: " << item.type() << ", storage kind: " << item.storage() << "\n";
    }

    json j2 = deep_copy(j_v);

    std::cout << "\n(2)\n" << pretty_print(j2) << "\n\n";

    for (const auto& item : j2.array_range())
    {
        std::cout << "json type: " << item.type() << ", storage kind: " << item.storage() << "\n";
    }
}

```
Output:
```json
(1)
[
    {
        "id": 1,
        "state": "running"
    },
    {
        "id": 3,
        "state": "running"
    }
]

json type: object, storage kind: json const pointer
json type: object, storage kind: json const pointer

(2)
[
    {
        "id": 1,
        "state": "running"
    },
    {
        "id": 3,
        "state": "running"
    }
]

json type: object, storage kind: object
json type: object, storage kind: object
```

