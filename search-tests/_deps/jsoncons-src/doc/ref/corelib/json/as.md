### jsoncons::basic_json::as

```cpp
template <typename T>
T as() const; (1)

template <typename T>
T as(byte_string_arg_t, semantic_tag hint) const; (2)
```

(1) Generic get `as` type `T`. Attempts to convert the json value to the template value type using [json_type_traits](../json_type_traits.md).

    std::string as<std::string>() const noexcept
If value is string, returns value, otherwise returns result of [dump](dump.md).

    as<X<T>>()
If the type `X` is not `std::basic_string` but otherwise satisfies [SequenceContainer](http://en.cppreference.com/w/cpp/concept/SequenceContainer), `as<X<T>>()` returns the `json` value as an `X<T>` if the `json` value is an array and each element is convertible to type `T`, otherwise throws.

    as<X<std::string,T>>()
If the type 'X' satisfies [AssociativeContainer](http://en.cppreference.com/w/cpp/concept/AssociativeContainer) or [UnorderedAssociativeContainer](http://en.cppreference.com/w/cpp/concept/UnorderedAssociativeContainer), `as<X<std::string,T>>()` returns the `json` value as an `X<std::string,T>` if the `json` value is an object and if each member value is convertible to type `T`, otherwise throws.

(2) Get as byte string with hint. This overload only participates in overload resolution if `uint8_t` is convertible to `T::value_type`.
If the json type is a string, converts string according to its `semantic_tag`, or if there is none, uses `hint`.

### Examples

#### Accessing integral, floating point, and boolean values

```cpp
json j = json::parse(R"(
{
    "k1" : 2147483647,
    "k2" : 2147483648,
    "k3" : -10,
    "k4" : 10.5,
    "k5" : true,
    "k6" : "10.5"
}
)");

std::cout << "(1) " << j["k1"].as<int32_t>() << '\n';
std::cout << "(2) " << j["k2"].as<int32_t>() << '\n';
std::cout << "(3) " << j["k2"].as<long long>() << '\n';
std::cout << "(4) " << j["k3"].as<signed char>() << '\n';
std::cout << "(5) " << j["k3"].as<uint32_t>() << '\n';
std::cout << "(6) " << j["k4"].as<int32_t>() << '\n';
std::cout << "(7) " << j["k4"].as<double>() << '\n';
std::cout << std::boolalpha << "(8) " << j["k5"].as<int>() << '\n';
std::cout << "(9) " << j["k6"].as<double>() << '\n';

```
Output:
```

(1) 2147483647
(2) -2147483648
(3) 2147483648
(4) ÷
(5) 4294967286
(6) 10
(7) 10.5
(8) true
(9) 10.5
```

#### Accessing a `json` array value as a `std::vector`
```cpp
std::string s = "{\"my-array\" : [1,2,3,4]}";
json val = json::parse(s);
std::vector<int> v = val["my-array"].as<std::vector<int>>();
for (std::size_t i = 0; i < v.size(); ++i)
{
    if (i > 0)
    {
        std::cout << ",";
    }
    std::cout << v[i]; 
}
std::cout << '\n';
```
Output:
```
1,2,3,4
```

#### Use string_view to access the actual memory that's being used to hold a string

You can use `j.as<jsoncons::string_view>()`, e.g.
```cpp
json j = json::parse("\"Hello World\"");
auto sv = j.as<jsoncons::string_view>();
```
`jsoncons::string_view` supports the member functions of `std::string_view`, including `data()` and `size()`. 

If your compiler supports `std::string_view`, you can also use `j.as<std::string_view>()`.

#### Accessing a `json` byte string as a byte string
```cpp
std::vector<uint8_t> u = {'H','e','l','l','o'};

json j(byte_string_arg, u, semantic_tag::base64);

auto bytes = j.as<std::vector<uint8_t>>();
std::cout << "(1) ";
for (auto b : bytes)
{
    std::cout << (char)b;
}
std::cout << "\n\n";

std::string s;
encode_json(j, s); // tag information is lost 
std::cout << "(2) " << s << "\n\n";

auto sj = decode_json<json>(s);

// provide hint
auto v = sj.as<std::vector<uint8_t>>(byte_string_arg,
                                     semantic_tag::base64);

assert(v == u);
```
Output:
```
(1) Hello

(2) "SGVsbG8="
```

