### jsoncons::bson::encode_bson

Encodes a C++ data structure to the [Binary JSON (BSON)](http://bsonspec.org/) data format.

```cpp
#include <jsoncons_ext/bson/bson.hpp>

template <typename T,typename ByteContainer>
void encode_bson(const T& jval, ByteContainer& cont,
    const bson_decode_options& options = bson_decode_options());            (1) 

template <typename T>
void encode_bson(const T& jval, std::ostream& os,
    const bson_decode_options& options = bson_decode_options());            (2)

template <typename T,typename ByteContainer>
void encode_bson(const allocator_set<Allocator,TempAllocator>& alloc_set,
    const T& jval, ByteContainer& cont,
    const bson_decode_options& options = bson_decode_options());            (3) (since 0.171.0) 

template <typename T>
void encode_bson(const allocator_set<Allocator,TempAllocator>& alloc_set,
    const T& jval, std::ostream& os,
    const bson_decode_options& options = bson_decode_options());            (4) (since 0.171.0)
```

(1) Writes a value of type T into a byte container in the BSON data format, using the specified (or defaulted) [options](bson_options.md). 
Type 'T' must be an instantiation of [basic_json](basic_json.md) 
or support [json_type_traits](../json_type_traits.md). 
Type `ByteContainer` must be back insertable and have member type `value_type` with size exactly 8 bits (since 0.152.0.)
Any of the values types `int8_t`, `uint8_t`, `char`, `unsigned char` and `std::byte` (since C++17) are allowed.

(2) Writes a value of type T into a binary stream in the BSON data format, using the specified (or defaulted) [options](bson_options.md). 
Type 'T' must be an instantiation of [basic_json](basic_json.md) 
or support [json_type_traits](../json_type_traits.md). 

Functions (3)-(4) are identical to (1)-(2) except an [allocator_set](../allocator_set.md) is passed as an additional argument.

### Examples

#### null

```cpp
void null_example()
{
    json j;
    j.try_emplace("hello", null_type()); 

    std::vector<char> buffer;
    bson::encode_bson(j, buffer);
    std::cout << byte_string_view(buffer) << "\n\n";
}
```
Output:
```
0c,00,00,00,0a,68,65,6c,6c,6f,00,00
```

#### bool

```cpp
int main()
{
    std::map<std::string, bool> m{ {"a", true} };

    std::vector<char> buffer;
    bson::encode_bson(m, buffer);
    std::cout << byte_string_view(buffer) << "\n\n";
}
```
Output:
```
09,00,00,00,08,61,00,01,00
```

#### int32

```cpp
int main()
{
    ojson j(json_object_arg);
    j.try_emplace("a", -123); // int32
    j.try_emplace("c", 0); // int32
    j.try_emplace("b", 123); // int32

    std::vector<char> buffer;
    bson::encode_bson(j, buffer);
    std::cout << byte_string_view(buffer) << "\n\n";
}
```
Output:
```
1a,00,00,00,10,61,00,85,ff,ff,ff,10,63,00,00,00,00,00,10,62,00,7b,00,00,00,00
```

#### int64

```cpp
int main()
{
    std::map<std::string, int64_t> m{ {"a", 100000000000000ULL} };

    std::vector<char> buffer;
    bson::encode_bson(m, buffer);
    std::cout << byte_string_view(buffer) << "\n\n";
}
```
Output:
```
10,00,00,00,12,61,00,00,40,7a,10,f3,5a,00,00,00
```

#### double

```cpp
 int main()
 {
     std::map<std::string, double> m{ {"a", 123.4567} };

     std::vector<char> buffer;
     bson::encode_bson(m, buffer);
     std::cout << byte_string_view(buffer) << "\n\n";
 }
```
Output:
```
10,00,00,00,01,61,00,53,05,a3,92,3a,dd,5e,40,00
```

#### utf8 string

```cpp
int main()
{
    json j;
    j.try_emplace("hello", "world");

    std::vector<char> buffer;
    bson::encode_bson(j, buffer);
    std::cout << byte_string_view(buffer) << "\n\n";
}
```
Output:
```
16,00,00,00,02,68,65,6c,6c,6f,00,06,00,00,00,77,6f,72,6c,64,00,00
```

#### binary

```cpp
int main()
{
    json j;
    std::vector<uint8_t> bstr = {'1', '2', '3', '4'};
    j.try_emplace("binary", byte_string_arg, bstr); // default subtype is user defined
    // or j.try_emplace("binary", byte_string_arg, bstr, 0x80);

    std::vector<char> buffer;
    bson::encode_bson(j, buffer);
    std::cout << byte_string_view(buffer) << "\n\n";
}
```
Output:
```
16,00,00,00,05,62,69,6e,61,72,79,00,04,00,00,00,80,31,32,33,34,00
```

#### array

```cpp
int main()
{
    json a(json_array_arg);
    a.push_back("hello");
    a.push_back("world");

    json j(json_object_arg);
    j["array"] = std::move(a);

    std::vector<char> buffer;
    bson::encode_bson(j, buffer);

    std::cout << byte_string_view(buffer) << "\n\n";
}
```
Output:
```
2b,00,00,00,04,61,72,72,61,79,00,1f,00,00,00,02,30,00,06,00,00,00,68,65,6c,6c,6f,00,02,31,00,06,00,00,00,77,6f,72,6c,64,00,00,00
```

#### UTC datetime

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/bson/bson.hpp>
#include <iostream>

int main()
{
    auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto time = std::chrono::duration_cast<std::chrono::milliseconds>(duration);

    json j;
    j.try_emplace("time", time);

    auto milliseconds = j["time"].as<std::chrono::milliseconds>();
    std::cout << "Time since epoch (milliseconds): " << milliseconds.count() << "\n\n";
    auto seconds = j["time"].as<std::chrono::seconds>();
    std::cout << "Time since epoch (seconds): " << seconds.count() << "\n\n";

    std::vector<uint8_t> data;
    bson::encode_bson(j, data);

    std::cout << "BSON bytes:\n" << jsoncons::byte_string_view(data) << "\n\n";

/*
    13,00,00,00, // document has 19 bytes
      09, // UTC datetime
        74,69,6d,65,00, // "time"
        ea,14,7f,96,73,01,00,00, // 1595957777642
    00 // terminating null    
*/
}
```
Output:
```
Time since epoch (milliseconds): 1595957777642

Time since epoch (seconds): 1595957777

BSON bytes:
13,00,00,00,09,74,69,6d,65,00,ea,14,7f,96,73,01,00,00,00
```

### See also

[decode_bson](decode_bson) decodes a [Bin­ary JSON](http://bsonspec.org/) data format to a json value.

