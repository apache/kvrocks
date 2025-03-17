### jsoncons::cbor::encode_cbor

```cpp
#include <jsoncons_ext/cbor/cbor.hpp>

template <typename T,typename ByteContainer>
void encode_cbor(const T& jval, ByteContainer& cont,
    const cbor_decode_options& options = cbor_decode_options());            (1)

template <typename T>
void encode_cbor(const T& val, std::ostream& os, 
    const cbor_encode_options& options = cbor_encode_options());            (2)

template <typename T,typename ByteContainer>
void encode_cbor(const allocator_set<Allocator,TempAllocator>& alloc_set,
    const T& jval, ByteContainer& cont,
    const cbor_decode_options& options = cbor_decode_options());            (3) (since 0.171.0)

template <typename T>
void encode_cbor(const allocator_set<Allocator,TempAllocator>& alloc_set,
    const T& val, std::ostream& os, 
    const cbor_encode_options& options = cbor_encode_options());            (4) (since 0.171.0)
```

Encodes a C++ data structure to the [Concise Binary Object Representation](http://cbor.io/) data format.

(1) Writes a value of type T into a byte container in the CBOR data format, using the specified (or defaulted) [options](cbor_options.md). 
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md).
Type `ByteContainer` must be back insertable and have member type `value_type` with size exactly 8 bits (since 0.152.0.)
Any of the values types `int8_t`, `uint8_t`, `char`, `unsigned char` and `std::byte` (since C++17) are allowed.

(2) Writes a value of type T into a binary stream in the CBOR data format, using the specified (or defaulted) [options](cbor_options.md). 
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md). 

Functions (3)-(4) are identical to (1)-(2) except an [allocator_set](../allocator_set.md) is passed as an additional argument.

### Examples

#### cbor example

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    ojson j1;
    j1["zero"] = 0;
    j1["one"] = 1;
    j1["two"] = 2;
    j1["null"] = null_type();
    j1["true"] = true;
    j1["false"] = false;
    j1["max int64_t"] = (std::numeric_limits<int64_t>::max)();
    j1["max uint64_t"] = (std::numeric_limits<uint64_t>::max)();
    j1["min int64_t"] = (std::numeric_limits<int64_t>::lowest)();
    j1["max int32_t"] = (std::numeric_limits<int32_t>::max)();
    j1["max uint32_t"] = (std::numeric_limits<uint32_t>::max)();
    j1["min int32_t"] = (std::numeric_limits<int32_t>::lowest)();
    j1["max int16_t"] = (std::numeric_limits<int16_t>::max)();
    j1["max uint16_t"] = (std::numeric_limits<uint16_t>::max)();
    j1["min int16_t"] = (std::numeric_limits<int16_t>::lowest)();
    j1["max int8_t"] = (std::numeric_limits<int8_t>::max)();
    j1["max uint8_t"] = (std::numeric_limits<uint8_t>::max)();
    j1["min int8_t"] = (std::numeric_limits<int8_t>::lowest)();
    j1["max double"] = (std::numeric_limits<double>::max)();
    j1["min double"] = (std::numeric_limits<double>::lowest)();
    j1["max float"] = (std::numeric_limits<float>::max)();
    j1["zero float"] = 0.0;
    j1["min float"] = (std::numeric_limits<float>::lowest)();
    j1["Key too long for small string optimization"] = "String too long for small string optimization";

    std::vector<uint8_t> cont;
    cbor::encode_cbor(j1, cont);

    ojson j2 = cbor::decode_cbor<ojson>(cont);

    std::cout << pretty_print(j2) << '\n';
}
```
Output:
```json
{
    "zero": 0,
    "one": 1,
    "two": 2,
    "null": null,
    "true": true,
    "false": false,
    "max int64_t": 9223372036854775807,
    "max uint64_t": 18446744073709551615,
    "min int64_t": -9223372036854775808,
    "max int32_t": 2147483647,
    "max uint32_t": 4294967295,
    "min int32_t": -2147483648,
    "max int16_t": 32767,
    "max uint16_t": 65535,
    "min int16_t": -32768,
    "max int8_t": 127,
    "max uint8_t": 255,
    "min int8_t": -128,
    "max double": 1.79769313486232e+308,
    "min double": -1.79769313486232e+308,
    "max float": 3.40282346638529e+038,
    "zero float": 0.0,
    "min float": -3.40282346638529e+038,
    "Key too long for small string optimization": "String too long for small string optimization"
}
```

#### Encode CBOR byte string

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    // construct byte string value
    std::vector<uint8_t> cont = {'H','e','l','l','o'};
    json j(byte_string_arg, cont);

    std::vector<uint8_t> buf;
    cbor::encode_cbor(j, buf);

    std::cout << "(1) " << byte_string_view(buf) << "\n\n";

    json j2 = cbor::decode_cbor<json>(buf);
    std::cout << "(2) " << j2 << '\n';
}
```
Output:
```
(1) 45,48,65,6c,6c,6f

(2) "SGVsbG8"
```

#### Encode byte string with encoding hint

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    // construct byte string value
    std::vector<uint8_t> cont = {'H','e','l','l','o'};
    json j1(byte_string_arg, cont, semantic_tag::base64);

    std::vector<uint8_t> buf;
    cbor::encode_cbor(j1, buf);

    std::cout << "(1) " << byte_string_view(buf) << "\n\n";

    json j2 = cbor::decode_cbor<json>(buf);
    std::cout << "(2) " << j2 << '\n';
}
```
Output:
```
(1) d6,45,48,65,6c,6c,6f

(2) "SGVsbG8="
```

#### Encode packed strings [stringref-namespace, stringref](http://cbor.schmorp.de/stringref) 

This example taken from [CBOR stringref extension](http://cbor.schmorp.de/stringref) shows how to encode a
data structure that contains many repeated strings more efficiently.

```cpp
#include <iomanip>
#include <cassert>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    ojson j = ojson::parse(R"(
[
     {
       "name" : "Cocktail",
       "count" : 417,
       "rank" : 4
     },
     {
       "rank" : 4,
       "count" : 312,
       "name" : "Bath"
     },
     {
       "count" : 691,
       "name" : "Food",
       "rank" : 4
     }
  ]
)");

    auto options = cbor::cbor_encode_options{}
        .pack_strings(true);
    std::vector<uint8_t> buf;

    cbor::encode_cbor(j, buf, options);

    std::cout << byte_string_view(buf) << "\n\n";

/*
    d90100 -- tag (256)
      83 -- array(3)
        a3 -- map(3)
          64 -- text string (4)
            6e616d65 -- "name"
          68 -- text string (8)
            436f636b7461696c -- "Cocktail"
          65 -- text string (5)
            636f756e74 -- "count"
            1901a1 -- unsigned(417)
          64 -- text string (4)
            72616e6b -- "rank"
            04 -- unsigned(4)
        a3 -- map(3)
          d819 -- tag(25)
            03 -- unsigned(3)
          04 -- unsigned(4)
          d819 -- tag(25)
            02 -- unsigned(2)
            190138 -- unsigned(312)
          d819 -- tag(25)
            00 -- unsigned(0)
          64 -- text string(4)
            42617468 -- "Bath"
        a3 -- map(3)
          d819 -- tag(25)
            02 -- unsigned(2)
          1902b3 -- unsigned(691)
          d819 -- tag(25)
            00 -- unsigned(0)
          64 - text string(4)
            466f6f64 -- "Food"
          d819 -- tag(25)
            03 -- unsigned(3)
            04 -- unsigned(4)
*/

    ojson j2 = cbor::decode_cbor<ojson>(buf);
    assert(j2 == j);
}
```
Output:
```
d9,01,00,83,a3,64,6e,61,6d,65,68,43,6f,63,6b,74,61,69,6c,65,63,6f,75,6e,74,19,01,a1,64,72,61,6e,6b,04,a3,d8,19,03,04,d8,19,02,19,01,38,d8,19,00,64,42,61,74,68,a3,d8,19,02,19,02,b3,d8,19,00,64,46,6f,6f,64,d8,19,03,04
```

### See also

[byte_string_view](../byte_string_view.md)  

[decode_cbor](decode_cbor.md) decodes a [Concise Binary Object Representation](http://cbor.io/) data format to a json value.  

