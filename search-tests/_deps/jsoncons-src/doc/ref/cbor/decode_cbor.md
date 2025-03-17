### jsoncons::cbor::decode_cbor

```cpp
#include <jsoncons_ext/cbor/cbor.hpp>
```

<br>

Decodes a [Concise Binary Object Representation](http://cbor.io/) data format into a C++ data structure.

```cpp
template <typename T>
T decode_cbor(const std::vector<uint8_t>& source,
    const cbor_decode_options& options = cbor_decode_options());        (1) (until 0.152.0)

template <typename T,typename Source>
T decode_cbor(const Source& source,
    const cbor_decode_options& options = cbor_decode_options());        (1) (since 0.152.0)

template <typename T>
T decode_cbor(std::istream& is,
    const cbor_decode_options& options = cbor_decode_options());        (2)

template <typename T,typename InputIt>
T decode_cbor(InputIt first, InputIt last,
    const cbor_decode_options& options = cbor_decode_options());        (3) (since 0.153.0)

template <typename T,typename Source,class Allocator,class TempAllocator>
T decode_cbor(const allocator_set<Allocator,TempAllocator>& alloc_set,
    const Source& source,
    const cbor_decode_options& options = cbor_decode_options());        (4) (since 0.171.0)

template <typename T,class Allocator,class TempAllocator>
T decode_cbor(const allocator_set<Allocator,TempAllocator>& alloc_set,
    std::istream& is,
    const cbor_decode_options& options = cbor_decode_options());        (5) (since 0.171.0)
```

(1) Reads CBOR data from a contiguous byte sequence provided by `source` into a type T, using the specified (or defaulted) [options](cbor_options.md). 
Type `Source` must be a container that has member functions `data()` and `size()`, 
and member type `value_type` with size exactly 8 bits (since 0.152.0.)
Any of the values types `int8_t`, `uint8_t`, `char`, `unsigned char` and `std::byte` (since C++17) are allowed.
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md). 

(2) Reads CBOR data from a binary stream into a type T, using the specified (or defaulted) [options](cbor_options.md). 
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md).

(3) Reads CBOR data from the range [`first`,`last`) into a type T, using the specified (or defaulted) [options](cbor_options.md). 
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md).

Functions (4)-(5) are identical to (1)-(2) except an [allocator_set](../allocator_set.md) is passed as an additional argument and
provides allocators for result data and temporary allocations.

#### Exceptions

Throws a [ser_error](../ser_error.md) if parsing fails, and a [conv_error](conv_error.md) if type conversion fails.

### Examples

#### Round trip (JSON to CBOR bytes back to JSON)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    ojson j1 = ojson::parse(R"(
    {
       "application": "hiking",
       "reputons": [
       {
           "rater": "HikingAsylum",
           "assertion": "advanced",
           "rated": "Marilyn C",
           "rating": 0.90
         }
       ]
    }
    )");

    std::vector<uint8_t> v;
    cbor::encode_cbor(j1, v);

    ojson j2 = cbor::decode_cbor<ojson>(v);
    std::cout << pretty_print(j2) << '\n';
}
```
Output:
```json
{
    "application": "hiking",
    "reputons": [
        {
            "rater": "HikingAsylum",
            "assertion": "advanced",
            "rated": "Marilyn C",
            "rating": 0.9
        }
    ]
}
```

#### Round trip (JSON to CBOR file back to JSON)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    json j = json::parse(R"(
    {
       "application": "hiking",
       "reputons": [
       {
           "rater": "HikingAsylum",
           "assertion": "advanced",
           "rated": "Marilyn C",
           "rating": 0.90
         }
       ]
    }
    )");

    std::ofstream os;
    os.open("./output/store.cbor", std::ios::binary | std::ios::out);
    cbor::encode_cbor(j,os);

    std::vector<uint8_t> v;
    std::ifstream is;
    is.open("./output/store.cbor", std::ios::binary | std::ios::in);

    json j2 = cbor::decode_cbor<json>(is);

    std::cout << pretty_print(j2) << '\n'; 
}
```
Output:
```json
{
    "application": "hiking",
    "reputons": [
        {
            "assertion": "advanced",
            "rated": "Marilyn C",
            "rater": "HikingAsylum",
            "rating": 0.9
        }
    ]
}
```

#### Decode CBOR byte string

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    // byte string of length 5
    std::vector<uint8_t> buf = {0x45,'H','e','l','l','o'};
    json j = cbor::decode_cbor<json>(buf);

    auto bstr = j.as<std::vector<uint8_t>>();

    // use byte_string_view to display as hex
    std::cout << "(1) "<< byte_string_view(bstr) << "\n\n";

    // byte string value to JSON text becomes base64url
    std::cout << "(2) " << j << '\n';
}
```
Output:
```
(1) 48,65,6c,6c,6f

(2) "SGVsbG8"
```

#### Decode CBOR byte string with base64 encoding hint

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    // semantic tag indicating expected conversion to base64
    // followed by byte string of length 5
    std::vector<uint8_t> buf = {0xd6,0x45,'H','e','l','l','o'};
    json j = cbor::decode_cbor<json>(buf);

    auto bs = j.as<byte_string>();

    // byte_string to ostream displays as hex
    std::cout << "(1) "<< bs << "\n\n";

    // byte string value to JSON text becomes base64
    std::cout << "(2) " << j << '\n';
}
```
Output:
```
(1) 48 65 6c 6c 6f

(2) "SGVsbG8="
```

#### Decode packed strings [stringref-namespace, stringref](http://cbor.schmorp.de/stringref)

This example taken from [CBOR stringref extension](http://cbor.schmorp.de/stringref) shows three stringref-namespace tags, 
with two nested inside another:

```cpp
int main()
{
    std::vector<uint8_t> v = {0xd9,0x01,0x00, // tag(256)
      0x85,                 // array(5)
         0x63,              // text(3)
            0x61,0x61,0x61, // "aaa"
         0xd8, 0x19,        // tag(25)
            0x00,           // unsigned(0)
         0xd9, 0x01,0x00,   // tag(256)
            0x83,           // array(3)
               0x63,        // text(3)
                  0x62,0x62,0x62, // "bbb"
               0x63,        // text(3)
                  0x61,0x61,0x61, // "aaa"
               0xd8, 0x19,  // tag(25)
                  0x01,     // unsigned(1)
         0xd9, 0x01,0x00,   // tag(256)
            0x82,           // array(2)
               0x63,        // text(3)
                  0x63,0x63,0x63, // "ccc"
               0xd8, 0x19,  // tag(25)
                  0x00,     // unsigned(0)
         0xd8, 0x19,        // tag(25)
            0x00           // unsigned(0)
    };

    ojson j = cbor::decode_cbor<ojson>(v);

    std::cout << pretty_print(j) << "\n";
}
```
Output:
```
[
    "aaa",
    "aaa",
    ["bbb", "aaa", "aaa"],
    ["ccc", "ccc"],
    "aaa"
]
```

#### Decode Typed Array tags

jsoncons implements [Tags for Typed Arrays](https://tools.ietf.org/html/rfc8746).
Tags 64-82 and Tags 84-86 are automatically decoded when detected.

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <iomanip>

int main()
{
    const std::vector<uint8_t> input = {
      0xd8,0x52, // Tag 82 (float64 big endian Typed Array)
        0x50,    // Byte string value of length 16
            0xff, 0xef, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0x7f, 0xef, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff
    };

    auto j = cbor::decode_cbor<json>(input);
    std::cout << "(1)\n" << pretty_print(j) << "\n\n";

    auto v = cbor::decode_cbor<std::vector<double>>(input);
    std::cout << "(2)\n";
    for (auto item : v)
    {
        std::cout << std::defaultfloat << item << "\n";
    }
    std::cout << "\n";

    std::vector<uint8_t> output1;
    cbor::encode_cbor(v, output1);

    // output1 contains a classical CBOR array
    std::cout << "(3)\n" << byte_string_view(output1) << "\n\n";

    std::vector<uint8_t> output2;
    auto options = cbor::cbor_options{}
        .use_typed_arrays(true);
    cbor::encode_cbor(v, output2, options);

    // output2 contains a float64, native endian, Typed Array 
    std::cout << "(4)\n" << byte_string_view(output2) << "\n\n";
}
```

Output:
```
(1)
[
    -1.7976931348623157e+308,
    1.7976931348623157e+308
]

(2)
-1.79769e+308
1.79769e+308

(3)
82 fb ff ef ff ff ff ff ff ff fb 7f ef ff ff ff ff ff ff

(4)
d8 56 50 ff ff ff ff ff ff ef ff ff ff ff ff ff ff ef 7f
```

#### Decode Typed Array tags - multi-dimensional row major tag 

jsoncons implements the tags for row-major and column-major order multi-dimensional arrays, as defined in [Tags for Typed Arrays](https://tools.ietf.org/html/rfc8746).

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

int main()
{
    const std::vector<uint8_t> input = {
      0xd8,0x28,     // Tag 40 (multi-dimensional row major array)
        0x82,        // array(2)
          0x82,      // array(2)
            0x02,    // unsigned(2) 1st Dimension
            0x03,    // unsigned(3) 2nd Dimension
          0xd8,0x41,     // Tag 65 (uint16 big endian Typed Array)
            0x4c,        // byte string(12)
              0x00,0x02, // unsigned(2)
              0x00,0x04, // unsigned(4)
              0x00,0x08, // unsigned(8)
              0x00,0x04, // unsigned(4)
              0x00,0x10, // unsigned(16)
              0x01,0x00  // unsigned(256)
    };

    json j = cbor::decode_cbor<json>(input);

    std::cout << j.tag() << "\n";
    std::cout << pretty_print(j) << "\n";
}
```
Output:
```
multi-dim-row-major
[
    [2, 3],
    [2, 4, 8, 4, 16, 256]
]
```

### See also

[byte_string_view](../byte_string_view.md)  

[encode_cbor](encode_cbor.md) encodes a json value to the [Concise Binary Object Representation](http://cbor.io/) data format.  


