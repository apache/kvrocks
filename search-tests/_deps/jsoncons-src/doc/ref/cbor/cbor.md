## cbor extension

The cbor extension implements decode from and encode to the IETF standard [Concise Binary Object Representation (CBOR)](http://cbor.io/).
You can either parse into or serialize from a variant-like data structure, [basic_json](../basic_json.md), or your own
data structures, using [json_type_traits](../json_type_traits.md).

[decode_cbor](decode_cbor.md)

[basic_cbor_cursor](basic_cbor_cursor.md)

[encode_cbor](encode_cbor.md)

[basic_cbor_encoder](basic_cbor_encoder.md)

[cbor_options](cbor_options.md)

### Tag handling and extensions

All tags not explicitly mentioned below are ignored.

0 (standard date/time string)  
CBOR standard date/time strings are decoded into jsoncons strings tagged with `semantic_tag::datetime`.
jsoncons strings tagged with `semantic_tag::datetime` are encoded into CBOR standard date/time strings.

1 (epoch time)  
CBOR epoch times are decoded into jsoncons int64_t, uint64_t and double and tagged with `semantic_tag::epoch_second`. 
jsoncons int64_t, uint64_t and double tagged with `semantic_tag::epoch_second` are encoded into CBOR epoch time.

2,3 (positive and negative bignum)  
CBOR positive and negative bignums are decoded into jsoncons strings and tagged with `semantic_tag::bigint`.
jsoncons strings tagged with `semantic_tag::bigint` are encoded into CBOR positive or negative bignums.

4 (decimal fratction)  
CBOR decimal fractions are decoded into jsoncons strings tagged with `semantic_tag::bigdec`.
jsoncons strings tagged with `semantic_tag::bigdec` are encoded into CBOR decimal fractions.

5 (bigfloat)  
CBOR bigfloats are decoded into a jsoncons string that consists of the following parts

- (optional) minus sign
- 0x
- nonempty sequence of hexadecimal digits (defines mantissa)
- p followed with optional minus or plus sign and nonempty sequence of hexadecimal digits (defines base-2 exponent)

and tagged with `semantic_tag::bigfloat`. 

jsoncons strings that consist of the following parts

- (optional) plus or minus sign
- 0x or 0X
- nonempty sequence of hexadecimal digits optionally containing a decimal-point character
- (optional) p or P followed with optional minus or plus sign and nonempty sequence of decimal digits

and tagged with `semantic_tag::bigfloat` are encoded into CBOR bignums.

21, 22, 23 (byte string expected conversion is base64url, base64 or base16)  
CBOR byte strings tagged with 21, 22 and 23 are decoded into jsoncons byte strings tagged with
`semantic_tag::base64url`, `semantic_tag::base64` and `semantic_tag::base16`.
jsoncons byte strings tagged with `semantic_tag::base64url`, `semantic_tag::base64` and `semantic_tag::base16`
are encoded into CBOR byte strings tagged with 21, 22 and 23.

32 (URI)  
CBOR URI strings are decoded into jsoncons strings tagged with `semantic_tag::uri`.
jsoncons strings tagged with  `semantic_tag::uri` are encoded into CBOR URI strings.

33, 34 (UTF-8 string is base64url or base64)  
CBOR strings tagged with 33 and 34 are decoded into jsoncons strings tagged with `semantic_tag::base64url` and `semantic_tag::base64`.
jsoncons strings tagged with `semantic_tag::base64url` and `semantic_tag::base64` are encoded into CBOR strings tagged with 33 and 34.

256, 25 [stringref-namespace, stringref](http://cbor.schmorp.de/stringref)  
Tags 256 and 25 are automatically decoded when detected. They are encoded when CBOR option `pack_strings` is set to true.

64-87 [Tags for Typed Arrays](https://tools.ietf.org/html/rfc8746)  
Tags 64-82 (excepting float128 big endian) and 84-86 (excepting float128 little endian) are automatically decoded when detected. They may be encoded when CBOR option `use_typed_arrays` is set to true.

#### Mappings between CBOR and jsoncons data items

CBOR data item|CBOR tag                                         | jsoncons data item|jsoncons tag  
---------------|------------------------------------------------| --------------|------------------
 null |&#160;                                                   | null          |                  
 undefined |&#160;                                              | null          | undefined        
 true or false |&#160;                                          | bool          |                  
 unsigned or negative integer |&#160;                           | int64         |                  
 unsigned or negative integer | 1 (epoch-based date/time)       | int64         | seconds        
 unsigned integer |&#160;                                       | uint64        |                  
 unsigned integer | 1 (epoch-based date/time)                   | uint64        | seconds        
 half-precision float, float, or double |&#160;                 | half          |                  
 float or double |&#160;                                        | double        |                  
 double | 1 (epoch-based date/time)                             | double        | seconds        
 string |&#160;                                                 | string        |                  
 byte string | 2 (positive bignum) or 2 (negative bignum)       | string        | bigint           
 array | 4 (decimal fraction)                                   | string        | bigdec           
 array | 5 (bigfloat)                                           | string        | bigfloat         
 string | 0 (date/time string)                                  | string        | datetime         
 string | 32 (uri)                                              | string        | uri              
 string | 33 (base64url)                                        | string        | base64url        
 string | 34 (base64)                                           | string        | base64           
 byte string |&#160;                                            | byte_string   |                  
 byte string | 21 (Expected conversion to base64url encoding)   | byte_string   | base64url        
 byte string | 22 (Expected conversion to base64 encoding)      | byte_string   | base64           
 byte string | 23 (Expected conversion to base16 encoding)      | byte_string   | base16           
 array |&#160;                                                  | array         |                  
 map |&#160;                                                    | object        |                  

## Examples

[Working with CBOR data](#A1)  
[Encode and decode of a large typed array](#A2)  
[CBOR and basic_json](#A3)  
[Byte string with unknown CBOR tag (unknown to jsoncons)](#A4)  
[Query CBOR with JSONPath](#A5)  

<div id="A1"/> 

### Working with CBOR data

For the examples below you need to include some header files and initialize a buffer of CBOR data:

```cpp
#include <iomanip>
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

using namespace jsoncons; // for convenience

const std::vector<uint8_t> data = {
    0x9f, // Start indefinte length array
      0x83, // Array of length 3
        0x63, // String value of length 3
          0x66,0x6f,0x6f, // "foo" 
        0x44, // Byte string value of length 4
          0x50,0x75,0x73,0x73, // 'P''u''s''s'
        0xc5, // Tag 5 (bigfloat)
          0x82, // Array of length 2
            0x20, // -1
            0x03, // 3   
      0x83, // Another array of length 3
        0x63, // String value of length 3
          0x62,0x61,0x72, // "bar"
        0xd6, // Expected conversion to base64
        0x44, // Byte string value of length 4
          0x50,0x75,0x73,0x73, // 'P''u''s''s'
        0xc4, // Tag 4 (decimal fraction)
          0x82, // Array of length 2
            0x38, // Negative integer of length 1
              0x1c, // -29
            0xc2, // Tag 2 (positive bignum)
              0x4d, // Byte string value of length 13
                0x01,0x8e,0xe9,0x0f,0xf6,0xc3,0x73,0xe0,0xee,0x4e,0x3f,0x0a,0xd2,
    0xff // "break"
};
```

jsoncons allows you to work with the CBOR data similarly to JSON data:

- As a variant-like data structure, [basic_json](../basic_json.md) 

- As a strongly typed C++ data structure that implements [json_type_traits](../json_type_traits.md) 

- With [cursor-level access](doc/ref/cbor/basic_cbor_cursor.md) to a stream of parse events

#### As a variant-like data structure

```cpp
int main()
{
    // Parse the CBOR data into a json value
    json j = cbor::decode_cbor<json>(data);

    // Pretty print
    std::cout << "(1)\n" << pretty_print(j) << "\n\n";

    // Iterate over rows
    std::cout << "(2)\n";
    for (const auto& row : j.array_range())
    {
        std::cout << row[1].as<jsoncons::byte_string>() << " (" << row[1].tag() << ")\n";
    }
    std::cout << "\n";

    // Select the third column with JSONPath
    std::cout << "(3)\n";
    json result = jsonpath::json_query(j,"$[*][2]");
    std::cout << pretty_print(result) << "\n\n";

    // Serialize back to CBOR
    std::vector<uint8_t> buffer;
    cbor::encode_cbor(j, buffer);
    std::cout << "(4)\n" << byte_string_view(buffer) << "\n\n";
}
```
Output:
```
(1)
[
    ["foo", "UHVzcw", "0x3p-1"],
    ["bar", "UHVzcw==", "1.23456789012345678901234567890"]
]

(2)
50,75,73,73 (n/a)
50,75,73,73 (base64)

(3)
[
    "0x3p-1",
    "1.23456789012345678901234567890"
]

(4)
82,83,63,66,6f,6f,44,50,75,73,73,c5,82,20,03,83,63,62,61,72,d6,44,50,75,73,73,c4,82,38,1c,c2,4d,01,8e,e9,0f,f6,c3,73,e0,ee,4e,3f,0a,d2
```

#### As a strongly typed C++ data structure

```cpp
int main()
{
    // Parse the string of data into a std::vector<std::tuple<std::string,jsoncons::byte_string,std::string>> value
    auto val = cbor::decode_cbor<std::vector<std::tuple<std::string,jsoncons::byte_string,std::string>>>(data);

    std::cout << "(1)\n";
    for (const auto& row : val)
    {
        std::cout << std::get<0>(row) << ", " << std::get<1>(row) << ", " << std::get<2>(row) << "\n";
    }
    std::cout << "\n";

    // Serialize back to CBOR
    std::vector<uint8_t> buffer;
    cbor::encode_cbor(val, buffer);
    std::cout << "(2)\n" << byte_string_view(buffer) << "\n\n";
}
```
Output:
```
(1)
foo, 50,75,73,73, 0x3p-1
bar, 50,75,73,73, 1.23456789012345678901234567890

(2)
82,9f,63,66,6f,6f,44,50,75,73,73,66,30,78,33,70,2d,31,ff,9f,63,62,61,72,44,50,75,73,73,78,1f,31,2e,32,33,34,35,36,37,38,39,30,31,32,33,34,35,36,37,38,39,30,31,32,33,34,35,36,37,38,39,30,ff
```

Note that when decoding the bigfloat and decimal fraction into a `std::string`, we lose the semantic information
that the variant like data structure preserved with a tag, so serializing back to CBOR produces a text string.

#### With cursor-level access

```cpp
int main()
{
    cbor::cbor_bytes_cursor cursor(data);
    for (; !cursor.done(); cursor.next())
    {
        const auto& event = cursor.current();
        switch (event.event_type())
        {
            case staj_event_type::begin_array:
                std::cout << event.event_type() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::end_array:
                std::cout << event.event_type() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::begin_object:
                std::cout << event.event_type() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::end_object:
                std::cout << event.event_type() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::key:
                // Or std::string_view, if supported
                std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::string_value:
                // Or std::string_view, if supported
                std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::byte_string_value:
                std::cout << event.event_type() << ": " << event.get<jsoncons::byte_string_view>() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::null_value:
                std::cout << event.event_type() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::bool_value:
                std::cout << event.event_type() << ": " << std::boolalpha << event.get<bool>() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::int64_value:
                std::cout << event.event_type() << ": " << event.get<int64_t>() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::uint64_value:
                std::cout << event.event_type() << ": " << event.get<uint64_t>() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::half_value:
            case staj_event_type::double_value:
                std::cout << event.event_type() << ": "  << event.get<double>() << " " << "(" << event.tag() << ")\n";
                break;
            default:
                std::cout << "Unhandled event type " << event.event_type() << " " << "(" << event.tag() << ")\n";
                break;
        }
    }
}
```
Output:
```
begin_array (n/a)
begin_array (n/a)
string_value: foo (n/a)
byte_string_value: 50,75,73,73 (n/a)
string_value: 0x3p-1 (bigfloat)
end_array (n/a)
begin_array (n/a)
string_value: bar (n/a)
byte_string_value: 50,75,73,73 (base64)
string_value: 1.23456789012345678901234567890 (bigdec)
end_array (n/a)
end_array (n/a)
```

You can apply a filter to a cursor using the pipe syntax, for example,

```cpp
int main()
{
    auto filter = [&](const staj_event& ev, const ser_context&) -> bool
    {
        return (ev.tag() == semantic_tag::bigdec) || (ev.tag() == semantic_tag::bigfloat);  
    };

    cbor::cbor_bytes_cursor cursor(data);

    auto filtered_c = cursor | filter;
    for (; !filtered_c.done(); filtered_c.next())
    {
        const auto& event = filtered_c.current();
        switch (event.event_type())
        {
            case staj_event_type::string_value:
                // Or std::string_view, if supported
                std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << " " << "(" << event.tag() << ")\n";
                break;
            default:
                std::cout << "Unhandled event type " << event.event_type() << " " << "(" << event.tag() << ")\n";
                break;
        }
    }
}
```
Output:
```
string_value: 0x3p-1 (bigfloat)
string_value: 1.23456789012345678901234567890 (bigdec)
```

<div id="A2"/> 

### Encode and decode of a large typed array

```cpp
#include <jsoncons_ext/cbor/cbor.hpp>
#include <iomanip>
#include <cassert>

using namespace jsoncons;

int main()
{
    std::vector<float> x(15000000); 
    for (std::size_t i = 0; i < x.size(); ++i)
    {
        x[i] = static_cast<float>(i);
    }
    auto options = cbor::cbor_options{}
        .use_typed_arrays(true);

    std::vector<uint8_t> buf;
    cbor::encode_cbor(x, buf, options);

    std::cout << "first 19 bytes:\n\n";
    std::cout << byte_string_view(buf).substr(0, 19) << "\n\n";
/*
    0xd8,0x55 -- Tag 85 (float32 little endian Typed Array)
    0x5a - byte string (four-byte uint32_t for n, and then  n bytes follow)
      03 93 87 00 -- 60000000
        00 00 00 00 -- 0.0f
        00 00 80 3f -- 1.0f
        00 00 00 40 -- 2.0f
*/
    auto y = cbor::decode_cbor<std::vector<float>>(buf);

    assert(y == x);
}
```
Output:
```
first 19 bytes:

d8,55,5a,03,93,87,00,00,00,00,00,00,00,80,3f,00,00,00,40
```

<div id="A3"/> 

### CBOR and basic_json

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

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

    // Encode a basic_json value to a CBOR value
    std::vector<uint8_t> data;
    cbor::encode_cbor(j1, data);

    // Decode a CBOR value to a basic_json value
    ojson j2 = cbor::decode_cbor<ojson>(data);
    std::cout << "(1)\n" << pretty_print(j2) << "\n\n";

    // Accessing the data items 

    const ojson& reputons = j2["reputons"];

    std::cout << "(2)\n";
    for (auto element : reputons.array_range())
    {
        std::cout << element.at("rated").as<std::string>() << ", ";
        std::cout << element.at("rating").as<double>() << "\n";
    }
    std::cout << '\n';

    // Get a CBOR value for a nested data item with jsonpointer
    std::error_code ec;
    const auto& rated = jsonpointer::get(j2, "/reputons/0/rated", ec);
    if (!ec)
    {
        std::cout << "(3) " << rated.as_string() << "\n";
    }

    std::cout << '\n';
}
```
Output:
```
(1)
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

(2)
Marilyn C, 0.9

(3) Marilyn C
```

<div id="A4"/> 

### Byte string with unknown CBOR tag (unknown to jsoncons)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

int main()
{
    // Create some CBOR
    std::vector<uint8_t> buffer;
    cbor::cbor_bytes_encoder encoder(buffer);

    std::vector<uint8_t> bstr = {'f','o','o','b','a','r'};
    encoder.byte_string_value(bstr, 274); // byte string with tag 274
    encoder.flush();

    std::cout << "(1)\n" << byte_string_view(buffer) << "\n\n";

    /*
        d9, // tag
            01,12, // 274
        46, // byte string, length 6
            66,6f,6f,62,61,72 // 'f','o','o','b','a','r'         
    */ 

    json j = cbor::decode_cbor<json>(buffer);

    std::cout << "(2)\n" << pretty_print(j) << "\n\n";
    std::cout << "(3) " << j.tag() << "("  << j.ext_tag() << ")\n\n";

    // Get byte string as a std::vector<uint8_t>
    auto bstr2 = j.as<std::vector<uint8_t>>();

    std::vector<uint8_t> buffer2;
    cbor::encode_cbor(j, buffer2);
    std::cout << "(4)\n" << byte_string_view(buffer2.data(),buffer2.size()) << "\n";
}
```
Output:
```
(1)
d9,01,12,46,66,6f,6f,62,61,72

(2)
"Zm9vYmFy"

(3) ext(274)

(4)
d9,01,12,46,66,6f,6f,62,61,72
```

<div id="A5"/> 

### Query CBOR with JSONPath
```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <iostream>
#include <iomanip>
#include <cassert>

using namespace jsoncons; // For convenience

int main()
{
    // Construct a json array of numbers
    json j(json_array_arg);

    j.emplace_back(5.0);

    j.emplace_back(0.000071);

    j.emplace_back("-18446744073709551617",semantic_tag::bigint);

    j.emplace_back("1.23456789012345678901234567890", semantic_tag::bigdec);

    j.emplace_back("0x3p-1", semantic_tag::bigfloat);

    // Encode to JSON
    std::cout << "(1)\n";
    std::cout << pretty_print(j);
    std::cout << "\n\n";

    // as<std::string>() and as<double>()
    std::cout << "(2)\n";
    std::cout << std::dec << std::setprecision(15);
    for (const auto& item : j.array_range())
    {
        std::cout << item.as<std::string>() << ", " << item.as<double>() << "\n";
    }
    std::cout << "\n";

    // Encode to CBOR
    std::vector<uint8_t> v;
    cbor::encode_cbor(j,v);

    std::cout << "(3)\n" << byte_string_view(v) << "\n\n";
/*
    85 -- Array of length 5     
      fa -- float 
        40a00000 -- 5.0
      fb -- double 
        3f129cbab649d389 -- 0.000071
      c3 -- Tag 3 (negative bignum)
        49 -- Byte string value of length 9
          010000000000000000
      c4 -- Tag 4 (decimal fraction)
        82 -- Array of length 2
          38 -- Negative integer of length 1
            1c -- -29
          c2 -- Tag 2 (positive bignum)
            4d -- Byte string value of length 13
              018ee90ff6c373e0ee4e3f0ad2
      c5 -- Tag 5 (bigfloat)
        82 -- Array of length 2
          20 -- -1
          03 -- 3   
*/

    // Decode back to json
    json other = cbor::decode_cbor<json>(v);
    assert(other == j);

    // Query with JSONPath
    std::cout << "(4)\n";
    json result = jsonpath::json_query(other,"$[?(@ < 1.5)]");
    std::cout << pretty_print(result) << "\n\n";
}
```
Output:
```
(1)
[
    5.0,
    7.1e-05,
    "-18446744073709551617",
    "1.23456789012345678901234567890",
    "0x3p-1"
]

(2)
5.0, 5
7.1e-05, 7.1e-05
-18446744073709551617, -1.84467440737096e+19
1.23456789012345678901234567890, 1.23456789012346
0x3p-1, 1.5

(3)
85,fa,40,a0,00,00,fb,3f,12,9c,ba,b6,49,d3,89,c3,49,01,00,00,00,00,00,00,00,00,c4,82,38,1c,c2,4d,01,8e,e9,0f,f6,c3,73,e0,ee,4e,3f,0a,d2,c5,82,20,03

(4)
[
    7.1e-05,
    "-18446744073709551617",
    "1.23456789012345678901234567890"
]
```

### See also

[byte_string_view](../byte_string_view.md)
