### bson extension

The bson extension implements decode from and encode to the [Binary JSON](http://bsonspec.org/) data format.
You can either parse into or serialize from a variant-like data structure, [basic_json](../basic_json.md), or your own
data structures, using [json_type_traits](../json_type_traits.md).

[decode_bson](decode_bson.md)

[basic_bson_cursor](basic_bson_cursor.md)

[encode_bson](encode_bson.md)

[basic_bson_encoder](basic_bson_encoder.md)

[bson_options](bson_options.md)

#### Mappings between BSON and jsoncons data items

BSON data item                   | jsoncons data item |jsoncons semantic_tag
---------------------------------|---------------|------------
 null                            | null          |  
 true or false                   | bool          |                  
 int32 or int64                  | int64         |                  
 int32 or int64                  | uint64        |                  
 UTC datetime                    | int64         | epoch_milli
 Timestamp                       | uint64        |
 double                          | double        |                  
 string                          | string        |                  
 binary                          | byte_string   | ext    
 array                           | array         |                  
 embedded document               | object        |   
 decimal128 (since 0.165.0)      | string        | float128              
 ObjectId (since 0.165.0)        | string        | id              
 regex (since 0.165.0)           | string        | regex              
 JavaScript code (since 0.165.0) | string        | code
 min_key (since 0.165.0)         | string        |               
 max_key (since 0.165.0)         | string        |               
 undefined (since 0.165.0)       | null          | undefined                
 symbol (since 0.165.0)          | string        |               

### Examples

[Document with string and binary](#eg1)  
[Decode a BSON 128-bit decimal floating point (since 0.165.0)](#eg2)  
[Encode a BSON 128-bit decimal floating point (since 0.165.0)](#eg3)  
[Regular expression data (since 0.165.0)](#eg4)  
[ObjectId data (since 0.165.0)](#eg5)  

 <div id="eg1"/>

#### Document with string and binary

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/bson/bson.hpp>
#include <cassert>

using jsoncons::json;
namespace bson = jsoncons::bson; // for brevity

int main()
{
    // Create some bson
    std::vector<uint8_t> buffer;
    bson::bson_bytes_encoder encoder(buffer);
    encoder.begin_object(); // The total number of bytes comprising 
                            // the bson document will be calculated
    encoder.key("Hello");
    encoder.string_value("World");
    encoder.key("Data");
    std::vector<uint8_t> bstr = {'f','o','o','b','a','r'};
    encoder.byte_string_value(bstr); // default subtype is user defined
    // or encoder.byte_string_value(bstr, 0x80); 
    encoder.end_object();
    encoder.flush();

    std::cout << "(1)\n" << jsoncons::byte_string_view(buffer) << "\n";

    /*
        0x27,0x00,0x00,0x00, // Total number of bytes comprising the document (40 bytes) 
            0x02, // URF-8 string
                0x48,0x65,0x6c,0x6c,0x6f, // Hello
                0x00, // trailing byte 
            0x06,0x00,0x00,0x00, // Number bytes in string (including trailing byte)
                0x57,0x6f,0x72,0x6c,0x64, // World
                0x00, // trailing byte
            0x05, // binary
                0x44,0x61,0x74,0x61, // Data
                0x00, // trailing byte
            0x06,0x00,0x00,0x00, // number of bytes
                0x80, // subtype
                0x66,0x6f,0x6f,0x62,0x61,0x72,
        0x00
    */

    ojson j = bson::decode_bson<ojson>(buffer);

    std::cout << "(2)\n" << pretty_print(j) << "\n\n";
    std::cout << "(3) " << j["Data"].tag() << "("  << j["Data"].ext_tag() << ")\n\n";

    // Get binary value as a std::vector<uint8_t>
    auto bstr2 = j["Data"].as<std::vector<uint8_t>>();
    assert(bstr2 == bstr);

    std::vector<uint8_t> buffer2;
    bson::encode_bson(j, buffer2);
    assert(buffer2 == buffer);
}
```
Output:
```
(1)
27,00,00,00,02,48,65,6c,6c,6f,00,06,00,00,00,57,6f,72,6c,64,00,05,44,61,74,61,00,06,00,00,00,80,66,6f,6f,62,61,72,00

(2)
{
    "Hello": "World",
    "Data": "Zm9vYmFy"
}

(3) ext(128)
```

 <div id="eg2"/>

#### Decode a BSON 128-bit decimal floating point (since 0.165.0)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/bson/bson.hpp>
#include <cassert>

using jsoncons::json;
namespace bson = jsoncons::bson; // for brevity

int main()
{
    std::vector<uint8_t> input = {
        0x18,0x00,0x00,0x00, // Document has 24 bytes
        0x13,                // 128-bit decimal floating point
        0x61,0x00,           // "a"
        0x01,0x00,0x00,0x00,
        0x00,0x00,0x00,0x00, // 1E-6176
        0x00,0x00,0x00,0x00,
        0x00,0x00,0x00,0x00, 
        0x00                 // terminating null
    };

    json j = bson::decode_bson<json>(input);

    std::cout << "(1) " << j << "\n\n";
    std::cout << "(2) " << j.at("a").tag() << "\n\n";

    std::vector<char> output;
    bson::encode_bson(j, output);
    assert(output == input);
}
```
Output:
```
(1) {"a":"1E-6176"}

(2) float128
```

 <div id="eg3"/>

#### Encode a BSON 128-bit decimal floating point (since 0.165.0)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/bson/bson.hpp>
#include <cassert>

using jsoncons::json;
namespace bson = jsoncons::bson; // for brevity

int main()
{
    json j;

    j.try_emplace("a", "1E-6176", jsoncons::semantic_tag::float128);
    // or j["a"] = json("1E-6176", jsoncons::semantic_tag::float128);

    std::cout << "(1) " << j << "\n\n";
    std::cout << "(2) " << j.at("a").tag() << "\n\n";

    std::vector<char> output;
    bson::encode_bson(j, output);
    std::cout << "(3) " << jsoncons::byte_string_view(output) << "\n\n";
    /*
        18,00,00,00,          // document has 24 bytes
          13,                 // 128-bit decimal floating point
            13,00,            // "a"
            01,00,00,00,
            00,00,00,00,      // 1E-6176
            00,00,00,00,
            00,00,00,00, 
        00                    // terminating null    
    */
}
```
Output:
```
(1) {"a":"1E-6176"}

(2) float128

(3) 18,00,00,00,13,61,00,01,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00
```

 <div id="eg4"/>

#### Regular expression data (since 0.165.0)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/bson/bson.hpp>
#include <cassert>

using jsoncons::json;
namespace bson = jsoncons::bson; // for brevity

int main()
{
    std::vector<uint8_t> input = {
        0x16,0x00,0x00,0x00,            // Document has 22 bytes
        0x0B,                           // Regular expression
        0x72,0x65,0x67,0x65,0x78,0x00,  // "regex"
        0x5E,0x61,0x62,0x63,0x64,0x00,  // "^abcd"
        0x69,0x6C,0x78,0x00,            // "ilx"
        0x00                            // terminating null
    };

    json j = bson::decode_bson<json>(input);

    std::cout << "(1) " << j << "\n\n";
    std::cout << "(2) " << j.at("regex").tag() << "\n\n";

    std::vector<char> output;
    bson::encode_bson(j, output);
    assert(output == input);
}
```
Output:
```
(1) {"regex":"/^abcd/ilx"}

(2) regex
```

 <div id="eg5"/>

#### ObjectId data (since 0.165.0)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/bson/bson.hpp>
#include <cassert>

using jsoncons::json;
namespace bson = jsoncons::bson; // for brevity

int main()
{
    std::vector<uint8_t> input = {
        0x16,0x00,0x00,0x00,            // Document has 22 bytes
        0x07,                           // ObjectId
        0x6F,0x69,0x64,0x00,            // "oid"
        0x12,0x34,0x56,0x78,0x90,0xAB,  
        0xCD,0xEF,0x12,0x34,0xAB,0xCD,  // (byte*12)
        0x00                            // terminating null
    };

    json j = bson::decode_bson<json>(input);

    std::cout << "(1) " << j << "\n\n";
    std::cout << "(2) " << j.at("oid").tag() << "\n\n";

    std::vector<char> output;
    bson::encode_bson(j, output);
    assert(output == input);
}
```
Output:
```
(1) {"oid":"1234567890abcdef1234abcd"}

(2) id
```

### See also

[byte_string_view](../byte_string_view.md)


### Acknowledgements

The jsoncons implementations of BSON decimal128 to and from string,
and ObjectId to and from string, are based on the Apache 2 licensed [libbson](https://github.com/mongodb/mongo-c-driver/tree/master/src/libbson).

The decimal128, regex, and ObjectId examples are from the libbson test cases.

