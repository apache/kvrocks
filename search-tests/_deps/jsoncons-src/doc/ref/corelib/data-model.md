### jsoncons data model

The jsoncons data model supports the familiar JSON types - nulls,
booleans, numbers, strings, arrays, objects - plus byte strings. 

Type|Description
----|-----------
null|
bool|
uint64|
int64|
half|Half precision floating point
double|Double precision floating point
string|
byte string|
array|
object|

In addition, jsoncons supports semantic tagging of values, allows it to preserve 
these type semantics when parsing JSON-like data formats such as CBOR that have them.

Tag|Description
---|-----------
undefined |  
datetime  |
epoch_second | 
epoch_milli | 
epoch_nano | 
bigint    | 
bigdec    | 
float128  |
bigfloat  | 
base16    | 
base64    | 
base64url | 
uri       | 
clamped   | 
multi_dim_row_major | 
multi_dim_column_major | 
ext |
id |
regex |
code |

### Examples

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

#### json value to CBOR item

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    json j(json_array_arg);

    j.emplace_back("foo");
    std::vector<uint8_t> bstr = {'b','a','r'};
    j.emplace_back(byte_string_arg, bstr);
    j.emplace_back("-18446744073709551617", semantic_tag::bigint);
    j.emplace_back("273.15", semantic_tag::bigdec);
    j.emplace_back("2018-10-19 12:41:07-07:00", semantic_tag::datetime);
    j.emplace_back(1431027667, semantic_tag::epoch_second);
    j.emplace_back(-1431027667, semantic_tag::epoch_second);
    j.emplace_back(1431027667.5, semantic_tag::epoch_second);

    std::cout << "(1)\n" << pretty_print(j) << "\n\n";

    std::vector<uint8_t> bytes;
    cbor::encode_cbor(j, bytes);
    std::cout << "(2)\n" << byte_string_view(bytes) << "\n\n";
/*
88 -- Array of length 8
  63 -- String value of length 3 
    666f6f -- "foo"
  43 -- Byte string value of length 3
    626172 -- 'b''a''r'
  c3 -- Tag 3 (negative bignum)
    49 Byte string value of length 9
      010000000000000000 -- Bytes content
  c4  - Tag 4 (decimal fraction)
    82 -- Array of length 2
      21 -- -2
      19 6ab3 -- 27315
  c0 -- Tag 0 (date-time)
    78 19 -- Length (25)
      323031382d31302d31392031323a34313a30372d30373a3030 -- "2018-10-19 12:41:07-07:00"
  c1 -- Tag 1 (epoch time)
    1a -- uint32_t
      554bbfd3 -- 1431027667 
  c1
    3a
      554bbfd2
  c1
    fb
      41d552eff4e00000
*/
}
```
Output
```
(1)
[
    "foo",
    "YmFy",
    "-18446744073709551617",
    "273.15",
    "2018-10-19 12:41:07-07:00",
    1431027667,
    -1431027667,
    1431027667.5
]

(2)
88,63,66,6f,6f,43,62,61,72,c3,49,01,00,00,00,00,00,00,00,00,c4,82,21,19,6a,b3,c0,78,19,32,30,31,38,2d,31,30,2d,31,39,20,31,32,3a,34,31,3a,30,37,2d,30,37,3a,30,30,c1,1a,55,4b,bf,d3,c1,3a,55,4b,bf,d2,c1,fb,41,d5,52,ef,f4,e0,00,00
```

#### CBOR item to json value

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

void main()
{
    std::vector<uint8_t> bytes;
    cbor::cbor_bytes_encoder encoder(bytes);
    encoder.begin_array(); // indefinite length outer array
    encoder.string_value("foo");
    encoder.byte_string_value(byte_string{'b','a','r'});
    encoder.string_value("-18446744073709551617", semantic_tag::bigint);
    encoder.string_value("273.15", semantic_tag::bigdec);
    encoder.string_value("2018-10-19 12:41:07-07:00", semantic_tag::datetime) ;
    encoder.int64_value(1431027667, semantic_tag::epoch_second);
    encoder.int64_value(-1431027667, semantic_tag::epoch_second);
    encoder.double_value(1431027667.5, semantic_tag::epoch_second);
    encoder.end_array();
    encoder.flush();

    std::cout << "(1)\n" << byte_string_view(bytes) << "\n\n";

/*
9f -- Start indefinite length array 
  63 -- String value of length 3 
    666f6f -- "foo"
  43 -- Byte string value of length 3
    626172 -- 'b''a''r'
  c3 -- Tag 3 (negative bignum)
    49 Byte string value of length 9
      010000000000000000 -- Bytes content
  c4  - Tag 4 (decimal fraction)
    82 -- Array of length 2
      21 -- -2
      19 6ab3 -- 27315
  c0 -- Tag 0 (date-time)
    78 19 -- Length (25)
      323031382d31302d31392031323a34313a30372d30373a3030 -- "2018-10-19 12:41:07-07:00"
  c1 -- Tag 1 (epoch time)
    1a -- uint32_t
      554bbfd3 -- 1431027667 
  c1
    3a
      554bbfd2
  c1
    fb
      41d552eff4e00000
  ff -- "break" 
*/

    json j = cbor::decode_cbor<json>(bytes);

    std::cout << "(2)\n" << pretty_print(j) << "\n\n";
```
Output:
```
(1)
9f,63,66,6f,6f,43,62,61,72,c3,49,01,00,00,00,00,00,00,00,00,c4,82,21,19,6a,b3,c0,78,19,32,30,31,38,2d,31,30,2d,31,39,20,31,32,3a,34,31,3a,30,37,2d,30,37,3a,30,30,c1,1a,55,4b,bf,d3,c1,3a,55,4b,bf,d2,c1,fb,41,d5,52,ef,f4,e0,00,00,ff

(2)
[
    "foo",
    "YmFy",
    "-18446744073709551617",
    "273.15",
    "2018-10-19 12:41:07-07:00",
    1431027667,
    -1431027667,
    1431027667.5
]
```

