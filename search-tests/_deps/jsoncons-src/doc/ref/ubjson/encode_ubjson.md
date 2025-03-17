### jsoncons::ubjson::encode_ubjson

Encodes a C++ data structure to the [Universal Binary JSON Specification (UBJSON)](http://ubjsonspec.org/) data format.

```cpp
#include <jsoncons_ext/ubjson/ubjson.hpp>

template <typename T,typename ByteContainer>
void encode_ubjson(const T& jval, ByteContainer& cont,
    const ubjson_decode_options& options = ubjson_decode_options());        (1) 

template <typename T>
void encode_ubjson(const T& jval, std::ostream& os,
    const bson_decode_options& options = bson_decode_options());            (2)

template <typename T,typename ByteContainer>
void encode_ubjson(const allocator_set<Allocator,TempAllocator>& alloc_set,
    const T& jval, ByteContainer& cont,
    const ubjson_decode_options& options = ubjson_decode_options());        (3) (since 0.171.0)

template <typename T>
void encode_ubjson(const allocator_set<Allocator,TempAllocator>& alloc_set,
    const T& jval, std::ostream& os,
    const bson_decode_options& options = bson_decode_options());            (4) (since 0.171.0)
```

(1) Writes a value of type T into a byte container in the UBJSON data format, using the specified (or defaulted) [options](ubjson_options.md).
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md).  
Type `ByteContainer` must be back insertable and have member type `value_type` with size exactly 8 bits (since 0.152.0.)
Any of the values types `int8_t`, `uint8_t`, `char`, `unsigned char` and `std::byte` (since C++17) are allowed.

(2) Writes a value of type T into a binary stream in the UBJSON data format, using the specified (or defaulted) [options](ubjson_options.md). 
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md).

Functions (3)-(4) are identical to (1)-(2) except an [allocator_set](../allocator_set.md) is passed as an additional argument.

### See also

[decode_ubjson](decode_ubjson) decodes a [Binary JSON](http://ubjsonspec.org/) data format to a json value.

