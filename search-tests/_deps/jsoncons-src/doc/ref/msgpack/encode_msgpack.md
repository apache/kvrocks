### jsoncons::msgpack::encode_msgpack

Encodes a C++ data structure into the [MessagePack](http://msgpack.org/index.html) data format.

```cpp
#include <jsoncons_ext/msgpack/msgpack.hpp>

template <typename T,typename ByteContainer>
void encode_msgpack(const T& jval, ByteContainer& cont,
    const msgpack_decode_options& options = msgpack_decode_options());         (1)

template <typename T>
void encode_msgpack(const T& jval, std::ostream& os,
    const msgpack_decode_options& options = msgpack_decode_options());         (2)

template <typename T,typename ByteContainer>
void encode_msgpack(const allocator_set<Allocator,TempAllocator>& alloc_set,
    const T& jval, ByteContainer& cont,
    const msgpack_decode_options& options = msgpack_decode_options());         (3) (since 0.171.0)

template <typename T>
void encode_msgpack(const allocator_set<Allocator,TempAllocator>& alloc_set,
    const T& jval, std::ostream& os,
    const msgpack_decode_options& options = msgpack_decode_options());         (4) (since 0.171.0)
```

(1) Writes a value of type T into a byte container in the MessagePack data format, using the specified (or defaulted) [options](msgpack_options.md). 
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md). 
Type `ByteContainer` must be back insertable and have member type `value_type` with size exactly 8 bits (since 0.152.0.)
Any of the values types `int8_t`, `uint8_t`, `char`, `unsigned char` and `std::byte` (since C++17) are allowed.

(2) Writes a value of type T into a binary stream in the MessagePack data format, using the specified (or defaulted) [options](msgpack_options.md). 
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md). 

Functions (3)-(4) are identical to (1)-(2) except an [allocator_set](../allocator_set.md) is passed as an additional argument.

### Examples

#### MessagePack example

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/msgpack/msgpack.hpp>

using namespace jsoncons;
using namespace jsoncons::msgpack;

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
    encode_msgpack(j1, cont);

    ojson j2 = decode_msgpack<ojson>(cont);

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

### See also

[decode_msgpack](decode_msgpack) decodes a [MessagePack](http://msgpack.org/index.html) data format to a json value.

