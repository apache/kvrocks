### jsoncons::msgpack::decode_msgpack

```cpp
#include <jsoncons_ext/msgpack/msgpack.hpp>

template <typename T>
T decode_msgpack(const std::vector<uint8_t>& source,
    const msgpack_decode_options& options = msgpack_decode_options());       (1) (until 0.152.0)

template <typename T,typename Source>
T decode_msgpack(const Source& source,
    const msgpack_decode_options& options = msgpack_decode_options());       (1) (since 0.152.0)

template <typename T>
T decode_msgpack(std::istream& is,
    const msgpack_decode_options& options = msgpack_decode_options());       (2)

template <typename T,typename InputIt>
T decode_msgpack(InputIt first, InputIt last,
    const msgpack_decode_options& options = msgpack_decode_options());       (3) (since 0.153.0)

template <typename T,typename Source,class Allocator,class TempAllocator>
T decode_msgpack(const allocator_set<Allocator,TempAllocator>& alloc_set,
    const Source& source,
    const msgpack_decode_options& options = msgpack_decode_options());       (4) (since 0.171.0)

template <typename T,class Allocator,class TempAllocator>
T decode_msgpack(const allocator_set<Allocator,TempAllocator>& alloc_set,
    std::istream& is,
    const msgpack_decode_options& options = msgpack_decode_options());       (5) (since 0.171.0)
```

Decodes a [MessagePack](http://msgpack.org/index.html) data format into a C++ data structure.

(1) Reads MessagePack data from a contiguous byte sequence provided by `source` into a type T, using the specified (or defaulted) [options](msgpack_options.md). 
Type `Source` must be a container that has member functions `data()` and `size()`, 
and member type `value_type` with size exactly 8 bits (since 0.152.0.)
Any of the values types `int8_t`, `uint8_t`, `char`, `unsigned char` and `std::byte` (since C++17) are allowed.
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md).

(2) Reads MessagePack data from a binary stream into a type T, using the specified (or defaulted) [options](msgpack_options.md). 
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md).

(3) Reads MessagePack data from the range [`first`,`last`) into a type T, using the specified (or defaulted) [options](msgpack_options.md). 
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md).

Functions (4)-(5) are identical to (1)-(2) except an [allocator_set](../allocator_set.md) is passed as an additional argument and
provides allocators for result data and temporary allocations.

#### Exceptions

Throws a [ser_error](../ser_error.md) if parsing fails, and a [conv_error](conv_error.md) if type conversion fails.

### See also

[encode_msgpack](encode_msgpack.md) encodes a json value to the [MessagePack](http://msgpack.org/index.html) data format.


