### jsoncons::cbor::basic_cbor_encoder

```cpp
#include <jsoncons_ext/cbor/cbor_encoder.hpp>

template<
    class Sink>
> class basic_cbor_encoder final : public json_visitor
```

`basic_cbor_encoder` is noncopyable

![basic_cbor_encoder](./diagrams/basic_cbor_encoder.png)

Four specializations for common sink types are defined:

Type                       |Definition
---------------------------|------------------------------
cbor_stream_encoder            |basic_cbor_encoder<jsoncons::binary_stream_sink>
cbor_bytes_encoder     |basic_cbor_encoder<jsoncons::bytes_sink<std::vector<uint8_t>>>

#### Member types

Type                       |Definition
---------------------------|------------------------------
char_type                  |char
sink_type                  |Sink
string_view_type           |

#### Constructors

    explicit basic_cbor_encoder(Sink&& sink)
Constructs a new encoder that writes to the specified destination.

#### Destructor

    virtual ~basic_cbor_encoder() noexcept

#### Member functions

    void reset();
Reset encoder to write another value to the same sink

    void reset(Sink&& sink)
Reset encoder to write a new value to a new sink

    void begin_object_with_tag(uint64_t raw_tag);                          // (since 1.2.0)

    void begin_object_with_tag(std::size_t length, uint64_t raw_tag);      // (since 1.2.0)

    void begin_array_with_tag(uint64_t raw_tag);                           // (since 1.2.0)

    void begin_array_with_tag(std::size_t length, uint64_t raw_tag);       // (since 1.2.0)

    void null_value_with_tag(uint64_t raw_tag);                            // (since 1.2.0)

    void bool_value_with_tag(bool value, uint64_t raw_tag);                // (since 1.2.0)

    void string_value_with_tag(const string_view_type& value, 
        uint64_t raw_tag);                                                 // (since 1.2.0) 

    template <typename ByteStringLike>
    void byte_string_value_with_tag(const ByteStringLike& value, 
        uint64_t raw_tag);                                                 // (since 1.2.0) 

    void double_value_with_tag(double value, uint64_t raw_tag);            // (since 1.2.0) 
    
    void uint64_value_with_tag(uint64_t value, uint64_t raw_tag);          // (since 1.2.0) 

    void int64_value_with_tag(int64_t value, uint64_t raw_tag);            // (since 1.2.0) 


#### Inherited from [jsoncons::basic_json_visitor](../basic_json_visitor.md)

    void flush();                                                  (1)

    bool begin_object(semantic_tag tag=semantic_tag::none,
        const ser_context& context=ser_context());                 (2)

    bool begin_object(std::size_t length, 
        semantic_tag tag=semantic_tag::none, 
        const ser_context& context = ser_context());               (3)

    bool end_object(const ser_context& context = ser_context());   (4)

    bool begin_array(semantic_tag tag=semantic_tag::none,
        const ser_context& context=ser_context());                 (5)

    bool begin_array(std::size_t length, 
        semantic_tag tag=semantic_tag::none,
        const ser_context& context=ser_context());                 (6)

    bool end_array(const ser_context& context=ser_context());      (7)

    bool key(const string_view_type& name, 
        const ser_context& context=ser_context());                 (8)

    bool null_value(semantic_tag tag = semantic_tag::none,
        const ser_context& context=ser_context());                 (9) 

    bool bool_value(bool value, 
        semantic_tag tag = semantic_tag::none,
        const ser_context& context=ser_context());                 (10) 

    bool string_value(const string_view_type& value, 
        semantic_tag tag = semantic_tag::none, 
        const ser_context& context=ser_context());                 (11) 

    bool byte_string_value(const byte_string_view& source, 
        semantic_tag tag=semantic_tag::none, 
        const ser_context& context=ser_context());                 (12) (until 0.152.0)

    template <typename ByteStringLike>
    bool byte_string_value(const ByteStringLike & souce, 
        semantic_tag tag=semantic_tag::none, 
        const ser_context& context=ser_context());                 (12) (since 0.152.0)

    template <typename ByteStringLike>
    bool byte_string_value(const ByteStringLike & souce, 
        uint64_t ext_tag, 
        const ser_context& context=ser_context());                 (13) (since 0.152.0)

    bool uint64_value(uint64_t value, 
        semantic_tag tag = semantic_tag::none, 
        const ser_context& context=ser_context());                 (14)

    bool int64_value(int64_t value, 
       semantic_tag tag = semantic_tag::none, 
       const ser_context& context=ser_context());                  (15)

    bool half_value(uint16_t value, 
      semantic_tag tag = semantic_tag::none, 
      const ser_context& context=ser_context());                   (16)

    bool double_value(double value, 
        semantic_tag tag = semantic_tag::none, 
        const ser_context& context=ser_context());                 (17)

    bool begin_object(semantic_tag tag,
        const ser_context& context,
        std::error_code& ec);                                      (18)

    bool begin_object(std::size_t length, 
        semantic_tag tag, 
        const ser_context& context,
        std::error_code& ec);                                      (19)

    bool end_object(const ser_context& context, 
        std::error_code& ec);                                      (20)

    bool begin_array(semantic_tag tag, 
       const ser_context& context, 
       std::error_code& ec);                                       (21)

    bool begin_array(std::size_t length, 
       semantic_tag tag, 
       const ser_context& context, 
       std::error_code& ec);                                       (22)

    bool end_array(const ser_context& context, 
     std::error_code& ec);                                         (23)

    bool key(const string_view_type& name, 
        const ser_context& context, 
        std::error_code& ec);                                      (24)

    bool null_value(semantic_tag tag,
        const ser_context& context,
        std::error_code& ec);                                      (25) 

    bool bool_value(bool value, 
        semantic_tag tag,
        const ser_context& context,
        std::error_code& ec);                                      (26) 

    bool string_value(const string_view_type& value, 
        semantic_tag tag, 
        const ser_context& context,
        std::error_code& ec);                                      (27) 

    bool byte_string_value(const byte_string_view& source, 
        semantic_tag tag, 
        const ser_context& context,
        std::error_code& ec);                                      (28) (until 0.152.0)

    template <typename Source>   
    bool byte_string_value(const Source& source, 
        semantic_tag tag, 
        const ser_context& context,
        std::error_code& ec);                                      (28) (since 0.152.0)

    template <typename Source>   
    bool byte_string_value(const Source& source, 
        uint64_t ext_tag, 
        const ser_context& context,
        std::error_code& ec);                                      (29) (since 0.152.0)

    bool uint64_value(uint64_t value, 
        semantic_tag tag, 
        const ser_context& context,
        std::error_code& ec);                                      (30)

    bool int64_value(int64_t value, 
       semantic_tag tag, 
       const ser_context& context,
       std::error_code& ec);                                       (31)

    bool half_value(uint16_t value, 
      semantic_tag tag, 
      const ser_context& context,
      std::error_code& ec);                                        (32)

    bool double_value(double value, 
        semantic_tag tag, 
        const ser_context& context,
        std::error_code& ec);                                      (33)

    template <typename T>
    bool typed_array(const span<T>& data, 
       semantic_tag tag=semantic_tag::none,
       const ser_context& context=ser_context());                  (34)

    bool typed_array(half_arg_t, const span<const uint16_t>& s,
        semantic_tag tag = semantic_tag::none,
        const ser_context& context = ser_context());               (35)

    bool begin_multi_dim(const span<const size_t>& shape,
        semantic_tag tag,
        const ser_context& context);                               (36) 

    bool end_multi_dim(const ser_context& context=ser_context());  (37) 

    template <typename T>
    bool typed_array(const span<T>& data, 
        semantic_tag tag,
        const ser_context& context,
        std::error_code& ec);                                      (38)

    bool typed_array(half_arg_t, const span<const uint16_t>& s,
        semantic_tag tag,
        const ser_context& context,
        std::error_code& ec);                                      (39)

    bool begin_multi_dim(const span<const size_t>& shape,
        semantic_tag tag,
        const ser_context& context, 
        std::error_code& ec);                                      (40)

    bool end_multi_dim(const ser_context& context,
         std::error_code& ec);                                     (41) 

(1) Flushes whatever is buffered to the destination.

(2) Indicates the begining of an object of indefinite length.
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(3) Indicates the begining of an object of known length. 
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(4) Indicates the end of an object.
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(5) Indicates the beginning of an indefinite length array. 
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(6) Indicates the beginning of an array of known length. 
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(7) Indicates the end of an array.
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(8) Writes the name part of an object name-value pair.
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(9) Writes a null value. 
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(10) Writes a boolean value.
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(11) Writes a text string value.
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(12) Writes a byte string value `source` with a generic tag.
Type `Source` must be a container that has member functions `data()` and `size()`, 
and member type `value_type` with size exactly 8 bits (since 0.152.0.)
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(13) Writes a byte string value `source` with a format specific tag, `ext_tag`.
Type `Source` must be a container that has member functions `data()` and `size()`, 
and member type `value_type` with size exactly 8 bits (since 0.152.0.)
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(14) Writes a non-negative integer value.
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(15) Writes a signed integer value.
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(16) Writes a half precision floating point value.
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(17) Writes a double precision floating point value.
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(18)-(33) Same as (2)-(17), except sets `ec` and returns `false` on parse errors.

### Examples

[Encode to CBOR buffer](#eg1)  
[Encode to CBOR stream](#eg2)  
[Encode with raw CBOR tags (since 1.2.0)](#eg3)  
[Encode Typed Array tags - array of half precision floating-point](#eg4)  
[Encode Typed Array tags - multi-dimensional column major tag](#eg5)  

 <div id="eg1"/>

#### Encode to CBOR buffer

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <iomanip>

int main()
{
    std::vector<uint8_t> buffer;
    cbor::cbor_bytes_encoder encoder(buffer);

    encoder.begin_array(); // Indefinite length array
    encoder.string_value("cat");
    std::vector<uint8_t> purr = {'p','u','r','r'};
    encoder.byte_string_value(purr);
    std::vector<uint8_t> hiss = {'h','i','s','s'};
    encoder.byte_string_value(hiss, semantic_tag::base64); // suggested conversion to base64
    encoder.int64_value(1431027667, semantic_tag::epoch_second);
    encoder.end_array();
    encoder.flush();

    std::cout << byte_string_view(buffer) << "\n\n";

/* 
    9f -- Start indefinte length array
      63 -- String value of length 3
        636174 -- "cat"
      44 -- Byte string value of length 4
        70757272 -- 'p''u''r''r'
      d6 - Expected conversion to base64
      44
        68697373 -- 'h''i''s''s'
      c1 -- Tag value 1 (seconds relative to 1970-01-01T00:00Z in UTC time)
        1a -- 32 bit unsigned integer
          554bbfd3 -- 1431027667
      ff -- "break" 
*/ 
}
```
Output:
```
9f,63,63,61,74,44,70,75,72,72,d6,44,68,69,73,73,c1,1a,55,4b,bf,d3,ff
```

 <div id="eg2"/>

#### Encode to CBOR stream

```cpp

#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <iomanip>

int main()
{
    std::ostringstream os;
    cbor::cbor_stream_encoder encoder(os);

    encoder.begin_array(3); // array of length 3
    encoder.string_value("-18446744073709551617", semantic_tag::bigint);
    encoder.string_value("184467440737095516.16", semantic_tag::bigdec);
    encoder.int64_value(1431027667, semantic_tag::epoch_second);
    encoder.end_array();
    encoder.flush();

    std::cout << byte_string_view(os.str()) << "\n\n";

/*
    83 -- array of length 3
      c3 -- Tag 3 (negative bignum)
      49 -- Byte string value of length 9
        010000000000000000 -- Bytes content
      c4 -- Tag 4 (decimal fraction)
        82 -- Array of length 2
          21 -- -2 (exponent)
          c2 Tag 2 (positive bignum)
          49 -- Byte string value of length 9
            010000000000000000
      c1 -- Tag 1 (seconds relative to 1970-01-01T00:00Z in UTC time)
        1a -- 32 bit unsigned integer
          554bbfd3 -- 1431027667
*/
}
```
Output:
```
83,c3,49,01,00,00,00,00,00,00,00,00,c4,82,21,c2,49,01,00,00,00,00,00,00,00,00,c1,1a,55,4b,bf,d3
```

 <div id="eg3"/>

#### Encode with raw CBOR tags (since 1.2.0)

```cpp
#include <cassert>
#include <iostream>
#include <jsoncons_ext/cbor/cbor.hpp>

namespace cbor = jsoncons::cbor;

int main()
{
    std::vector<uint8_t> data;
    cbor::cbor_bytes_encoder encoder(data);
    encoder.begin_array(1);
    encoder.uint64_value_with_tag(10, 0xC1);
    encoder.end_array();
    encoder.flush();

    cbor::cbor_bytes_cursor cursor(data);
    assert(cursor.current().event_type() == jsoncons::staj_event_type::begin_array);
    cursor.next();
    assert(cursor.raw_tag() == 0xC1);
    assert(cursor.current().get<uint64_t>() == 10);
}
```

 <div id="eg4"/>

#### Encode Typed Array tags - array of half precision floating-point

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <iomanip>

int main()
{
    std::vector<uint8_t> buffer;

    auto options = cbor::cbor_options{}
        .use_typed_arrays(true);
    cbor::cbor_bytes_encoder encoder(buffer, options);

    std::vector<uint16_t> values = {0x3bff,0x3c00,0x3c01,0x3555};
    encoder.typed_array(half_arg, values);

    // buffer contains a half precision floating-point, native endian, Typed Array 
    std::cout << "(1)\n" << byte_string_view(buffer) << "\n\n";

    auto j = cbor::decode_cbor<json>(buffer);

    std::cout << "(2)\n";
    for (auto item : j.array_range())
    {
        std::cout << std::boolalpha << item.is_half() 
                  << " " << std::hex << (int)item.as<uint16_t>() 
                  << " " << std::defaultfloat << item.as<double>() << "\n";
    }
    std::cout << "\n";

    std::cout << "(3)\n" << pretty_print(j) << "\n\n";
}
```
Output
```
(1)
d8 54 48 ff 3b 00 3c 01 3c 55 35

(2)
true 3bff 0.999512
true 3c00 1
true 3c01 1.00098
true 3555 0.333252

(3)
[
    0.99951171875,
    1.0,
    1.0009765625,
    0.333251953125
]
```

 <div id="eg5"/>

#### Encode Typed Array tags - multi-dimensional column major tag 

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

int main()
{
    std::vector<uint8_t> v;

    cbor::cbor_bytes_encoder encoder(v);
    std::vector<std::size_t> shape = { 2,3 };
    encoder.begin_multi_dim(shape, semantic_tag::multi_dim_column_major);
    encoder.begin_array(6);
    encoder.uint64_value(2);
    encoder.uint64_value(4);
    encoder.uint64_value(8);
    encoder.uint64_value(4);
    encoder.uint64_value(16);
    encoder.uint64_value(256);
    encoder.end_array();
    encoder.end_multi_dim();

    std::cout << "(1)\n" << byte_string_view(v) << "\n\n";

    auto j = cbor::decode_cbor<json>(v);
    std::cout << "(2) " << j.tag() << "\n";
    std::cout << pretty_print(j) << "\n\n";
}
```
Output:
```
(1)
d9 04 10 82 82 02 03 86 02 04 08 04 10 19 01 00

(2) multi-dim-column-major
[
    [2, 3],
    [2, 4, 8, 4, 16, 256]
]
```

### See also

[byte_string_view](../byte_string_view.md)
