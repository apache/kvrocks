### jsoncons::basic_json_filter

```cpp
#include <jsoncons/json_filter.hpp>

template <
    class CharT
> class basic_json_filter
```

Defines an interface for filtering JSON events. 

`basic_json_filter` is noncopyable and nonmoveable.

![basic_json_filter](./diagrams/basic_json_filter.png)

Aliases for common character types are provided:

Type                |Definition
--------------------|------------------------------
json_filter    |`basic_json_filter<char>`
wjson_filter   |`basic_json_filter<wchar_t>`

#### Member types

Member type                         |Definition
------------------------------------|------------------------------
`char_type`|CharT
`string_view_type`|A non-owning view of a string, holds a pointer to character data and length. Supports conversion to and from strings. Will be typedefed to the C++ 17 [std::string view](http://en.cppreference.com/w/cpp/string/basic_string_view) if C++ 17 is detected or if `JSONCONS_HAS_STD_STRING_VIEW` is defined, otherwise proxied.  

#### Constructors

    basic_json_filter(basic_json_visitor<char_type>& visitor)
All JSON events that pass through the `basic_json_filter` go to the specified [visitor](basic_json_visitor.md).
You must ensure that the `visitor` exists as long as does `basic_json_filter`, as `basic_json_filter` holds a pointer to but does not own this object.

#### Accessors

    basic_json_visitor<char_type>& destination()
Returns a reference to the JSON visitor that sends json events to the destination handler. 

### Inherited from [jsoncons::basic_json_visitor](basic_json_visitor.md)

#### Public member functions

    void flush(); (1)

    bool begin_object(semantic_tag tag=semantic_tag::none,
                      const ser_context& context=ser_context()); (2)

    bool begin_object(std::size_t length, 
                      semantic_tag tag=semantic_tag::none, 
                      const ser_context& context = ser_context()); (3)

    bool end_object(const ser_context& context = ser_context()); (4)

    bool begin_array(semantic_tag tag=semantic_tag::none,
                     const ser_context& context=ser_context()); (5)

    bool begin_array(std::size_t length, 
                     semantic_tag tag=semantic_tag::none,
                     const ser_context& context=ser_context()); (6)

    bool end_array(const ser_context& context=ser_context()); (7)

    bool key(const string_view_type& name, 
              const ser_context& context=ser_context()); (8)

    bool null_value(semantic_tag tag = semantic_tag::none,
                    const ser_context& context=ser_context()); (9) 

    bool bool_value(bool value, 
                    semantic_tag tag = semantic_tag::none,
                    const ser_context& context=ser_context()); (10) 

    bool string_value(const string_view_type& value, 
                      semantic_tag tag = semantic_tag::none, 
                      const ser_context& context=ser_context()); (11) 

    bool byte_string_value(const byte_string_view& source, 
                           semantic_tag tag=semantic_tag::none, 
                           const ser_context& context=ser_context()); (12) (until 0.152.0)

    template <typename ByteStringLike>
    bool byte_string_value(const ByteStringLike& souce, 
                           semantic_tag tag=semantic_tag::none, 
                           const ser_context& context=ser_context()); (12) (since 0.152.0)

    template <typename ByteStringLike>
    bool byte_string_value(const ByteStringLike& souce, 
                           uint64_t ext_tag, 
                           const ser_context& context=ser_context()); (13) (since 0.152.0)

    bool uint64_value(uint64_t value, 
                      semantic_tag tag = semantic_tag::none, 
                      const ser_context& context=ser_context()); (14)

    bool int64_value(int64_t value, 
                     semantic_tag tag = semantic_tag::none, 
                     const ser_context& context=ser_context()); (15)

    bool half_value(uint16_t value, 
                    semantic_tag tag = semantic_tag::none, 
                    const ser_context& context=ser_context()); (16)

    bool double_value(double value, 
                      semantic_tag tag = semantic_tag::none, 
                      const ser_context& context=ser_context()); (17)

    bool begin_object(semantic_tag tag,
                      const ser_context& context,
                      std::error_code& ec); (18)

    bool begin_object(std::size_t length, 
                      semantic_tag tag, 
                      const ser_context& context,
                      std::error_code& ec); (19)

    bool end_object(const ser_context& context, 
                    std::error_code& ec); (20)

    bool begin_array(semantic_tag tag, 
                     const ser_context& context, 
                     std::error_code& ec); (21)

    bool begin_array(std::size_t length, 
                     semantic_tag tag, 
                     const ser_context& context, 
                     std::error_code& ec); (22)

    bool end_array(const ser_context& context, 
                   std::error_code& ec); (23)

    bool key(const string_view_type& name, 
              const ser_context& context, 
              std::error_code& ec); (24)

    bool null_value(semantic_tag tag,
                    const ser_context& context,
                    std::error_code& ec); (25) 

    bool bool_value(bool value, 
                    semantic_tag tag,
                    const ser_context& context,
                    std::error_code& ec); (26) 

    bool string_value(const string_view_type& value, 
                      semantic_tag tag, 
                      const ser_context& context,
                      std::error_code& ec); (27) 

    bool byte_string_value(const byte_string_view& source, 
                           semantic_tag tag, 
                           const ser_context& context,
                           std::error_code& ec); (28) (until 0.152.0)

    template <typename ByteStringLike>   
    bool byte_string_value(const ByteStringLike& source, 
                           semantic_tag tag, 
                           const ser_context& context,
                           std::error_code& ec); (28) (since 0.152.0)

    template <typename ByteStringLike>   
    bool byte_string_value(const ByteStringLike& source, 
                           uint64_t ext_tag, 
                           const ser_context& context,
                           std::error_code& ec); (29) (since 0.152.0)

    bool uint64_value(uint64_t value, 
                      semantic_tag tag, 
                      const ser_context& context,
                      std::error_code& ec); (30)

    bool int64_value(int64_t value, 
                     semantic_tag tag, 
                     const ser_context& context,
                     std::error_code& ec); (31)

    bool half_value(uint16_t value, 
                    semantic_tag tag, 
                    const ser_context& context,
                    std::error_code& ec); (32)

    bool double_value(double value, 
                      semantic_tag tag, 
                      const ser_context& context,
                      std::error_code& ec); (33)

    template <typename T>
    bool typed_array(const span<T>& data, 
                     semantic_tag tag=semantic_tag::none,
                     const ser_context& context=ser_context()); (34)

    bool typed_array(half_arg_t, const span<const uint16_t>& s,
        semantic_tag tag = semantic_tag::none,
        const ser_context& context = ser_context()); (35)

    bool begin_multi_dim(const span<const size_t>& shape,
                         semantic_tag tag,
                         const ser_context& context); (36) 

    bool end_multi_dim(const ser_context& context=ser_context()); (37) 

    template <typename T>
    bool typed_array(const span<T>& data, 
                     semantic_tag tag,
                     const ser_context& context,
                     std::error_code& ec); (38)

    bool typed_array(half_arg_t, const span<const uint16_t>& s,
                     semantic_tag tag,
                     const ser_context& context,
                     std::error_code& ec); (39)

    bool begin_multi_dim(const span<const size_t>& shape,
                         semantic_tag tag,
                         const ser_context& context, 
                         std::error_code& ec); (40)

    bool end_multi_dim(const ser_context& context,
                       std::error_code& ec); (41) 

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
Type `ByteStringLike` must be a container that has member functions `data()` and `size()`, 
and member type `value_type` with size exactly 8 bits (since 0.152.0.)
Returns `true` if the consumer wishes to receive more events, `false` otherwise.
Throws a [ser_error](ser_error.md) on parse errors. 

(13) Writes a byte string value `source` with a format specific tag, `ext_tag`.
Type `ByteStringLike` must be a container that has member functions `data()` and `size()`, 
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

#### Private virtual functions


    virtual void visit_flush() = 0; (1)

    virtual bool visit_begin_object(semantic_tag tag, 
                                    const ser_context& context, 
                                    std::error_code& ec) = 0; (2)

    virtual bool visit_begin_object(std::size_t length, 
                                    semantic_tag tag, 
                                    const ser_context& context, 
                                    std::error_code& ec); (3)

    virtual bool visit_end_object(const ser_context& context, 
                                  std::error_code& ec) = 0; (4)

    virtual bool visit_begin_array(semantic_tag tag, 
                                   const ser_context& context, 
                                   std::error_code& ec) = 0; (5)

    virtual bool visit_begin_array(std::size_t length, 
                                   semantic_tag tag, 
                                   const ser_context& context, 
                                   std::error_code& ec); (6)

    virtual bool visit_end_array(const ser_context& context, 
                                 std::error_code& ec) = 0; (7)

    virtual bool visit_key(const string_view_type& name, 
                           const ser_context& context, 
                           std::error_code&) = 0; (8)

    virtual bool visit_null(semantic_tag tag, 
                            const ser_context& context, 
                            std::error_code& ec) = 0; (9)

    virtual bool visit_bool(bool value, 
                            semantic_tag tag, 
                            const ser_context& context, 
                            std::error_code&) = 0; (10)

    virtual bool visit_string(const string_view_type& value, 
                              semantic_tag tag, 
                              const ser_context& context, 
                              std::error_code& ec) = 0; (11)

    virtual bool visit_byte_string(const byte_string_view& value, 
                                   semantic_tag tag, 
                                   const ser_context& context,
                                   std::error_code& ec) = 0; (12)

    virtual bool visit_byte_string(const byte_string_view& value, 
                                   uint64_t ext_tag, 
                                   const ser_context& context,
                                   std::error_code& ec); (13)

    virtual bool visit_uint64(uint64_t value, 
                              semantic_tag tag, 
                              const ser_context& context,
                              std::error_code& ec) = 0; (14)

    virtual bool visit_int64(int64_t value, 
                             semantic_tag tag,
                             const ser_context& context,
                             std::error_code& ec) = 0; (15)

    virtual bool visit_half(uint16_t value, 
                            semantic_tag tag,
                            const ser_context& context,
                            std::error_code& ec); (16)

    virtual bool visit_double(double value, 
                              semantic_tag tag,
                              const ser_context& context,
                              std::error_code& ec) = 0; (17)

    virtual bool visit_typed_array(const span<const uint8_t>& data, 
                                   semantic_tag tag,
                                   const ser_context& context, 
                                   std::error_code& ec); (18)

    virtual bool visit_typed_array(const span<const uint16_t>& data, 
                                   semantic_tag tag,
                                   const ser_context& context, 
                                   std::error_code& ec); (19)

    virtual bool visit_typed_array(const span<const uint32_t>& data, 
                                   semantic_tag tag,
                                   const ser_context& context, 
                                   std::error_code& ec); (20)

    virtual bool visit_typed_array(const span<const uint64_t>& data, 
                                   semantic_tag tag,
                                   const ser_context& context, 
                                   std::error_code& ec); (21)

    virtual bool visit_typed_array(const span<const int8_t>& data, 
                                   semantic_tag tag,
                                   const ser_context& context, 
                                   std::error_code& ec); (22)

    virtual bool visit_typed_array(const span<const int16_t>& data, 
                                   semantic_tag tag,
                                   const ser_context& context, 
                                   std::error_code& ec); (23)

    virtual bool visit_typed_array(const span<const int32_t>& data, 
                                   semantic_tag tag,
                                   const ser_context& context, 
                                   std::error_code& ec); (24)

    virtual bool visit_typed_array(const span<const int64_t>& data, 
                                   semantic_tag tag,
                                   const ser_context& context, 
                                   std::error_code& ec); (25)

    virtual bool visit_typed_array(half_arg_t, 
                                   const span<const uint16_t>& data, 
                                   semantic_tag tag,
                                   const ser_context& context, 
                                   std::error_code& ec); (26)

    virtual bool visit_typed_array(const span<const float>& data, 
                                   semantic_tag tag,
                                   const ser_context& context, 
                                   std::error_code& ec); (27)

    virtual bool visit_typed_array(const span<const double>& data, 
                                   semantic_tag tag,
                                   const ser_context& context, 
                                   std::error_code& ec); (28)

    virtual bool visit_typed_array(const span<const float128_type>& data, 
                                   semantic_tag tag,
                                   const ser_context& context, 
                                   std::error_code& ec); (29)

    virtual bool visit_begin_multi_dim(const span<const size_t>& shape,
                                       semantic_tag tag,
                                       const ser_context& context, 
                                       std::error_code& ec); (30)

    virtual bool visit_end_multi_dim(const ser_context& context,
                                     std::error_code& ec); (31)

(1) Allows producers of json events to flush any buffered data.

(2) Handles the beginning of an object of indefinite length.
Returns `true` if the producer should generate more events, `false` otherwise.
Sets `ec` and returns `false` on parse errors. 

(3) Handles the beginning of an object of known length.
Returns `true` if the producer should generate more events, `false` otherwise.
Sets `ec` and returns `false` on parse errors. 

(4) Handles the end of an object.
Returns `true` if the producer should generate more events, `false` otherwise.
Sets `ec` and returns `false` on parse errors. 

(5) Handles the beginning of an array of indefinite length.
Returns `true` if the producer should generate more events, `false` otherwise.
Sets `ec` and returns `false` on parse errors. 

(6) Handles the beginning of an array of known length.
Returns `true` if the producer should generate more events, `false` otherwise.
Sets `ec` and returns `false` on parse errors. 

(7) Handles the end of an array.
Returns `true` if the producer should generate more events, `false` otherwise.
Sets `ec` and returns `false` on parse errors. 

(8) Handles the name part of an object name-value pair.
Returns `true` if the producer should generate more events, `false` otherwise.
Sets `ec` and returns `false` on parse errors. 

(9) Handles a null value.
Returns `true` if the producer should generate more events, `false` otherwise.
Sets `ec` and returns `false` on parse errors. 

(10) Handles a boolean value. 
Returns `true` if the producer should generate more events, `false` otherwise.
Sets `ec` and returns `false` on parse errors. 

(11) Handles a string value.
Returns `true` if the producer should generate more events, `false` otherwise.
Sets `ec` and returns `false` on parse errors. 

(12) Handles a byte string value associated with a generic tag.
Returns `true` if the producer should generate more events, `false` otherwise.
Sets `ec` and returns `false` on parse errors. 

(13) Handles a byte string value associated with a format specific tag.
Returns `true` if the producer should generate more events, `false` otherwise.
Sets `ec` and returns `false` on parse errors. 

(14) Handles a non-negative integer value.
Returns `true` if the producer should generate more events, `false` otherwise.
Sets `ec` and returns `false` on parse errors. 

(15) Handles a signed integer value.
Returns `true` if the producer should generate more events, `false` otherwise.
Sets `ec` and returns `false` on parse errors. 

(16) Handles a half precision floating point value. 
Returns `true` if the producer should generate more events, `false` otherwise.
Sets `ec` and returns `false` on parse errors. 

(17) Handles a double precision floating point value. 
Returns `true` if the producer should generate more events, `false` otherwise.
Sets `ec` and returns `false` on parse errors. 

#### Parameters

`tag` - a jsoncons semantic tag
`context` - parse context information including line and column number
`ec` - a parse error code

#### Exceptions

The overloads that do not take a `std::error_code&` parameter throw a
[ser_error](ser_error.md) on parse errors, constructed with the error code as the error code argument
and line and column from the `context`. 

The overloads that take a `std::error_code&` parameter set it to the error code and return `false` on parse errors.

### Examples

#### Rename object member names with the built in filter [rename_object_key_filter](rename_object_key_filter.md)

```cpp
#include <sstream>
#include <jsoncons/json.hpp>
#include <jsoncons/json_filter.hpp>

using namespace jsoncons;

int main()
{
    std::string s = R"({"first":1,"second":2,"fourth":3,"fifth":4})";    

    json_stream_encoder encoder(std::cout);

    // Filters can be chained
    rename_object_key_filter filter2("fifth", "fourth", encoder);
    rename_object_key_filter filter1("fourth", "third", filter2);

    // A filter can be passed to any function that takes
    // a json_visitor ...
    std::cout << "(1) ";
    std::istringstream is(s);
    //json_reader reader(is, filter1);        // (until 0.164.0)
    json_stream_reader reader(is, filter1);   // (since 0.164.0)
    reader.read();
    std::cout << '\n';

    // or a json_visitor    
    std::cout << "(2) ";
    ojson j = ojson::parse(s);
    j.dump(filter1);
    std::cout << '\n';
}
```
Output:
```json
(1) {"first":1,"second":2,"third":3,"fourth":4}
(2) {"first":1,"second":2,"third":3,"fourth":4}
```

#### Fix up names in an address book JSON file

Input JSON file `address-book.json`:

```json
{
    "address-book" : 
    [
        {
            "name":"Jane Roe",
            "email":"jane.roe@example.com"
        },
        {
             "name":"John",
             "email" : "john.doe@example.com"
         }
    ]
}
```

Suppose you want to break the name into a first name and last name, and report a warning when `name` does not contain a space or tab separated part. 

You can achieve the desired result by subclassing the [basic_json_filter](basic_json_filter.md) class, overriding the default methods for receiving name and string value events, and passing modified events on to the parent [json_visitor](basic_json_visitor.md) (which in this example will forward them to a [basic_json_encoder](basic_json_encoder.md).) 
```cpp
#include <jsoncons/json_encoder.hpp>
#include <jsoncons/json_filter.hpp>
#include <jsoncons/json_reader.hpp>

using namespace jsoncons;


class name_fixup_filter : public json_filter
{
    std::string member_name_;

public:
    name_fixup_filter(json_visitor& visitor)
        : json_filter(visitor)
    {
    }

private:
    bool visit_key(const string_view_type& name, 
                   const ser_context& context,
                   std::error_code&) override
    {
        member_name_ = name;
        if (member_name_ != "name")
        {
            this->destination().key(name, context);
        }
        return true;
    }

    bool visit_string(const string_view_type& s, 
                         const ser_context& context,
                         std::error_code&) override
    {
        if (member_name_ == "name")
        {
            std::size_t end_first = val.find_first_of(" \t");
            std::size_t start_last = val.find_first_not_of(" \t", end_first);
            this->destination().key("first-name", context);
            string_view_type first = val.substr(0, end_first);
            this->destination().value(first, context);
            if (start_last != string_view_type::npos)
            {
                this->destination().key("last-name", context);
                string_view_type last = val.substr(start_last);
                this->destination().value(last, context);
            }
            else
            {
                std::cerr << "Incomplete name \"" << s
                   << "\" at line " << context.line()
                   << " and column " << context.column() << '\n';
            }
        }
        else
        {
            this->destination().value(s, context);
        }
        return true;
    }
};
```
Configure a [rename_object_key_filter](rename_object_key_filter.md) to emit json events to a [basic_json_encoder](basic_json_encoder.md). 
```cpp
std::ofstream os("output/new-address-book.json");
json_stream_encoder encoder(os);
name_fixup_filter filter(encoder);
```
Parse the input and send the json events through the filter ...
```cpp
std::cout << "(1) ";
std::ifstream is("input/address-book.json");
//json_reader reader(is, filter);          // (until 0.164.0)
json_stream_reader reader(is, filter);     // (since 0.164.0)
reader.read();
std:: << "\n";
```
or read into a json value and write to the filter
```cpp
std::cout << "(2) ";
json j;
is >> j;
j.dump(filter);
std:: << "\n";
```
Output:
```
(1) Incomplete name "John" at line 9 and column 26 
(2) Incomplete name "John" at line 0 and column 0
```
Note that when filtering `json` events written from a `json` value to an output visitor, contexual line and column information in the original file has been lost. 
```

The output JSON file `address-book-new.json` with name fixes is

```json
{
    "address-book":
    [
        {
            "first-name":"Jane",
            "last-name":"Roe",
            "email":"jane.roe@example.com"
        },
        {
            "first-name":"John",
            "email":"john.doe@example.com"
        }
    ]
}
```

### See also

[basic_json_visitor](basic_json_visitor.md)  

[byte_string_view](byte_string_view.md)  
