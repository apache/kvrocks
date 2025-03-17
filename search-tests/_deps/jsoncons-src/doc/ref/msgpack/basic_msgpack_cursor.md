### jsoncons::msgpack::basic_msgpack_cursor

```cpp
#include <jsoncons_ext/msgpack/msgpack_cursor.hpp>

template<
    class Source=jsoncons::binary_stream_source,
    class Allocator=std::allocator<char>>
class basic_msgpack_cursor;
```

A pull parser for reporting MessagePack parse events. A typical application will 
repeatedly process the `current()` event and call the `next()`
function to advance to the next event, until `done()` returns `true`.
In addition, when positioned on a `begin_object` event, 
the `read_to` function can pull a complete object representing
the events from `begin_object` to `end_object`, 
and when positioned on a `begin_array` event, a complete array
representing the events from `begin_array` ro `end_array`.

`basic_msgpack_cursor` is noncopyable and nonmoveable.

Aliases for common sources are provided:

Type                |Definition
--------------------|------------------------------
msgpack_stream_cursor  |basic_msgpack_cursor<jsoncons::binary_stream_source>
msgpack_bytes_cursor   |basic_msgpack_cursor<jsoncons::bytes_source>

### Implemented interfaces

[staj_cursor](staj_cursor.md)

#### Constructors

    template <typename Sourceable>
    basic_msgpack_cursor(Sourceable&& source,
                         const msgpack_decode_options& options = msgpack_decode_options(),
                         const Allocator& alloc = Allocator()); (1)

    template <typename Sourceable>
    basic_msgpack_cursor(Sourceable&& source,
                         std::error_code& ec); (2)
    template <typename Sourceable>
    basic_msgpack_cursor(Sourceable&& source,
                         const msgpack_decode_options& options,
                         std::error_code& ec); (3)

    template <typename Sourceable>
    basic_msgpack_cursor(std::allocator_arg_t, const Allocator& alloc, 
                         Sourceable&& source,
                         const msgpack_decode_options& options,
                         std::error_code& ec); (4)

Constructor (1) reads from a buffer or stream source and throws a 
[ser_error](ser_error.md) if a parsing error is encountered while processing the initial event.

Constructors (2)-(4) read from a buffer or stream source and set `ec`
if a parsing error is encountered while processing the initial event.

Note: It is the programmer's responsibility to ensure that `basic_msgpack_cursor` does not outlive any source passed in the constuctor, 
as `basic_msgpack_cursor` holds a pointer to but does not own this object.

#### Parameters

`source` - a value from which a `source_type` is constructible. 

#### Member functions

    bool done() const override;
Checks if there are no more events.

    const staj_event& current() const override;
Returns the current [staj_event](basic_staj_event.md).

    void read_to(json_visitor& visitor) override
Feeds the current and succeeding [staj events](basic_staj_event.md) through the provided
[visitor](basic_json_visitor.md), until the visitor indicates
to stop. If a parsing error is encountered, throws a [ser_error](ser_error.md).

    void read_to(json_visitor& visitor, std::error_code& ec) override
Feeds the current and succeeding [staj events](basic_staj_event.md) through the provided
[visitor](basic_json_visitor.md), until the visitor indicates
to stop. If a parsing error is encountered, sets `ec`.

    void next() override;
Advances to the next event. If a parsing error is encountered, throws a 
[ser_error](ser_error.md).

    void next(std::error_code& ec) override;
Advances to the next event. If a parsing error is encountered, sets `ec`.

    const ser_context& context() const override;
Returns the current [context](ser_context.md)

    void reset();
Reset cursor to read another value from the same source

    template <typename Sourceable>
    reset(Sourceable&& source)
Reset cursor to read new value from a new sources

#### Non-member functions

   template <typename Source,typename Allocator>
   staj_filter_view operator|(basic_msgpack_cursor<Source,Allocator>& cursor, 
                              std::function<bool(const staj_event&, const ser_context&)> pred);

### See also

[staj_event](../basic_staj_event.md)  

[staj_array_iterator](../staj_array_iterator.md)  

[staj_object_iterator](../staj_object_iterator.md)  

