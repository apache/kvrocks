### jsoncons::csv::basic_csv_cursor

```cpp
#include <jsoncons_ext/csv/csv_cursor.hpp>

template<
    class CharT,
    class Source=jsoncons::stream_source<CharT>,
    class Allocator=std::allocator<char>> basic_csv_cursor;
```

A pull parser for reporting CSV parse events. A typical application will 
repeatedly process the `current()` event and call the `next()`
function to advance to the next event, until `done()` returns `true`.
In addition, when positioned on a `begin_object` event, 
the `read_to` function can pull a complete object representing
the events from `begin_object` to `end_object`, 
and when positioned on a `begin_array` event, a complete array
representing the events from `begin_array` ro `end_array`.

`basic_csv_cursor` is noncopyable and nonmoveable.

Aliases for common character types are provided:

Type                |Definition
--------------------|------------------------------
`csv_stream_cursor` (since 0.167.0)  |`basic_csv_cursor<char,csvcons::stream_source<char>>`
`csv_string_cursor` (since 0.167.0)  |`basic_csv_cursor<char,csvcons::string_source<char>>`
`wcsv_stream_cursor` (since 0.167.0) |`basic_csv_cursor<wchar_t,csvcons::stream_source<wchar_t>>`
`wcsv_string_cursor` (since 0.167.0) |`basic_csv_cursor<wchar_t,csvcons::string_source<wchar_t>>`
`csv_cursor` (until 0.167.0)         |`basic_csv_cursor<char>`
`wcsv_cursor` (until 0.167.0)        |`basic_csv_cursor<wchar_t>`

### Implemented interfaces

[basic_staj_cursor](../staj_cursor.md)

#### Constructors

    template <typename Sourceable>
    basic_csv_cursor(Sourceable&& source, 
                     const basic_csv_decode_options<CharT>& options = basic_csv_decode_options<CharT>(),
                     std::function<bool(csv_errc,const ser_context&)> err_handler = default_csv_parsing(),
                     const Allocator& alloc = Allocator()); (1)

    template <typename Sourceable>
    basic_csv_cursor(Sourceable&& source, 
                     std::error_code& ec); (2)

    template <typename Sourceable>
    basic_csv_cursor(Sourceable&& source, 
                     const basic_csv_decode_options<CharT>& options,
                     std::error_code& ec); (3)

    template <typename Sourceable>
    basic_csv_cursor(Sourceable&& source, 
                     const basic_csv_decode_options<CharT>& options,
                     std::function<bool(csv_errc,const ser_context&)> err_handler,
                     std::error_code& ec); (4)

    template <typename Sourceable>
    basic_csv_cursor(std::allocator_arg_t, const Allocator& alloc, 
                     Sourceable&& source, 
                     const basic_csv_decode_options<CharT>& options,
                     std::function<bool(csv_errc,const ser_context&)> err_handler,
                     std::error_code& ec); (5)

Constructors (1) reads from a character sequence or stream and throws a 
[ser_error](../ser_error.md) if a parsing error is encountered while processing the initial event.

Constructors (2)-(5) read from a character sequence or stream and set `ec`
if a parsing error is encountered while processing the initial event.

Note: It is the programmer's responsibility to ensure that `basic_csv_cursor` does not outlive the source  
passed in the constuctor, as `basic_csv_cursor` holds pointers to but does not own this resource.

#### Parameters

`source` - a value from which a `jsoncons::basic_string_view<char_type>` is constructible, 
or a value from which a `source_type` is constructible. In the case that a `jsoncons::basic_string_view<char_type>` is constructible
from `source`, `source` is dispatched immediately to the parser. Otherwise, the `csv_cursor` reads from a `source_type` in chunks. 

#### Member functions

    bool done() const override;
Checks if there are no more events.

    const basic_staj_event& current() const override;
Returns the current [basic_staj_event](../staj_event.md).

    void read_to(json_visitor& visitor) override
Feeds the current and succeeding [staj events](basic_staj_event.md) through the provided
[visitor](basic_json_visitor.md), until the visitor indicates
to stop. If a parsing error is encountered, throws a [ser_error](../ser_error.md).

    void read_to(basic_json_visitor<char_type>& visitor,
                std::error_code& ec) override
Feeds the current and succeeding [staj events](basic_staj_event.md) through the provided
[visitor](basic_json_visitor.md), until the visitor indicates
to stop. If a parsing error is encountered, sets `ec`.

    void next() override;
Advances to the next event. If a parsing error is encountered, throws a 
[ser_error](../ser_error.md).

    void next(std::error_code& ec) override;
Advances to the next event. If a parsing error is encountered, sets `ec`.

    const ser_context& context() const override;
Returns the current [context](../ser_context.md)

    void reset();
Reset cursor to read another value from the same source

    template <typename Sourceable>
    reset(Sourceable&& source)
Reset cursor to read new value from a new sources

#### Non-member functions

   template <typename CharT,typename Source,typename Allocator>
   basic_staj_filter_view<CharT> operator|(basic_csv_cursor<CharT,Source,Allocator>& cursor, 
                                    std::function<bool(const basic_staj_event<CharT>&, const ser_context&)> pred);

### Examples

### See also

[basic_staj_event](../basic_staj_event.md)  

[staj_array_iterator](../staj_array_iterator.md)  

[staj_object_iterator](../staj_object_iterator.md)  

