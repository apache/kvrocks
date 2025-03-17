### jsoncons::bson::basic_bson_cursor

```cpp
#include <jsoncons_ext/bson/bson_cursor.hpp>

template<
    class Source=jsoncons::binary_stream_source,
    class Allocator=std::allocator<char>>
class basic_bson_cursor;
```

A pull parser for reporting BSON parse events. A typical application will 
repeatedly process the `current()` event and call the `next()`
function to advance to the next event, until `done()` returns `true`.
In addition, when positioned on a `begin_object` event, 
the `read_to` function can pull a complete object representing
the events from `begin_object` to `end_object`, 
and when positioned on a `begin_array` event, a complete array
representing the events from `begin_array` ro `end_array`.

`basic_bson_cursor` is noncopyable and nonmoveable.

Aliases for common sources are provided:

Type                |Definition
--------------------|------------------------------
bson_stream_cursor  |basic_bson_cursor<jsoncons::binary_stream_source>
bson_bytes_cursor   |basic_bson_cursor<jsoncons::bytes_source>

### Implemented interfaces

[staj_cursor](staj_cursor.md)

#### Constructors

    template <typename Sourceable>
    basic_bson_cursor(Sourceable&& source,
                      const bson_decode_options& options = bson_decode_options(),
                      const Allocator& alloc = Allocator()); (1)

    template <typename Sourceable>
    basic_bson_cursor(Sourceable&& source, 
                      std::error_code& ec); (2)

    template <typename Sourceable>
    basic_bson_cursor(Sourceable&& source,
                      const bson_decode_options& options,
                      std::error_code& ec); (3)

    template <typename Sourceable>
    basic_bson_cursor(std::allocator_arg_t, const Allocator& alloc, 
                      Sourceable&& source,
                      const bson_decode_options& options,
                      std::error_code& ec); (4)

Constructors (1) reads from a buffer or stream source and throws a 
[ser_error](ser_error.md) if a parsing error is encountered while processing the initial event.

Constructors (2)-(4) read from a buffer or stream source and set `ec`
if a parsing error is encountered while processing the initial event.

Note: It is the programmer's responsibility to ensure that `basic_bson_cursor` does not outlive any source passed in the constuctor, 
as `basic_bson_cursor` holds a pointer to but does not own this object.

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
Reset cursor to read new value from a new source

#### Non-member functions

   template <typename Source,typename Allocator>
   staj_filter_view operator|(basic_bson_cursor<Source,Allocator>& cursor, 
                              std::function<bool(const staj_event&, const ser_context&)> pred);

### Examples

Input JSON file `book_catalog.json`:

```json
[ 
  { 
      "author" : "Haruki Murakami",
      "title" : "Hard-Boiled Wonderland and the End of the World",
      "isbn" : "0679743464",
      "publisher" : "Vintage",
      "date" : "1993-03-02",
      "price": 18.90
  },
  { 
      "author" : "Graham Greene",
      "title" : "The Comedians",
      "isbn" : "0099478374",
      "publisher" : "Vintage Classics",
      "date" : "2005-09-21",
      "price": 15.74
  }
]
```

#### Read BSON parse events

```cpp
#include <jsoncons_ext/bson/bson_cursor.hpp>
#include <string>
#include <fstream>

using namespace jsoncons;

int main()
{
    std::ifstream is("book_catalog.json");

    bson_cursor cursor(is);

    for (; !cursor.done(); cursor.next())
    {
        const auto& event = cursor.current();
        switch (event.event_type())
        {
            case staj_event_type::begin_array:
                std::cout << event.event_type() << "\n";
                break;
            case staj_event_type::end_array:
                std::cout << event.event_type() << "\n";
                break;
            case staj_event_type::begin_object:
                std::cout << event.event_type() << "\n";
                break;
            case staj_event_type::end_object:
                std::cout << event.event_type() << "\n";
                break;
            case staj_event_type::key:
                // Or std::string_view, if supported
                std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << "\n";
                break;
            case staj_event_type::string_value:
                // Or std::string_view, if supported
                std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << "\n";
                break;
            case staj_event_type::null_value:
                std::cout << event.event_type() << ": " << "\n";
                break;
            case staj_event_type::bool_value:
                std::cout << event.event_type() << ": " << std::boolalpha << event.get<bool>() << "\n";
                break;
            case staj_event_type::int64_value:
                std::cout << event.event_type() << ": " << event.get<int64_t>() << "\n";
                break;
            case staj_event_type::uint64_value:
                std::cout << event.event_type() << ": " << event.get<uint64_t>() << "\n";
                break;
            case staj_event_type::double_value:
                std::cout << event.event_type() << ": " << event.get<double>() << "\n";
                break;
            default:
                std::cout << "Unhandled event type\n";
                break;
        }
    }
}
```
Output:
```
begin_array
begin_object
key: author
string_value: Haruki Murakami
key: title
string_value: Hard-Boiled Wonderland and the End of the World
key: isbn
string_value: 0679743464
key: publisher
string_value: Vintage
key: date
string_value: 1993-03-02
key: price
double_value: 19
end_object
begin_object
key: author
string_value: Graham Greene
key: title
string_value: The Comedians
key: isbn
string_value: 0099478374
key: publisher
string_value: Vintage Classics
key: date
string_value: 2005-09-21
key: price
double_value: 16
end_object
end_array
```

#### Filter BSON parse events

```cpp
#include <jsoncons_ext/bson/bson_cursor.hpp>
#include <string>
#include <fstream>

using namespace jsoncons;

int main()
{
    bool author_next = false;
    auto filter = [&](const staj_event& event, const ser_context&) -> bool
    {
        if (event.event_type() == staj_event_type::key &&
            event.get<jsoncons::string_view>() == "author")
        {
            author_next = true;
            return false;
        }
        if (author_next)
        {
            author_next = false;
            return true;
        }
        return false;
    };

    std::ifstream is("book_catalog.json");

    bson_cursor cursor(is);
    auto filtered_c = cursor | filter;

    for (; !filtered_c.done(); filtered_c.next())
    {
        const auto& event = filtered_c.current();
        switch (event.event_type())
        {
            case staj_event_type::string_value:
                std::cout << event.get<jsoncons::string_view>() << "\n";
                break;
        }
    }
}
```
Output:
```
Haruki Murakami
Graham Greene
```

### See also

[staj_event](../basic_staj_event.md)  

[staj_array_iterator](../staj_array_iterator.md)  

[staj_object_iterator](../staj_object_iterator.md)  

