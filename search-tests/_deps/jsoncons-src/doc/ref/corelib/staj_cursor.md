### jsoncons::staj_cursor

```cpp
#include <jsoncons/staj_cursor.hpp>

typedef basic_staj_cursor<char> staj_cursor
```

The `staj_cursor` interface supports forward, read-only, access to JSON and JSON-like data formats.

The `staj_cursor` is designed to iterate over stream events until `done()` returns `true`.
The `next()` function causes the reader to advance to the next stream event. The `current()` function
returns the current stream event. The data can be accessed using the [staj_event](basic_staj_event.md) 
interface. When `next()` is called, copies of data previously accessed may be invalidated.

#### Destructor

    virtual ~basic_staj_cursor() noexcept = default;

#### Member functions

    virtual bool done() const = 0;
Check if there are no more events.

    virtual const staj_event& current() const = 0;
Returns the current [staj_event](basic_staj_event.md).

    virtual void read_to(json_visitor& visitor) = 0;
Sends the parse events from the current event to the
matching completion event to the supplied [visitor](basic_json_visitor.md)
E.g., if the current event is `begin_object`, sends the `begin_object`
event and all inbetween events until the matching `end_object` event.
If a parsing error is encountered, throws a [ser_error](ser_error.md).

    virtual void read_to(json_visitor& visitor,
                        std::error_code& ec) = 0;
Sends the parse events from the current event to the
matching completion event to the supplied [visitor](basic_json_visitor.md)
E.g., if the current event is `begin_object`, sends the `begin_object`
event and all inbetween events until the matching `end_object` event.
If a parsing error is encountered, sets `ec`.

    virtual void next() = 0;
Get the next event. If a parsing error is encountered, throws a [ser_error](ser_error.md).

    virtual void next(std::error_code& ec) = 0;
Get the next event. If a parsing error is encountered, sets `ec`.

    virtual const ser_context& context() const = 0;
Returns the current [context](ser_context.md)

#### Non-member functions

    template <typename T,typename CharT,typename Json=std::conditional<is_basic_json<T>::value,T,basic_json<CharT>>::type>
    staj_array_view<Json,T> staj_array(basic_staj_cursor<CharT>& cursor);
Create a view of the parse events as an array of items of type `T`. 
The current event type must be `staj_event_type::begin_array`.

    template <typename Key,typename T,typename CharT,typename Json=std::conditional<is_basic_json<T>::value,T,basic_json<CharT>>::type>
    staj_object_view<Key, T, Json> staj_object(basic_staj_cursor<CharT>& cursor);
Create a view of the parse events as an object of key-value pairs.
The current event type must be `staj_event_type::begin_object`.

