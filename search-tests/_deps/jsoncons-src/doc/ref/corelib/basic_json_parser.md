### jsoncons::basic_json_parser

```cpp
#include <jsoncons/json_parser.hpp>

template< 
    typename CharT,
    typename TempAllocator = std::allocator<char>
> class basic_json_parser;
```

A `basic_json_parser` is an incremental json parser. It can be fed its input
in chunks, and does not require an entire file to be loaded in memory
at one time.

`basic_json_parser` is used by the push parser [basic_json_reader](basic_json_reader.md),
and by the pull parser [basic_json_cursor](basic_json_cursor.md).

`basic_json_parser` is noncopyable and nonmoveable.

Aliases for common character types are provided:

Type                |Definition
--------------------|------------------------------
json_parser     |`jsoncons::basic_json_parser<char,std::allocator<char>>`
wjson_parser    |`jsoncons::basic_json_parser<wchar_t,std::allocator<char>>`

#### Member types

Type                       |Definition
---------------------------|------------------------------
char_type                  |CharT
temp_allocator_type        |TempAllocator

#### Constructors

    basic_json_parser(const TempAllocator& temp_alloc = TempAllocator());                      (1)

    basic_json_parser(const basic_json_decode_options<CharT>& options, 
        const TempAllocator& temp_alloc = TempAllocator());                                    (2)

    basic_json_parser(std::function<bool(json_errc,const ser_context&)> err_handler, 
        const TempAllocator& temp_alloc = TempAllocator());                                    (3)   (deprecated since 0.171.0)

    basic_json_parser(const basic_json_decode_options<CharT>& options, 
        std::function<bool(json_errc,const ser_context&)> err_handler,                         (4)   (deprecated since 0.171.0) 
        const TempAllocator& temp_alloc = TempAllocator());                       


(1) Constructs a `json_parser` that uses default [basic_json_options](basic_json_options.md)
and a default [err_handler](err_handler.md).

(2) Constructs a `json_parser` that uses the specified [basic_json_options](basic_json_options.md)
and a default [err_handler](err_handler.md).

(3) Constructs a `json_parser` that uses default [basic_json_options](basic_json_options.md)
and a specified [err_handler](err_handler.md).

(4) Constructs a `json_parser` that uses the specified [basic_json_options](basic_json_options.md)
and a specified [err_handler](err_handler.md).

#### Member functions

    void update(const string_view_type& sv)              
    void update(const CharT* data, std::size_t length)             (until 1.0.0, since 1.1.0)
Update the parser with a chunk of JSON

    bool done() const
Returns `true` when the parser has consumed a complete JSON text, `false` otherwise

    bool stopped() const
Returns `true` if the parser is stopped, `false` otherwise.
The parser may enter a stopped state as a result of a visitor
function returning `false`, an error occurred,
or after having consumed a complete JSON text.

    bool finished() const
Returns `true` if the parser is finished parsing, `false` otherwise.

    bool source_exhausted() const
Returns `true` if the input in the source buffer has been exhausted, `false` otherwise

    void parse_some(json_visitor& visitor)
Parses the source until a complete json text has been consumed or the source has been exhausted.
Parse events are sent to the supplied `visitor`.
Throws a [ser_error](ser_error.md) if parsing fails.

    void parse_some(json_visitor<CharT>& visitor,
                    std::error_code& ec)
Parses the source until a complete json text has been consumed or the source has been exhausted.
Parse events are sent to the supplied `visitor`.
Sets `ec` to a [json_errc](jsoncons::json_errc.md) if parsing fails.

    void finish_parse(json_visitor<CharT>& visitor)
Called after `source_exhausted()` is `true` and there is no more input. 
Repeatedly calls `parse_some(visitor)` until `finished()` returns `true`
Throws a [ser_error](ser_error.md) if parsing fails.

    void finish_parse(json_visitor<CharT>& visitor,
                   std::error_code& ec)
Called after `source_exhausted()` is `true` and there is no more input. 
Repeatedly calls `parse_some(visitor)` until `finished()` returns `true`
Sets `ec` to a [json_errc](jsoncons::json_errc.md) if parsing fails.

    void skip_bom()
Reads the next JSON text from the stream and reports JSON events to a [basic_json_visitor](basic_json_visitor.md), such as a [json_decoder](json_decoder.md).
Throws a [ser_error](ser_error.md) if parsing fails.

    void check_done()
Throws if there are any unconsumed non-whitespace characters in the input.
Throws a [ser_error](ser_error.md) if parsing fails.

    void check_done(std::error_code& ec)
Sets `ec` to a [json_errc](jsoncons::json_errc.md) if parsing fails.

    void reset() const
Resets the state of the parser to its initial state. In this state
`stopped()` returns `false` and `done()` returns `false`.

    void restart() const
Resets the `stopped` state of the parser to `false`, allowing parsing
to continue.

### Examples

#### nan, inf, and -inf substitition

```cpp
int main()
{
    std::string s = R"(
        {
           "A" : "NaN",
           "B" : "Infinity",
           "C" : "-Infinity"
        }
    )";

    auto options = json_options{}
        .nan_to_str("NaN")
        .inf_to_str("Infinity");

    json_parser parser(options);
    jsoncons::json_decoder<json> decoder;
    try
    {
        parser.update(s);
        parser.parse_some(decoder);
        parser.finish_parse(decoder);
        parser.check_done();
    }
    catch (const ser_error& e)
    {
        std::cout << e.what() << '\n';
    }

    json j = decoder.get_result(); // performs move
    if (j["A"].is<double>())
    {
        std::cout << "A: " << j["A"].as<double>() << '\n';
    }
    if (j["B"].is<double>())
    {
        std::cout << "B: " << j["B"].as<double>() << '\n';
    }
    if (j["C"].is<double>())
    {
        std::cout << "C: " << j["C"].as<double>() << '\n';
    }
}
```

Output:

```
A: nan
B: inf
C: -inf
```

#### Incremental parsing (until 1.0.0, since 1.1.0)

```cpp
#include <jsoncons/json.hpp>
#include <iostream>

int main()
{
    jsoncons::json_decoder<jsoncons::json> decoder;
    jsoncons::json_parser parser;
    try
    {
        parser.update("[fal");
        parser.parse_some(decoder);
        std::cout << "(1) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.update("se,");
        parser.parse_some(decoder);
        std::cout << "(2) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.update("9");
        parser.parse_some(decoder);
        std::cout << "(3) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.update("0]");
        parser.parse_some(decoder);
        std::cout << "(4) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.finish_parse(decoder);
        std::cout << "(5) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.check_done();
        std::cout << "(6) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        jsoncons::json j = decoder.get_result();
        std::cout << "(7) " << j << "\n\n";
    }
    catch (const jsoncons::ser_error& e)
    {
        std::cout << e.what() << '\n';
    }
}
```

Output:

```
(1) done: false, source_exhausted: true

(2) done: false, source_exhausted: true

(3) done: false, source_exhausted: true

(4) done: false, source_exhausted: true

(5) done: true, source_exhausted: true

(6) done: true, source_exhausted: true

(7) [false,90]
```

