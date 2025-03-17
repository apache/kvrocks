# Examples

### Parse and decode

[Parse JSON from a string](#A1)  
[Parse JSON from a file](#A2)  
[Read JSON Lines](#A10)  
[Query JSON Lines in Parallel with JMESPath](#A13)  
[Parse JSON from an iterator range](#A11)  
[Parse numbers without loosing precision](#A8)  
[Validate JSON without incurring parse exceptions](#A3)  
[How to allow comments? How not to?](#A4)  
[Set a maximum nesting depth](#A5)  
[Prevent the alphabetic sort of the outputted JSON, retaining the original insertion order](#A6)  
[Parse a JSON text using a polymorphic_allocator (since 0.171.0)](#A12)  
[Decode a JSON text using stateful result and work allocators](#A9)

### Encode

[Encode a json value to a string](#B1)  
[Encode Chinese characters](#B5)  
[Encode a json value to a stream](#B2)  
[Escape all non-ascii characters](#B3)  
[Replace the representation of NaN, Inf and -Inf when serializing. And when reading in again.](#B4)

### Stream

[Write some JSON (push)](#I6)  
[Read some JSON (pull)](#I1)  
[Filter the event stream](#I2)  
[Pull nested objects into a basic_json](#I3)  
[Iterate over basic_json items](#I4)  
[Iterate over strongly typed items](#I5)  

### Decode JSON to C++ data structures, encode C++ data structures to JSON

[Serialize with the C++ member names of the class](#G2)  
[Serialize with provided names using the `_NAME_` macros](#G3)  
[Mapping to C++ data structures with and without defaults allowed](#G4)  
[Specialize json_type_traits explicitly](#G1)  
[Serialize non-mandatory std::optional values using the convenience macros](#G5)  
[An example with std::shared_ptr and std::unique_ptr](#G6)  
[Serialize a templated class with the `_TPL_` macros](#G7)  
[An example using JSONCONS_ENUM_TRAITS and JSONCONS_ALL_CTOR_GETTER_TRAITS](#G8)  
[Serialize a polymorphic type based on the presence of members](#G9)  
[Ensuring type selection is possible](#G10)  
[Decode to a polymorphic type based on a type marker (since 0.157.0)](#G14)  
[An example with std::variant](#G11)  
[Type selection and std::variant](#G12)  
[Decode to a std::variant based on a type marker (since 0.158.0)](#G15)  
[Convert JSON numbers to/from boost multiprecision numbers](#G13)

### Construct

[Construct a json object](#C1)  
[Insert a value into a location after creating objects when missing object keys](#C3)  
[Construct a json array](#C2)  
[Construct a json byte string](#C6)  
[Construct a multidimensional json array](#C7)  
[Construct a json array that contains non-owning references to other json values (since 0.156.0)](#C8)

### Access

[Use string_view to access the actual memory that's being used to hold a string](#E1)  
[Given a string in a json object that represents a decimal number, assign it to a double](#E2)  
[Retrieve a big integer that's been parsed as a string](#E3)  
[Look up a key, if found, return the value converted to type T, otherwise, return a default value of type T](#E4)  
[Retrieve a value in a hierarchy of JSON objects](#E5)  
[Retrieve a json value as a byte string](#E6)

### Iterate

[Iterate over a json array](#D1)  
[Iterate over a json object](#D2)  

### Modify

[Insert a new value in an array at a specific position](#J1)  
[Merge two json objects](#J2)  
[Erase an object with a specified key from an array](#J3)  
[Iterating an array and erasing elements](#J4)  
[Iterating an object and erasing members](#J5)  

### Flatten and unflatten

[Flatten a json object with numberish keys to JSON Pointer/value pairs](#H1)  
[Flatten a json object to JSONPath/value pairs](#H2)  

### Search and Replace

[Search for and repace an object member key](#F1)  
[Search for and replace a value](#F2)  
[Update JSON in place](#F3)  

### Parse and decode

<div id="A1"/> 

#### Parse JSON from a string

```
std::string s = R"({"first":1,"second":2,"fourth":3,"fifth":4})";    

json j = json::parse(s);
```

or

```cpp
using namespace jsoncons::literals;

json j = R"(
{
    "StartDate" : "2017-03-01",
    "MaturityDate" : "2020-12-30"          
}
)"_json;
```

See [basic_json::parse](ref/json/parse.md). 

<div id="A2"/> 

#### Parse JSON from a file

```
std::ifstream is("myfile.json");    

json j = json::parse(is);
```

See [basic_json::parse](ref/json/parse.md). 

<div id="A10"/> 

#### Read JSON Lines

This example is from [JSON Lines Examples](https://jsonlines.org/examples/).

Data:
```
["Name", "Session", "Score", "Completed"]
["Gilbert", "2013", 24, true]
["Alexa", "2013", 29, true]
["May", "2012B", 14, false]
["Deloise", "2012A", 19, true] 
```

```cpp
std::ifstream is("path_to_data");
json_decoder<json> decoder;
json_stream_reader reader(is, decoder);

while (!reader.eof())
{
    reader.read_next();

    // until 1.0.0
    //if (!reader.eof())
    //{
    //    json j = decoder.get_result();
    //    std::cout << j << '\n';
    //}
    // since 1.0.0
    json j = decoder.get_result();
    std::cout << j << '\n';
}
```
Output:
```
["Name","Session","Score","Completed"]
["Gilbert","2013",24,true]
["Alexa","2013",29,true]
["May","2012B",14,false]
```

See [JSON Lines](https://jsonlines.org/). 

<div id="A13"/> 

#### Query JSON Lines in Parallel with JMESPath

``` cpp
#include "jsoncons_ext/jmespath/jmespath.hpp"
#include <string>
#include <execution>
#include <concurrent_vector.h> // microsoft PPL library

int main(int argc, char* argv[])
{
    std::vector<std::string> lines = {{
        R"({"name": "Seattle", "state" : "WA"})",
        R"({ "name": "New York", "state" : "NY" })",
        R"({ "name": "Bellevue", "state" : "WA" })",
        R"({ "name": "Olympia", "state" : "WA" })"
    }};

    auto expr = jsoncons::jmespath::make_expression<jsoncons::json>(
        R"([@][?state=='WA'].name)");

    concurrency::concurrent_vector<std::string> result;

    auto f = [&](const std::string& line)
        {
            const auto j = jsoncons::json::parse(line);
            const auto r = expr.evaluate(j);
            if (!r.empty())
                result.push_back(r.at(0).as<std::string>());
        };

    std::for_each(std::execution::par, lines.begin(), lines.end(), f);

    for (const auto& s : result)
    {
        std::cout << s << "\n";
    }
}
```
Output:
```
Seattle
Bellevue
Olympia
```

<div id="A11"/> 

#### Parse JSON from an iterator range

```cpp
#include <jsoncons/json.hpp>

class MyIterator
{
    const char* p_;
public:
    using iterator_category = std::input_iterator_tag;
    using value_type = char;
    using difference_type = std::ptrdiff_t;
    using pointer = const char*; 
    using reference = const char&;

    MyIterator(const char* p)
        : p_(p)
    {
    }

    reference operator*() const
    {
        return *p_;
    }

    pointer operator->() const 
    {
        return p_;
    }

    MyIterator& operator++()
    {
        ++p_;
        return *this;
    }

    MyIterator operator++(int) 
    {
        MyIterator temp(*this);
        ++*this;
        return temp;
    }

    bool operator!=(const MyIterator& rhs) const
    {
        return p_ != rhs.p_;
    }
};

int main()
{
    char source[] = {'[','\"', 'f','o','o','\"',',','\"', 'b','a','r','\"',']'};

    MyIterator first(source);
    MyIterator last(source + sizeof(source));

    json j = json::parse(first, last);

    std::cout << j << "\n\n";
}
```

Output:

```json
["foo","bar"]
```

See [basic_json::parse](ref/json/parse.md). 

<div id="A8"/> 

#### Parse numbers without loosing precision

By default, jsoncons parses a number with an exponent or fractional part
into a double precision floating point number. If you wish, you can
keep the number as a string with semantic tagging `bigdec`, 
using the `lossless_number` option. You can then put it into a `float`, 
`double`, a boost multiprecision number, or whatever other type you want. 

```cpp
#include <jsoncons/json.hpp>

int main()
{
    std::string s = R"(
    {
        "a" : 12.00,
        "b" : 1.23456789012345678901234567890
    }
    )";

    // Default
    json j = json::parse(s);

    std::cout.precision(15);

    // Access as string
    std::cout << "(1) a: " << j["a"].as<std::string>() << ", b: " << j["b"].as<std::string>() << "\n"; 
    // Access as double
    std::cout << "(2) a: " << j["a"].as<double>() << ", b: " << j["b"].as<double>() << "\n\n"; 

    // Using lossless_number option
    auto options = json_options{}
        .lossless_number(true);

    json j2 = json::parse(s, options);
    // Access as string
    std::cout << "(3) a: " << j2["a"].as<std::string>() << ", b: " << j2["b"].as<std::string>() << "\n";
    // Access as double
    std::cout << "(4) a: " << j2["a"].as<double>() << ", b: " << j2["b"].as<double>() << "\n\n"; 
}
```
Output:
```
(1) a: 12.0, b: 1.2345678901234567
(2) a: 12, b: 1.23456789012346

(3) a: 12.00, b: 1.23456789012345678901234567890
(4) a: 12, b: 1.23456789012346
```

<div id="A3"/> 

#### Validate JSON without incurring parse exceptions
```cpp
std::string s = R"(
{
    "StartDate" : "2017-03-01",
    "MaturityDate" "2020-12-30"          
}
)";

//json_reader reader(s);          // (until 0.164.0)
json_string_reader reader(s);     // (since 0.164.0)

// or,
// std::stringstream is(s);
// json_reader reader(is);        // (until 0.164.0)
// json_stream_reader reader(is); // (since 0.164.0)

std::error_code ec;
reader.read(ec);

if (ec)
{
    std::cout << ec.message() 
              << " on line " << reader.line()
              << " and column " << reader.column()
              << '\n';
}
```
Output:
```
Expected name separator ':' on line 4 and column 20
```

<div id="A4"/> 

#### How to allow comments? How not to?

jsoncons, by default, accepts and ignores C-style comments

```cpp
std::string s = R"(
{
    // Single line comments
    /*
        Multi line comments 
    */
}
)";

// Default
json j = json::parse(s);
std::cout << "(1) " << j << '\n';

// Strict
try
{
    // until 0.170.0
    auto j1 = jsoncons::json::parse(s, jsoncons::strict_json_parsing());

    // since 0.171.0
    auto options = json_options{}
        .err_handler(jsoncons::strict_json_parsing());
    auto j2 = jsoncons::json::parse(s, options);

    // since 1.3.0
    auto options = json_options{}
        .allow_comments(false);
    auto j3 = jsoncons::json::parse(s, options);
}
catch (const ser_error& e)
{
    std::cout << "(2) " << e.what() << '\n';
}
```
Output:
```
(1) {}
(2) Illegal comment at line 3 and column 10
```

<div id="A5"/> 

#### Set a maximum nesting depth

Like this,
```cpp
std::string s = "[[[[[[[[[[[[[[[[[[[[[\"Too deep\"]]]]]]]]]]]]]]]]]]]]]";
try
{
    auto options = json_options{}
        .max_nesting_depth(20);
    json j = json::parse(s, options);
}
catch (const ser_error& e)
{
     std::cout << e.what() << '\n';
}
```
Output:
```
Maximum JSON depth exceeded at line 1 and column 21
```

<div id="A6"/> 

#### Prevent the alphabetic sort of the outputted JSON, retaining the original insertion order

Use `ojson` instead of `json` (or `wojson` instead of `wjson`) to retain the original insertion order. 

```cpp
ojson j = ojson::parse(R"(
{
    "street_number" : "100",
    "street_name" : "Queen St W",
    "city" : "Toronto",
    "country" : "Canada"
}
)");
std::cout << "(1)\n" << pretty_print(j) << '\n';

// Insert "postal_code" at end
j.insert_or_assign("postal_code", "M5H 2N2");
std::cout << "(2)\n" << pretty_print(j) << '\n';

// Insert "province" before "country"
auto it = j.find("country");
j.insert_or_assign(it,"province","Ontario");
std::cout << "(3)\n" << pretty_print(j) << '\n';
```
Output:
```
(1)
{
    "street_number": "100",
    "street_name": "Queen St W",
    "city": "Toronto",
    "country": "Canada"
}
(2)
{
    "street_number": "100",
    "street_name": "Queen St W",
    "city": "Toronto",
    "country": "Canada",
    "postal_code": "M5H 2N2"
}
(3)
{
    "street_number": "100",
    "street_name": "Queen St W",
    "city": "Toronto",
    "province": "Ontario",
    "country": "Canada",
    "postal_code": "M5H 2N2"
}
```

<div id="A12"/> 

### Parse a JSON text using a polymorphic_allocator (since 0.171.0)

```cpp
using pmr_json = jsoncons::pmr::json;

char buffer[1024] = {}; // a small buffer on the stack
std::pmr::monotonic_buffer_resource pool{std::data(buffer), std::size(buffer)};
std::pmr::polymorphic_allocator<char> alloc(&pool);

std::string json_text = R"(
{
    "street_number" : "100",
    "street_name" : "Queen St W",
    "city" : "Toronto",
    "country" : "Canada"
}
)";

auto doc = pmr_json::parse(combine_allocators(alloc), json_text, json_options{});
std::cout << pretty_print(doc) << "\n\n";
```

<div id="A9"/> 

### Decode a JSON text using stateful result and work allocators

```cpp
// Given allocator my_alloc with a single-argument constructor my_alloc(int),
// use my_alloc(1) to allocate basic_json memory, my_alloc(2) to allocate
// working memory used by json_decoder, and my_alloc(3) to allocate
// working memory used by basic_json_reader. 

using my_json = basic_json<char,sorted_policy,my_alloc>; // until 0.171.0

using my_json = basic_json<char,sorted_policy,std::scoped_allocator_adaptor<my_alloc>>; // since 0.171.0

std::ifstream is("book_catalog.json");
json_decoder<my_json,my_alloc> decoder(my_alloc(1),my_alloc(2));

basic_json_reader<char,stream_source<char>,my_alloc> reader(is, decoder, my_alloc(3));
reader.read();

my_json j = decoder.get_result();
std::cout << pretty_print(j) << "\n";
```

### Encode

<div id="B1"/>

#### Encode a json value to a string

```
std::string s;

j.dump(s); // compressed

j.dump(s, indenting::indent); // pretty print
```

<div id="B5"/>

#### Encode Chinese characters

For all versions of jsoncons:

```
jsoncons::json j;

std::string s = (const char*)u8"你好";
j.try_emplace("hello", s);
assert(j["hello"].as<std::string>() == s);

std::string json_string;
j.dump(json_string);
```

Since 0.171.0, and assuming C++ 20:

```
jsoncons::json j;

std::u8string s = u8"你好";
j.try_emplace("hello", s);
assert(j["hello"].as<std::u8string>() == s);

std::string json_string;
j.dump(json_string);
```

<div id="B2"/>

#### Encode a json value to a stream

```
j.dump(std::cout); // compressed

j.dump(std::cout, indenting::indent); // pretty print
```
or
```
std::cout << j << '\n'; // compressed

std::cout << pretty_print(j) << '\n'; // pretty print
```

<div id="B3"/>

#### Escape all non-ascii characters

```
auto options = json_options{}
    .escape_all_non_ascii(true);

j.dump(std::cout, options); // compact

j.dump(std::cout, options, indenting::indent); // pretty print
```
or
```
std::cout << print(j, options) << '\n'; // compressed

std::cout << pretty_print(j, options) << '\n'; // pretty print
```

<div id="B4"/>

#### Replace the representation of NaN, Inf and -Inf when serializing. And when reading in again.

Set the serializing options for `nan` and `inf` to distinct string values.

```cpp
json j;
j["field1"] = std::sqrt(-1.0);
j["field2"] = 1.79e308 * 1000;
j["field3"] = -1.79e308 * 1000;

auto options = json_options{}
    .nan_to_str("NaN")
    .inf_to_str("Inf"); 

std::ostringstream os;
os << pretty_print(j, options);

std::cout << "(1)\n" << os.str() << '\n';

json j2 = json::parse(os.str(),options);

std::cout << "\n(2) " << j2["field1"].as<double>() << '\n';
std::cout << "(3) " << j2["field2"].as<double>() << '\n';
std::cout << "(4) " << j2["field3"].as<double>() << '\n';

std::cout << "\n(5)\n" << pretty_print(j2,options) << '\n';
```

Output:
```json
(1)
{
    "field1": "NaN",
    "field2": "Inf",
    "field3": "-Inf"
}

(2) nan
(3) inf
(4) -inf

(5)
{
    "field1": "NaN",
    "field2": "Inf",
    "field3": "-Inf"
}
```

### Stream

<div id="I6"/> 

#### Write some JSON (push)

```cpp
#include <jsoncons/json_cursor.hpp>
#include <jsoncons/json_encoder.hpp>
#include <fstream>
#include <cassert>

int main()
{
    std::ofstream os("./output/book_catalog.json", 
                     std::ios_base::out | std::ios_base::trunc);
    assert(os);

    compact_json_stream_encoder encoder(os); // no indent

    encoder.begin_array();
    encoder.begin_object();
    encoder.key("author");
    encoder.string_value("Haruki Murakami");
    encoder.key("title");
    encoder.string_value("Hard-Boiled Wonderland and the End of the World");
    encoder.key("price");
    encoder.double_value(18.9);
    encoder.end_object();
    encoder.begin_object();
    encoder.key("author");
    encoder.string_value("Graham Greene");
    encoder.key("title");
    encoder.string_value("The Comedians");
    encoder.key("price");
    encoder.double_value(15.74);
    encoder.end_object();
    encoder.end_array();
    encoder.flush();

    os.close();

    // Read the JSON and write it prettified to std::cout
    json_stream_encoder writer(std::cout); // indent

    std::ifstream is("./output/book_catalog.json");
    assert(is);

    //json_reader reader(is, writer);        // (until 0.164.0)
    json_stream_reader reader(is, writer);   // (since 0.164.0)
    reader.read();
    std::cout << "\n\n";
}
```
Output:
```
[
    {
        "author": "Haruki Murakami",
        "title": "Hard-Boiled Wonderland and the End of the World",
        "price": 18.9
    },
    {
        "author": "Graham Greene",
        "title": "The Comedians",
        "price": 15.74
    }
]
```

<div id="I1"/> 

#### Read some JSON (pull)

A typical pull parsing application will repeatedly process the `current()` 
event and call `next()` to advance to the next event, until `done()` 
returns `true`.

```cpp
#include <jsoncons/json_cursor.hpp>
#include <fstream>

int main()
{
    std::ifstream is("./output/book_catalog.json");

    json_stream_cursor cursor(is);

    for (; !cursor.done(); cursor.next())
    {
        const auto& event = cursor.current();
        switch (event.event_type())
        {
            case staj_event_type::begin_array:
                std::cout << event.event_type() << " " << "\n";
                break;
            case staj_event_type::end_array:
                std::cout << event.event_type() << " " << "\n";
                break;
            case staj_event_type::begin_object:
                std::cout << event.event_type() << " " << "\n";
                break;
            case staj_event_type::end_object:
                std::cout << event.event_type() << " " << "\n";
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
                std::cout << event.event_type() << "\n";
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
                std::cout << "Unhandled event type: " << event.event_type() << " " << "\n";
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
key: price
double_value: 18.9
end_object
begin_object
key: author
string_value: Graham Greene
key: title
string_value: The Comedians
key: price
double_value: 15.74
end_object
end_array
```

<div id="I2"/> 

#### Filter the event stream

You can apply a filter to a cursor using the pipe syntax (e.g., `cursor | filter1 | filter2 | ...`)

```cpp

#include <jsoncons/json_cursor.hpp>
#include <fstream>

// Filter out all events except names of authors

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

    std::ifstream is("./output/book_catalog.json");

    json_stream_cursor cursor(is);
    auto filtered_c = cursor | filter;

    for (; !filtered_c.done(); filtered_c.next())
    {
        const auto& event = filtered_c.current();
        switch (event.event_type())
        {
            case staj_event_type::string_value:
                std::cout << event.get<jsoncons::string_view>() << "\n";
                break;
            default:
                std::cout << "Unhandled event type: " << event.event_type() << " " << "\n";
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

<div id="I3"/> 

#### Pull nested objects into a basic_json

When positioned on a `begin_object` event, 
the `read_to` function can pull a complete object representing
the events from `begin_object` to `end_object`, 
and when positioned on a `begin_array` event, a complete array
representing the events from `begin_array` ro `end_array`.

```cpp
#include <jsoncons/json.hpp> // json_decoder and json
#include <fstream>

int main()
{
    std::ifstream is("./output/book_catalog.json");

    json_stream_cursor cursor(is);

    json_decoder<json> decoder;
    for (; !cursor.done(); cursor.next())
    {
        const auto& event = cursor.current();
        switch (event.event_type())
        {
            case staj_event_type::begin_array:
            {
                std::cout << event.event_type() << " " << "\n";
                break;
            }
            case staj_event_type::end_array:
            {
                std::cout << event.event_type() << " " << "\n";
                break;
            }
            case staj_event_type::begin_object:
            {
                std::cout << event.event_type() << " " << "\n";
                cursor.read_to(decoder);
                json j = decoder.get_result();
                std::cout << pretty_print(j) << "\n";
                break;
            }
            default:
            {
                std::cout << "Unhandled event type: " << event.event_type() << " " << "\n";
                break;
            }
        }
    }
}
```
Output:
```
begin_array
begin_object
{
    "author": "Haruki Murakami",
    "price": 18.9,
    "title": "Hard-Boiled Wonderland and the End of the World"
}
begin_object
{
    "author": "Graham Greene",
    "price": 15.74,
    "title": "The Comedians"
}
end_array
```

See [basic_json_cursor](ref/basic_json_cursor.md) 

<div id="I4"/> 

#### Iterate over basic_json items

```cpp
#include <jsoncons/json.hpp> 
#include <fstream>

int main()
{
    std::ifstream is("./output/book_catalog.json");

    json_stream_cursor cursor(is);

    auto view = staj_array<json>(cursor);
    for (const auto& j : view)
    {
        std::cout << pretty_print(j) << "\n";
    }
}
```
Output:
```
{
    "author": "Haruki Murakami",
    "price": 18.9,
    "title": "Hard-Boiled Wonderland and the End of the World"
}
{
    "author": "Graham Greene",
    "price": 15.74,
    "title": "The Comedians"
}
```

See [staj_array_iterator](ref/staj_array_iterator.md) 

<div id="I5"/> 

#### Iterate over strongly typed items

```cpp
#include <jsoncons/json.hpp> 
#include <fstream>

namespace ns {

    struct book
    {
        std::string author;
        std::string title;
        double price;
    };

} // namespace ns

JSONCONS_ALL_MEMBER_TRAITS(ns::book,author,title,price)

int main()
{
    std::ifstream is("./output/book_catalog.json");

    json_stream_cursor cursor(is);

    auto view = staj_array<ns::book>(cursor);
    for (const auto& book : view)
    {
        std::cout << book.author << ", " << book.title << "\n";
    }
}
```
Output:
```
Haruki Murakami, Hard-Boiled Wonderland and the End of the World
Graham Greene, The Comedians
```

See [staj_array_iterator](ref/staj_array_iterator.md) 

<div id="G0"/>

### Decode JSON to C++ data structures, encode C++ data structures to JSON

<div id="G2"/>

#### Serialize with the C++ member names of the class

```cpp
#include <jsoncons/json.hpp>

namespace ns {

    enum class BookCategory {fiction,biography};

    inline
    std::ostream& operator<<(std::ostream& os, const BookCategory& category)
    {
        switch (category)
        {
            case BookCategory::fiction: os << "fiction, "; break;
            case BookCategory::biography: os << "biography, "; break;
        }
        return os;
    }

    // #1 Class with public member data and default constructor   
    struct Book1
    {
        BookCategory category;
        std::string author;
        std::string title;
        double price;
    };

    // #2 Class with private member data and default constructor   
    class Book2
    {
        BookCategory category;
        std::string author;
        std::string title;
        double price;
        Book2() = default;

        JSONCONS_TYPE_TRAITS_FRIEND
    public:
        BookCategory get_category() const {return category;}

        const std::string& get_author() const {return author;}

        const std::string& get_title() const{return title;}

        double get_price() const{return price;}
    };

    // #3 Class with getters and initializing constructor
    class Book3
    {
        BookCategory category_;
        std::string author_;
        std::string title_;
        double price_;
    public:
        Book3(BookCategory category,
              const std::string& author,
              const std::string& title,
              double price)
            : category_(category), author_(author), title_(title), price_(price)
        {
        }

        BookCategory category() const {return category_;}

        const std::string& author() const{return author_;}

        const std::string& title() const{return title_;}

        double price() const{return price_;}
    };

    // #4 Class with getters and setters
    class Book4
    {
        BookCategory category_;
        std::string author_;
        std::string title_;
        double price_;
    public:
        Book4()
            : price_(0)
        {
        }

        Book4(BookCategory category,
              const std::string& author,
              const std::string& title,
              double price)
            : category_(category), author_(author), title_(title), price_(price)
        {
        }

        BookCategory get_category() const
        {
            return category_;
        }

        void set_category(BookCategory value)
        {
            category_ = value;
        }

        const std::string& get_author() const
        {
            return author_;
        }

        void set_author(const std::string& value)
        {
            author_ = value;
        }

        const std::string& get_title() const
        {
            return title_;
        }

        void set_title(const std::string& value)
        {
            title_ = value;
        }

        double get_price() const
        {
            return price_;
        }

        void set_price(double value)
        {
            price_ = value;
        }
    };

} // namespace ns

// Declare the traits at global scope
JSONCONS_ENUM_TRAITS(ns::BookCategory,fiction,biography)

JSONCONS_ALL_MEMBER_TRAITS(ns::Book1,category,author,title,price)
JSONCONS_ALL_MEMBER_TRAITS(ns::Book2,category,author,title,price)
JSONCONS_ALL_CTOR_GETTER_TRAITS(ns::Book3,category,author,title,price)
JSONCONS_ALL_GETTER_SETTER_TRAITS(ns::Book4,get_,set_,category,author,title,price)

using namespace jsoncons; // for convenience

int main()
{
    const std::string input = R"(
    [
        {
            "category" : "fiction",
            "author" : "Haruki Murakami",
            "title" : "Kafka on the Shore",
            "price" : 25.17
        },
        {
            "category" : "biography",
            "author" : "Robert A. Caro",
            "title" : "The Path to Power: The Years of Lyndon Johnson I",
            "price" : 16.99
        }
    ]
    )";

    std::cout << "(1)\n\n";
    auto books1 = decode_json<std::vector<ns::Book1>>(input);
    for (const auto& item : books1)
    {
        std::cout << item.category << ", "
                  << item.author << ", " 
                  << item.title << ", " 
                  << item.price << "\n";
    }
    std::cout << "\n";
    encode_json(books1, std::cout, indenting::indent);
    std::cout << "\n\n";

    std::cout << "(2)\n\n";
    auto books2 = decode_json<std::vector<ns::Book2>>(input);
    for (const auto& item : books2)
    {
        std::cout << item.get_category() << ", "
                  << item.get_author() << ", " 
                  << item.get_title() << ", " 
                  << item.get_price() << "\n";
    }
    std::cout << "\n";
    encode_json(books2, std::cout, indenting::indent);
    std::cout << "\n\n";

    std::cout << "(3)\n\n";
    auto books3 = decode_json<std::vector<ns::Book3>>(input);
    for (const auto& item : books3)
    {
        std::cout << item.category() << ", "
                  << item.author() << ", " 
                  << item.title() << ", " 
                  << item.price() << "\n";
    }
    std::cout << "\n";
    encode_json(books3, std::cout, indenting::indent);
    std::cout << "\n\n";

    std::cout << "(4)\n\n";
    auto books4 = decode_json<std::vector<ns::Book4>>(input);
    for (const auto& item : books4)
    {
        std::cout << item.get_category() << ", "
                  << item.get_author() << ", " 
                  << item.get_title() << ", " 
                  << item.get_price() << "\n";
    }
    std::cout << "\n";
    encode_json(books4, std::cout, indenting::indent);
    std::cout << "\n\n";
}
```
Output:
```
(1)

fiction, Haruki Murakami, Kafka on the Shore, 25.170000
biography, Robert A. Caro, The Path to Power: The Years of Lyndon Johnson I, 16.990000

[
    {
        "author": "Haruki Murakami",
        "category": "fiction",
        "price": 25.17,
        "title": "Kafka on the Shore"
    },
    {
        "author": "Robert A. Caro",
        "category": "biography",
        "price": 16.99,
        "title": "The Path to Power: The Years of Lyndon Johnson I"
    }
]
```

The output for (2), (3) and (4) is the same.

<div id="G3"/>

#### Serialize with provided names using the `_NAME_` macros

```cpp
#include <jsoncons/json.hpp>

namespace ns {

    enum class BookCategory {fiction,biography};

    inline
    std::ostream& operator<<(std::ostream& os, const BookCategory& category)
    {
        switch (category)
        {
            case BookCategory::fiction: os << "fiction, "; break;
            case BookCategory::biography: os << "biography, "; break;
        }
        return os;
    }

    // #1 Class with public member data and default constructor   
    struct Book1
    {
        BookCategory category;
        std::string author;
        std::string title;
        double price;
    };

    // #2 Class with private member data and default constructor   
    class Book2
    {
        BookCategory category_;
        std::string author_;
        std::string title_;
        double price_;
        Book2() = default;

        JSONCONS_TYPE_TRAITS_FRIEND
    public:
        BookCategory category() const {return category_;}

        const std::string& author() const {return author_;}

        const std::string& title() const{return title_;}

        double price() const{return price_;}
    };

    // #3 Class with getters and initializing constructor
    class Book3
    {
        BookCategory category_;
        std::string author_;
        std::string title_;
        double price_;
    public:
        Book3(BookCategory category,
              const std::string& author,
              const std::string& title,
              double price)
            : category_(category), author_(author), title_(title), price_(price)
        {
        }

        BookCategory category() const {return category_;}

        const std::string& author() const{return author_;}

        const std::string& title() const{return title_;}

        double price() const{return price_;}
    };

    // #4 Class with getters, setters and default constructor
    class Book4
    {
        BookCategory category_;
        std::string author_;
        std::string title_;
        double price_;

    public:
        BookCategory getCategory() const {return category_;}
        void setCategory(const BookCategory& value) {category_ = value;}

        const std::string& getAuthor() const {return author_;}
        void setAuthor(const std::string& value) {author_ = value;}

        const std::string& getTitle() const {return title_;}
        void setTitle(const std::string& value) {title_ = value;}

        double getPrice() const {return price_;}
        void setPrice(double value) {price_ = value;}
    };

} // namespace ns

// Declare the traits at global scope
JSONCONS_ENUM_NAME_TRAITS(ns::BookCategory,(fiction,"Fiction"),(biography,"Biography"))

JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Book1,(category,"Category"),(author,"Author"),
                                          (title,"Title"),(price,"Price"))
JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Book2,(category_,"Category"),(author_,"Author"),
                                          (title_,"Title"),(price_,"Price"))
JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Book3,(category,"Category"),(author,"Author"),
                                               (title,"Title"),(price,"Price"))
JSONCONS_ALL_GETTER_SETTER_NAME_TRAITS(ns::Book4,(getCategory,setCategory,"Category"),
                                                 (getAuthor,setAuthor,"Author"),
                                                 (getTitle,setTitle,"Title"),
                                                 (getPrice,setPrice,"Price"))

using namespace jsoncons; // for convenience

int main()
{
    const std::string input = R"(
    [
        {
            "Category" : "Fiction",
            "Author" : "Haruki Murakami",
            "Title" : "Kafka on the Shore",
            "Price" : 25.17
        },
        {
            "Category" : "Biography",
            "Author" : "Robert A. Caro",
            "Title" : "The Path to Power: The Years of Lyndon Johnson I",
            "Price" : 16.99
        }
    ]
    )";

    std::cout << "(1)\n\n";
    auto books1 = decode_json<std::vector<ns::Book1>>(input);
    for (const auto& item : books1)
    {
        std::cout << item.category << ", "
                  << item.author << ", " 
                  << item.title << ", " 
                  << item.price << "\n";
    }
    std::cout << "\n";
    encode_json(books1, std::cout, indenting::indent);
    std::cout << "\n\n";

    std::cout << "(2)\n\n";
    auto books2 = decode_json<std::vector<ns::Book2>>(input);
    for (const auto& item : books2)
    {
        std::cout << item.category() << ", "
                  << item.author() << ", " 
                  << item.title() << ", " 
                  << item.price() << "\n";
    }
    std::cout << "\n";
    encode_json(books2, std::cout, indenting::indent);
    std::cout << "\n\n";

    std::cout << "(3)\n\n";
    auto books3 = decode_json<std::vector<ns::Book3>>(input);
    for (const auto& item : books3)
    {
        std::cout << item.category() << ", "
                  << item.author() << ", " 
                  << item.title() << ", " 
                  << item.price() << "\n";
    }
    std::cout << "\n";
    encode_json(books3, std::cout, indenting::indent);
    std::cout << "\n\n";

    std::cout << "(4)\n\n";
    auto books4 = decode_json<std::vector<ns::Book4>>(input);
    for (const auto& item : books4)
    {
        std::cout << item.getCategory() << ", "
                  << item.getAuthor() << ", " 
                  << item.getTitle() << ", " 
                  << item.getPrice() << "\n";
    }
    std::cout << "\n";
    encode_json(books4, std::cout, indenting::indent);
    std::cout << "\n\n";
}
```
Output:
```
(1)

fiction, Haruki Murakami, Kafka on the Shore, 25.170000
biography, Robert A. Caro, The Path to Power: The Years of Lyndon Johnson I, 16.990000

[
    {
        "Author": "Haruki Murakami",
        "Category": "Fiction",
        "Price": 25.17,
        "Title": "Kafka on the Shore"
    },
    {
        "Author": "Robert A. Caro",
        "Category": "Biography",
        "Price": 16.99,
        "Title": "The Path to Power: The Years of Lyndon Johnson I"
    }
]
```

The output for (2), (3) and (4) is the same.

<div id="G4"/>

#### Mapping to C++ data structures with and without defaults allowed

`JSONCONS_N_MEMBER_TRAITS` and `JSONCONS_ALL_MEMBER_TRAITS` both generate
the code to specialize `json_type_traits` from member data. The difference is that `JSONCONS_N_MEMBER_TRAITS`
does not require all member names to be present in the JSON data, while `JSONCONS_ALL_MEMBER_TRAITS` does.
More generaly, the qualifier _N_ in the macro name indicates that only a specified number of members
must be present in the JSON.

```cpp
#include <iostream>
#include <jsoncons/json.hpp>
#include <vector>
#include <string>

namespace ns {

    class Person
    {
    public:
        Person(const std::string& name, const std::string& surname,
               const std::string& ssn, unsigned int age)
           : name(name), surname(surname), ssn(ssn), age(age) { }

    private:
        // Make json_type_traits specializations friends to give accesses to private members
        JSONCONS_TYPE_TRAITS_FRIEND

        Person() : age(0) {}

        std::string name;
        std::string surname;
        std::string ssn;
        unsigned int age;
    };

} // namespace ns

// Declare the traits. Specify which data members need to be serialized, and how many are mandatory.
JSONCONS_N_MEMBER_TRAITS(ns::Person, 2, name, surname, ssn, age)

int main()
{
    try
    {
        // Incomplete JSON data: field ssn missing
        std::string data = R"({"name":"Rod","surname":"Bro","age":30})";
        auto person = jsoncons::decode_json<ns::Person>(data);

        std::string s;
        jsoncons::encode_json(person, s, indenting::indent);
        std::cout << s << "\n";
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << "";
    }
}
```
Output:
```
{
    "age": 30,
    "name": "Rod",
    "ssn": "",
    "surname": "Bro"
}
```

If all members of the JSON data must be present, use
```
JSONCONS_ALL_MEMBER_TRAITS(ns::Person, name, surname, ssn, age)
```
instead. This will cause a [jsoncons::conv_error](ref/conv_error.md) to be thrown with the message
```
Key not found: 'ssn' 
```

<div id="G1"/>

#### Specialize json_type_traits explicitly

jsoncons supports conversion between JSON text and C++ data structures. The functions [decode_json](ref/decode_json.md) 
and [encode_json](ref/encode_json.md) convert JSON formatted strings or streams to C++ data structures and back. 
Decode and encode work for all C++ classes that have 
[json_type_traits](ref/json_type_traits.md) 
defined. jsoncons already supports many types in the standard library, 
and your own types will be supported too if you specialize `json_type_traits`
in the `jsoncons` namespace. 


```cpp
#include <iostream>
#include <jsoncons/json.hpp>
#include <vector>
#include <string>

namespace ns {
    struct book
    {
        std::string author;
        std::string title;
        double price;
    };
} // namespace ns

namespace jsoncons {

    template <typename Json>
    struct json_type_traits<Json, ns::book>
    {
        using allocator_type = Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_object() && j.contains("author") && 
                   j.contains("title") && j.contains("price");
        }
        static ns::book as(const Json& j)
        {
            ns::book val;
            val.author = j.at("author").template as<std::string>();
            val.title = j.at("title").template as<std::string>();
            val.price = j.at("price").template as<double>();
            return val;
        }
        static Json to_json(const ns::book& val, 
                            allocator_type allocator=allocator_type())
        {
            Json j(allocator);
            j.try_emplace("author", val.author);
            j.try_emplace("title", val.title);
            j.try_emplace("price", val.price);
            return j;
        }
    };
} // namespace jsoncons
```

To save typing and enhance readability, the jsoncons library defines macros, 
so you could also write

```cpp
JSONCONS_ALL_MEMBER_TRAITS(ns::book, author, title, price)
```

which expands to the code above.

```cpp
using namespace jsoncons; // for convenience

int main()
{
    const std::string s = R"(
    [
        {
            "author" : "Haruki Murakami",
            "title" : "Kafka on the Shore",
            "price" : 25.17
        },
        {
            "author" : "Charles Bukowski",
            "title" : "Pulp",
            "price" : 22.48
        }
    ]
    )";

    std::vector<ns::book> book_list = decode_json<std::vector<ns::book>>(s);

    std::cout << "(1)\n";
    for (const auto& item : book_list)
    {
        std::cout << item.author << ", " 
                  << item.title << ", " 
                  << item.price << "\n";
    }

    std::cout << "\n(2)\n";
    encode_json(book_list, std::cout, indenting::indent);
    std::cout << "\n\n";
}
```
Output:
```
(1)
Haruki Murakami, Kafka on the Shore, 25.17
Charles Bukowski, Pulp, 22.48

(2)
[
    {
        "author": "Haruki Murakami",
        "price": 25.17,
        "title": "Kafka on the Shore"
    },
    {
        "author": "Charles Bukowski",
        "price": 22.48,
        "title": "Pulp"
    }
]
```

<div id="G5"/>

#### Serialize non-mandatory std::optional values using the convenience macros

The jsoncons library includes a [json_type_traits](ref/json_type_traits.md) specialization for 
`jsoncons::optional<T>` if `T` is also specialized. `jsoncons::optional<T>` is aliased to 
[std::optional<T>](https://en.cppreference.com/w/cpp/utility/optional) if 
jsoncons detects the presence of C++17, or if `JSONCONS_HAS_STD_OPTIONAL` is defined.
An empty `jsoncons::optional<T>` value correspond to JSON null.

This example assumes C++17 language support (otherwise substitute `jsoncons::optional`.)

Macro names include qualifiers `_ALL_` or `_N_` to indicate that the generated traits require all
members be present in the JSON, or a specified number be present. For non-mandatory members, the generated 
traits `to_json` function will exclude altogether empty values for `std::optional`.

```cpp
#include <cassert>
#include <jsoncons/json.hpp>

namespace ns
{
    class MetaDataReplyTest 
    {
    public:
        MetaDataReplyTest()
            : description()
        {
        }
        const std::string& GetStatus() const 
        {
            return status;
        }
        const std::string& GetPayload() const 
        {
            return payload;
        }
        const std::optional<std::string>& GetDescription() const 
        {
            return description;
        }
    private:
        JSONCONS_TYPE_TRAITS_FRIEND
        std::string status;
        std::string payload;
        std::optional<std::string> description;
    };
}

JSONCONS_N_MEMBER_TRAITS(ns::MetaDataReplyTest, 2, status, payload, description)

using namespace jsoncons;

int main()
{
    std::string input1 = R"({
      "status": "OK",
      "payload": "Modified",
      "description": "TEST"
    })";
    std::string input2 = R"({
      "status": "OK",
      "payload": "Modified"
    })";

    auto val1 = decode_json<ns::MetaDataReplyTest>(input1);
    assert(val1.GetStatus() == "OK");
    assert(val1.GetPayload() == "Modified");
    assert(val1.GetDescription());
    assert(val1.GetDescription() == "TEST");

    auto val2 = decode_json<ns::MetaDataReplyTest>(input2);
    assert(val2.GetStatus() == "OK");
    assert(val2.GetPayload() == "Modified");
    assert(!val2.GetDescription());

    std::string output1;
    std::string output2;

    encode_json(val2, output2, indenting::indent);
    encode_json(val1, output1, indenting::indent);

    std::cout << "(1)\n";
    std::cout << output1 << "\n\n";

    std::cout << "(2)\n";
    std::cout << output2 << "\n\n";
}
```
Output:
```
(1)
{
    "description": "TEST",
    "payload": "Modified",
    "status": "OK"
}

(2)
{
    "payload": "Modified",
    "status": "OK"
}
```

<div id="G6"/>

#### An example with std::shared_ptr and std::unique_ptr

The jsoncons library includes [json_type_traits](ref/json_type_traits.md) specializations for 
`std::shared_ptr<T>` and `std::unique_ptr<T>` if `T` is not a [polymorphic class](https://en.cppreference.com/w/cpp/language/object#Polymorphic_objects), 
i.e., does not have any virtual functions, and if `T` is also specialized. Empty `std::shared_ptr<T>` and `std::unique_ptr<T>` values correspond to JSON null.
In addition, users can implement `json_type_traits` for `std::shared_ptr` and `std::unique_ptr`
with polymorphic classes using the convenience macro `JSONCONS_POLYMORPHIC_TRAITS`, or by specializing `json_type_traits` explicitly.

The convenience macros whose names include the qualifier `_N_` do not require all members to be present in the JSON.
For these, the generated traits `to_json` function will exclude altogether empty values 
for `std::shared_ptr` and `std::unique_ptr`.

```cpp
namespace ns {

    struct smart_pointer_test
    {
        std::shared_ptr<std::string> field1;
        std::unique_ptr<std::string> field2;
        std::shared_ptr<std::string> field3;
        std::unique_ptr<std::string> field4;
        std::shared_ptr<std::string> field5;
        std::unique_ptr<std::string> field6;
        std::shared_ptr<std::string> field7;
        std::unique_ptr<std::string> field8;
    };

} // namespace ns

// Declare the traits, first 4 members mandatory, last 4 non-mandatory
JSONCONS_N_MEMBER_TRAITS(ns::smart_pointer_test,4,field1,field2,field3,field4,field5,field6,field7,field8)

int main()
{
    ns::smart_pointer_test val;
    val.field1 = std::make_shared<std::string>("Field 1"); 
    val.field2 = jsoncons::make_unique<std::string>("Field 2"); 
    val.field3 = std::shared_ptr<std::string>(nullptr);
    val.field4 = std::unique_ptr<std::string>(nullptr);
    val.field5 = std::make_shared<std::string>("Field 5"); 
    val.field6 = jsoncons::make_unique<std::string>("Field 6"); 
    val.field7 = std::shared_ptr<std::string>(nullptr);
    val.field8 = std::unique_ptr<std::string>(nullptr);

    std::string buf;
    encode_json(val, buf, indenting::indent);

    std::cout << buf << "\n";

    auto other = decode_json<ns::smart_pointer_test>(buf);

    assert(*other.field1 == *val.field1);
    assert(*other.field2 == *val.field2);
    assert(!other.field3);
    assert(!other.field4);
    assert(*other.field5 == *val.field5);
    assert(*other.field6 == *val.field6);
    assert(!other.field7);
    assert(!other.field8);
}
```
Output:
```
{
    "field1": "Field 1",
    "field2": "Field 2",
    "field3": null,
    "field4": null,
    "field5": "Field 5",
    "field6": "Field 6"
}
```

<div id="G7"/>

#### Serialize a templated class with the `_TPL_` macros

```cpp
#include <cassert>
#include <jsoncons/json.hpp>

namespace ns {
    template <typename T1,typename T2>
    struct TemplatedStruct
    {
          T1 aT1;
          T2 aT2;

          friend bool operator==(const TemplatedStruct& lhs, const TemplatedStruct& rhs)
          {
              return lhs.aT1 == rhs.aT1 && lhs.aT2 == rhs.aT2;  
          }

          friend bool operator!=(const TemplatedStruct& lhs, const TemplatedStruct& rhs)
          {
              return !(lhs == rhs);
          }
    };

} // namespace ns

// Declare the traits. Specify the number of template parameters and which data members need to be serialized.
JSONCONS_TPL_ALL_MEMBER_TRAITS(2,ns::TemplatedStruct,aT1,aT2)

using namespace jsoncons; // for convenience

int main()
{
    using value_type = ns::TemplatedStruct<int,std::wstring>;

    value_type val{1, L"sss"};

    std::wstring s;
    encode_json(val, s);

    auto val2 = decode_json<value_type>(s);
    assert(val2 == val);
}
```

<div id="G8"/>

#### An example using JSONCONS_ENUM_TRAITS and JSONCONS_ALL_CTOR_GETTER_TRAITS

This example makes use of the convenience macros `JSONCONS_ENUM_TRAITS`
and `JSONCONS_ALL_CTOR_GETTER_TRAITS` to specialize the 
[json_type_traits](ref/json_type_traits.md) for the enum type
`ns::hiking_experience` and the classes `ns::hiking_reputon` and 
`ns::hiking_reputation`.
The macro `JSONCONS_ENUM_TRAITS` generates the code from
the enum values, and the macro `JSONCONS_ALL_CTOR_GETTER_TRAITS` 
generates the code from the get functions and a constructor. 
These macro declarations must be placed outside any namespace blocks.

```cpp
namespace ns {
    enum class hiking_experience {beginner,intermediate,advanced};

    class hiking_reputon
    {
        std::string rater_;
        hiking_experience assertion_;
        std::string rated_;
        double rating_;
        std::optional<std::chrono::seconds> generated_; // assumes C++17, if not use jsoncons::optional
        std::optional<std::chrono::seconds> expires_;
    public:
        hiking_reputon(const std::string& rater,
                       hiking_experience assertion,
                       const std::string& rated,
                       double rating,
                       const std::optional<std::chrono::seconds>& generated = std::optional<std::chrono::seconds>(),
                       const std::optional<std::chrono::seconds>& expires = std::optional<std::chrono::seconds>())
            : rater_(rater), assertion_(assertion), rated_(rated), rating_(rating),
              generated_(generated), expires_(expires)
        {
        }

        const std::string& rater() const {return rater_;}
        hiking_experience assertion() const {return assertion_;}
        const std::string& rated() const {return rated_;}
        double rating() const {return rating_;}
        std::optional<std::chrono::seconds> generated() const {return generated_;}
        std::optional<std::chrono::seconds> expires() const {return expires_;}

        friend bool operator==(const hiking_reputon& lhs, const hiking_reputon& rhs)
        {
            return lhs.rater_ == rhs.rater_ && lhs.assertion_ == rhs.assertion_ && 
                   lhs.rated_ == rhs.rated_ && lhs.rating_ == rhs.rating_ &&
                   lhs.confidence_ == rhs.confidence_ && lhs.expires_ == rhs.expires_;
        }

        friend bool operator!=(const hiking_reputon& lhs, const hiking_reputon& rhs)
        {
            return !(lhs == rhs);
        };
    };

    class hiking_reputation
    {
        std::string application_;
        std::vector<hiking_reputon> reputons_;
    public:
        hiking_reputation(const std::string& application, 
                          const std::vector<hiking_reputon>& reputons)
            : application_(application), 
              reputons_(reputons)
        {}

        const std::string& application() const { return application_;}
        const std::vector<hiking_reputon>& reputons() const { return reputons_;}
    };

} // namespace ns

// Declare the traits. Specify which data members need to be serialized.

JSONCONS_ENUM_TRAITS(ns::hiking_experience, beginner, intermediate, advanced)
// First four members listed are mandatory, generated and expires are optional
JSONCONS_N_CTOR_GETTER_TRAITS(ns::hiking_reputon, 4, rater, assertion, rated, rating, 
                              generated, expires)

// All members are mandatory
JSONCONS_ALL_CTOR_GETTER_TRAITS(ns::hiking_reputation, application, reputons)

int main()
{
    // Decode the string of data into a c++ structure
    ns::hiking_reputation v = decode_json<ns::hiking_reputation>(data);

    // Iterate over reputons array value
    std::cout << "(1)\n";
    for (const auto& item : v.reputons())
    {
        std::cout << item.rated() << ", " << item.rating();
        if (item.generated())
        {
            std::cout << ", " << (*item.generated()).count();
        }
        std::cout << "\n";
    }

    // Encode the c++ structure into a string
    std::string s;
    encode_json(v, s, indenting::indent);
    std::cout << "(2)\n";
    std::cout << s << "\n";
}
```
Output:
```
(1)
Marilyn C, 0.9, 1514862245
(2)
{
    "application": "hiking",
    "reputons": [
        {
            "assertion": "advanced",
            "generated": 1514862245,
            "rated": "Marilyn C",
            "rater": "HikingAsylum",
            "rating": 0.9
        }
    ]
}
```

<div id="G9"/>

#### Serialize a polymorphic type based on the presence of members

This example uses the convenience macro `JSONCONS_N_CTOR_GETTER_TRAITS`
to generate the [json_type_traits](ref/json_type_traits.md) boilerplate for the `HourlyEmployee` and `CommissionedEmployee` 
derived classes, and `JSONCONS_POLYMORPHIC_TRAITS` to generate the `json_type_traits` boilerplate
for `std::shared_ptr<Employee>` and `std::unique_ptr<Employee>`. The type selection strategy is based
on the presence of mandatory members, in particular, to the `firstName`, `lastName`, and `wage` members of an
`HourlyEmployee`, and to the `firstName`, `lastName`, `baseSalary`, and `commission` members of a `CommissionedEmployee`.
Non-mandatory members are not considered for the purpose of type selection.

```cpp
#include <cassert>
#include <iostream>
#include <vector>
#include <jsoncons/json.hpp>

using namespace jsoncons;

namespace ns {

    class Employee
    {
        std::string firstName_;
        std::string lastName_;
    public:
        Employee(const std::string& firstName, const std::string& lastName)
            : firstName_(firstName), lastName_(lastName)
        {
        }
        virtual ~Employee() noexcept = default;

        virtual double calculatePay() const = 0;

        const std::string& firstName() const {return firstName_;}
        const std::string& lastName() const {return lastName_;}
    };

    class HourlyEmployee : public Employee
    {
        double wage_;
        unsigned hours_;
    public:
        HourlyEmployee(const std::string& firstName, const std::string& lastName, 
                       double wage, unsigned hours)
            : Employee(firstName, lastName), 
              wage_(wage), hours_(hours)
        {
        }

        double wage() const {return wage_;}

        unsigned hours() const {return hours_;}

        double calculatePay() const override
        {
            return wage_*hours_;
        }
    };

    class CommissionedEmployee : public Employee
    {
        double baseSalary_;
        double commission_;
        unsigned sales_;
    public:
        CommissionedEmployee(const std::string& firstName, const std::string& lastName, 
                             double baseSalary, double commission, unsigned sales)
            : Employee(firstName, lastName), 
              baseSalary_(baseSalary), commission_(commission), sales_(sales)
        {
        }

        double baseSalary() const
        {
            return baseSalary_;
        }

        double commission() const
        {
            return commission_;
        }

        unsigned sales() const
        {
            return sales_;
        }

        double calculatePay() const override
        {
            return baseSalary_ + commission_*sales_;
        }
    };

} // namespace ns

JSONCONS_N_CTOR_GETTER_TRAITS(ns::HourlyEmployee, 3, firstName, lastName, wage, hours)
JSONCONS_N_CTOR_GETTER_TRAITS(ns::CommissionedEmployee, 4, firstName, lastName, baseSalary, commission, sales)
JSONCONS_POLYMORPHIC_TRAITS(ns::Employee, ns::HourlyEmployee, ns::CommissionedEmployee)

int main()
{
    std::string input = R"(
[
    {
        "firstName": "John",
        "hours": 1000,
        "lastName": "Smith",
        "wage": 40.0
    },
    {
        "baseSalary": 30000.0,
        "commission": 0.25,
        "firstName": "Jane",
        "lastName": "Doe",
        "sales": 1000
    }
]
    )"; 

    auto v = decode_json<std::vector<std::unique_ptr<ns::Employee>>>(input);

    std::cout << "(1)\n";
    for (const auto& p : v)
    {
        std::cout << p->firstName() << " " << p->lastName() << ", " << p->calculatePay() << "\n";
    }

    std::cout << "\n(2)\n";
    encode_json(v, std::cout, indenting::indent);

    std::cout << "\n\n(3)\n";
    json j(v);
    std::cout << pretty_print(j) << "\n\n";
}
```
Output:
```
(1)
John Smith, 40000
Jane Doe, 30250

(2)
[
    {
        "firstName": "John",
        "hours": 1000,
        "lastName": "Smith",
        "wage": 40.0
    },
    {
        "baseSalary": 30000.0,
        "commission": 0.25,
        "firstName": "Jane",
        "lastName": "Doe",
        "sales": 1000
    }
]

(3)
[
    {
        "firstName": "John",
        "hours": 1000,
        "lastName": "Smith",
        "wage": 40.0
    },
    {
        "baseSalary": 30000.0,
        "commission": 0.25,
        "firstName": "Jane",
        "lastName": "Doe",
        "sales": 1000
    }
]
```

<div id="G10"/>

#### Ensuring type selection is possible

When deserializing a polymorphic type, jsoncons needs to know how
to convert a json value to the proper derived class. In the Employee
example above, the type selection strategy is based
on the presence of mandatory members in the derived classes. If
derived classes cannot be distinguished in this way, 
you can introduce extra members. The convenience
macros `JSONCONS_N_MEMBER_TRAITS`, `JSONCONS_ALL_MEMBER_TRAITS`,
`JSONCONS_TPL_N_MEMBER_TRAITS`, `JSONCONS_TPL_ALL_MEMBER_TRAITS`,
`JSONCONS_N_MEMBER_NAME_TRAITS`, `JSONCONS_ALL_MEMBER_NAME_TRAITS`,
`JSONCONS_TPL_N_MEMBER_NAME_TRAITS`, and `JSONCONS_TPL_ALL_MEMBER_NAME_TRAITS`
allow you to have `const` or `static const` data members that are serialized and that 
particpate in the type selection strategy during deserialization. 

```cpp
namespace ns {

class Foo
{
public:
    virtual ~Foo() noexcept = default;
};

class Bar : public Foo
{
    static const bool bar = true;
    JSONCONS_TYPE_TRAITS_FRIEND
};

class Baz : public Foo 
{
    static const bool baz = true;
    JSONCONS_TYPE_TRAITS_FRIEND
};

} // ns

JSONCONS_N_MEMBER_TRAITS(ns::Bar,1,bar)
JSONCONS_N_MEMBER_TRAITS(ns::Baz,1,baz)
JSONCONS_POLYMORPHIC_TRAITS(ns::Foo, ns::Bar, ns::Baz)

int main()
{
    std::vector<std::unique_ptr<ns::Foo>> u;
    u.emplace_back(new ns::Bar());
    u.emplace_back(new ns::Baz());

    std::string buffer;
    encode_json(u, buffer);
    std::cout << "(1)\n" << buffer << "\n\n";

    auto v = decode_json<std::vector<std::unique_ptr<ns::Foo>>>(buffer);

    std::cout << "(2)\n";
    for (const auto& ptr : v)
    {
        if (dynamic_cast<ns::Bar*>(ptr.get()))
        {
            std::cout << "A bar\n";
        }
        else if (dynamic_cast<ns::Baz*>(ptr.get()))
        {
            std::cout << "A baz\n";
        } 
    }
}
```

Output:
```
(1)
[{"bar":true},{"baz":true}]

(2)
A bar
A baz
```

<div id="G14"/>

#### Decode to a polymorphic type based on a type marker (since 0.157.0)

```cpp
namespace ns {

    class Shape
    {
    public:
        virtual ~Shape() = default;
        virtual double area() const = 0;
    };
      
    class Rectangle : public Shape
    {
        double height_;
        double width_;
    public:
        Rectangle(double height, double width)
            : height_(height), width_(width)
        {
        }

        const std::string& type() const
        {
            static const std::string type_ = "rectangle"; 
            return type_;
        }

        double height() const
        {
            return height_;
        }

        double width() const
        {
            return width_;
        }

        double area() const override
        {
            return height_ * width_;
        }
    };

    class Triangle : public Shape
    { 
        double height_;
        double width_;

    public:
        Triangle(double height, double width)
            : height_(height), width_(width)
        {
        }

        const std::string& type() const
        {
            static const std::string type_ = "triangle"; 
            return type_;
        }

        double height() const
        {
            return height_;
        }

        double width() const
        {
            return width_;
        }

        double area() const override
        {
            return (height_ * width_)/2.0;
        }
    };                 

    class Circle : public Shape
    { 
        double radius_;

    public:
        Circle(double radius)
            : radius_(radius)
        {
        }

        const std::string& type() const
        {
            static const std::string type_ = "circle"; 
            return type_;
        }

        double radius() const
        {
            return radius_;
        }

        double area() const override
        {
            constexpr double pi = 3.14159265358979323846;
            return pi*radius_*radius_;
        }
    };                 

} // ns

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Rectangle,
    (type,"type",JSONCONS_RDONLY,[](const std::string& type) noexcept{return type == "rectangle";}),
    (height, "height"),
    (width, "width")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Triangle,
    (type,"type", JSONCONS_RDONLY, [](const std::string& type) noexcept {return type == "triangle";}),
    (height, "height"),
    (width, "width")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Circle,
    (type,"type", JSONCONS_RDONLY, [](const std::string& type) noexcept {return type == "circle";}),
    (radius, "radius")
)

JSONCONS_POLYMORPHIC_TRAITS(ns::Shape,ns::Rectangle,ns::Triangle,ns::Circle)

int main()
{
    std::string input = R"(
[
    {"type" : "rectangle", "width" : 2.0, "height" : 1.5 },
    {"type" : "triangle", "width" : 4.0, "height" : 2.0 },
    {"type" : "circle", "radius" : 1.0 }
]
    )";

    auto shapes = jsoncons::decode_json<std::vector<std::unique_ptr<ns::Shape>>>(input);

    std::cout << "(1)\n";
    for (const auto& shape : shapes)
    {
        std::cout << typeid(*shape.get()).name() << " area: " << shape->area() << "\n";
    }

    std::string output;

    jsoncons::encode_json(shapes, output, indenting::indent);
    std::cout << "\n(2)\n" << output << "\n";
}
```

Output:
```
(1)
class `ns::Rectangle area: 3.0000000
class `ns::Triangle area: 4.0000000
class `ns::Circle area: 3.1415927

(2)
[
    {
        "height": 1.5,
        "type": "rectangle",
        "width": 2.0
    },
    {
        "height": 2.0,
        "type": "triangle",
        "width": 4.0
    },
    {
        "radius": 1.0,
        "type": "circle"
    }
]
```

This example maps a `type()` getter to a "type" data member in the JSON.
However, we can also achieve this without using a `type()` getter at all. 
Compare with the very similar example [decode to a std::variant based on a type marker](#G15)

<div id="G11"/>

#### An example with std::variant

This example assumes C++17 language support and jsoncons v0.154.0 or later.

```cpp
#include <jsoncons/json.hpp>

namespace ns {

    enum class Color {yellow, red, green, blue};

    inline
    std::ostream& operator<<(std::ostream& os, Color val)
    {
        switch (val)
        {
            case Color::yellow: os << "yellow"; break;
            case Color::red: os << "red"; break;
            case Color::green: os << "green"; break;
            case Color::blue: os << "blue"; break;
        }
        return os;
    }

    class Fruit 
    {
    private:
        JSONCONS_TYPE_TRAITS_FRIEND
        std::string name_;
        Color color_;
    public:
        friend std::ostream& operator<<(std::ostream& os, const Fruit& val)
        {
            os << "name: " << val.name_ << ", color: " << val.color_ << "\n";
            return os;
        }
    };

    class Fabric 
    {
    private:
      JSONCONS_TYPE_TRAITS_FRIEND
      int size_;
      std::string material_;
    public:
        friend std::ostream& operator<<(std::ostream& os, const Fabric& val)
        {
            os << "size: " << val.size_ << ", material: " << val.material_ << "\n";
            return os;
        }
    };

    class Basket {
     private:
      JSONCONS_TYPE_TRAITS_FRIEND
      std::string owner_;
      std::vector<std::variant<Fruit, Fabric>> items_;

    public:
        std::string owner() const
        {
            return owner_;
        }

        std::vector<std::variant<Fruit, Fabric>> items() const
        {
            return items_;
        }
    };

} // ns
} // namespace

JSONCONS_ENUM_NAME_TRAITS(ns::Color, (yellow, "YELLOW"), (red, "RED"), (green, "GREEN"), (blue, "BLUE"))

JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Fruit,
                                (name_, "name"),
                                (color_, "color"))
JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Fabric,
                                (size_, "size"),
                                (material_, "material"))
JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Basket,
                                (owner_, "owner"),
                                (items_, "items"))

int main()
{
    std::string input = R"(
{
  "owner": "Rodrigo",
  "items": [
    {
      "name": "banana",
      "color": "YELLOW"
    },
    {
      "size": 40,
      "material": "wool"
    },
    {
      "name": "apple",
      "color": "RED"
    },
    {
      "size": 40,
      "material": "cotton"
    }
  ]
}
    )";

    ns::Basket basket = jsoncons::decode_json<ns::Basket>(input);
    std::cout << basket.owner() << "\n\n";

    std::cout << "(1)\n";
    for (const auto& var : basket.items()) 
    {
        std::visit([](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, ns::Fruit>)
                std::cout << "Fruit " << arg << '\n';
            else if constexpr (std::is_same_v<T, ns::Fabric>)
                std::cout << "Fabric " << arg << '\n';
        }, var);
    }

    std::string output;
    jsoncons::encode_json(basket, output, indenting::indent);
    std::cout << "(2)\n" << output << "\n\n";
}
```
Output:
```
Rodrigo

(1)
Fruit name: banana, color: yellow

Fabric size: 28, material: wool

Fruit name: apple, color: red

Fabric size: 28, material: cotton

(2)
{
    "items": [
        {
            "color": "YELLOW",
            "name": "banana"
        },
        {
            "material": "wool",
            "size": 40
        },
        {
            "color": "RED",
            "name": "apple"
        },
        {
            "material": "cotton",
            "size": 40
        }
    ],
    "owner": "Rodrigo"
}
```

<div id="G12"/>

#### Type selection and std::variant

For classes supported through the convenience macros, e.g. `Fruit` and `Fabric` from the previous example, 
the type selection strategy is the same as for polymorphic types, and is based 
on the presence of mandatory members in the classes. More generally, 
the type selection strategy is based on the `json_type_traits<Json,T>::is(const Json& j)` 
function, checking each type in the variant from left to right, and stopping when 
`json_type_traits<Json,T>::is(j)` returns `true`. 

Now consider 

```cpp
#include <jsoncons/json.hpp>

namespace ns {

    enum class Color {yellow, red, green, blue};

} // ns

JSONCONS_ENUM_NAME_TRAITS(ns::Color, (yellow, "YELLOW"), (red, "RED"), (green, "GREEN"), (blue, "BLUE"))

int main()
{
    using variant_type  = std::variant<int, double, bool, std::string, ns::Color>;

    std::vector<variant_type> vars = {100, 10.1, false, std::string("Hello World"), ns::Color::yellow};

    std::string buffer;
    jsoncons::encode_json(vars, buffer, indenting::indent);

    std::cout << "(1)\n" << buffer << "\n\n";

    auto vars2 = jsoncons::decode_json<std::vector<variant_type>>(buffer);

    auto visitor = [](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, int>)
                std::cout << "int " << arg << '\n';
            else if constexpr (std::is_same_v<T, double>)
                std::cout << "double " << arg << '\n';
            else if constexpr (std::is_same_v<T, bool>)
                std::cout << "bool " << arg << '\n';
            else if constexpr (std::is_same_v<T, std::string>)
                std::cout << "std::string " << arg << '\n';
            else if constexpr (std::is_same_v<T, ns::Color>)
                std::cout << "ns::Color " << arg << '\n';
        };

    std::cout << "(2)\n";
    for (const auto& item : vars2)
    {
        std::visit(visitor, item);
    }
    std::cout << "\n";
}
```
Output:
```
(1)
[
    100,
    10.1,
    false,
    "Hello World",
    "YELLOW"
]

(2)
int 100
double 10.1
bool false
std::string Hello World
std::string YELLOW
```

Encode is fine. But when decoding, jsoncons checks if the JSON string "YELLOW" is a `std::string` 
before it checks whether it is an `ns::Color`, and since the answer is yes, 
it is stored in the variant as a `std::string`.

But if we switch the order of `ns::Color` and `std::string` in the variant definition, viz.

```cpp
 using variant_type  = std::variant<int, double, bool, ns::Color, std::string>;
```
strings containing  the text "YELLOW", "RED", "GREEN", or "BLUE" are detected to be `ns::Color`, and the others `std::string`.  

And the output becomes
```
(1)
[
    100,
    10.1,
    false,
    "Hello World",
    "YELLOW"
]

(2)
int 100
double 10.1
bool false
std::string Hello World
ns::Color yellow
```

So: types that are more constrained should appear to the left of types that are less constrained.

<div id="G15"/>

#### Decode to a std::variant based on a type marker (since 0.158.0)

This example is very similar to [decode to a polymorphic type based on a type marker](#G14),
and in fact the json traits defined for that example would do for `std::variant` as well.
But here we add a wrinkle by omitting the `type()` function in the `Rectangle`, `Triangle` and
`Circle` classes. More generally, we show how to augment the JSON output with name/value pairs 
that are not present in the class definitions, and to perform type selection with them.

```cpp
#include <jsoncons/json.hpp>

namespace ns {

    class Rectangle
    {
        double height_;
        double width_;
    public:
        Rectangle(double height, double width)
            : height_(height), width_(width)
        {
        }

        double height() const
        {
            return height_;
        }

        double width() const
        {
            return width_;
        }

        double area() const
        {
            return height_ * width_;
        }
    };

    class Triangle
    { 
        double height_;
        double width_;

    public:
        Triangle(double height, double width)
            : height_(height), width_(width)
        {
        }

        double height() const
        {
            return height_;
        }

        double width() const
        {
            return width_;
        }

        double area() const
        {
            return (height_ * width_)/2.0;
        }
    };                 

    class Circle
    { 
        double radius_;

    public:
        Circle(double radius)
            : radius_(radius)
        {
        }

        double radius() const
        {
            return radius_;
        }

        double area() const
        {
            constexpr double pi = 3.14159265358979323846;
            return pi*radius_*radius_;
        }
    };                 

    inline constexpr auto rectangle_marker = [](double) noexcept {return "rectangle"; };
    inline constexpr auto triangle_marker = [](double) noexcept {return "triangle";};
    inline constexpr auto circle_marker = [](double) noexcept {return "circle";};

} // namespace ns

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Rectangle,
    (height,"type",JSONCONS_RDONLY,
     [](const std::string& type) noexcept{return type == "rectangle";},
     ns::rectangle_marker),
    (height, "height"),
    (width, "width")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Triangle,
    (height,"type", JSONCONS_RDONLY, 
     [](const std::string& type) noexcept {return type == "triangle";},
     ns::triangle_marker),
    (height, "height"),
    (width, "width")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Circle,
    (radius,"type", JSONCONS_RDONLY, 
     [](const std::string& type) noexcept {return type == "circle";},
     ns::circle_marker),
    (radius, "radius")
)

int main()
{
    using shapes_t = std::variant<ns::Rectangle,ns::Triangle,ns::Circle>;

    std::string input = R"(
[
    {"type" : "rectangle", "width" : 2.0, "height" : 1.5 },
    {"type" : "triangle", "width" : 4.0, "height" : 2.0 },
    {"type" : "circle", "radius" : 1.0 }
]
    )";

    auto shapes = jsoncons::decode_json<std::vector<shapes_t>>(input);

    auto visitor = [](auto&& shape) {
        using T = std::decay_t<decltype(shape)>;
        if constexpr (std::is_same_v<T, ns::Rectangle>)
            std::cout << "rectangle area: " << shape.area() << '\n';
        else if constexpr (std::is_same_v<T, ns::Triangle>)
            std::cout << "triangle area: " << shape.area() << '\n';
        else if constexpr (std::is_same_v<T, ns::Circle>)
            std::cout << "circle area: " << shape.area() << '\n';
    };

    std::cout << "(1)\n";
    for (const auto& shape : shapes)
    {
        std::visit(visitor, shape);
    }

    std::string output;
    jsoncons::encode_json(shapes, output, indenting::indent);
    std::cout << "\n(2)\n" << output << "\n";
}
```
Output:
```
(1)
rectangle area: 3.0000000
triangle area: 4.0000000
circle area: 3.1415927

(2)
[
    {
        "height": 1.5,
        "type": "rectangle",
        "width": 2.0
    },
    {
        "height": 2.0,
        "type": "triangle",
        "width": 4.0
    },
    {
        "radius": 1.0,
        "type": "circle"
    }
]
```

Note the mapping to the "type" member, in particular, for the rectangle,

```cpp
(height,"type",JSONCONS_RDONLY,
 [](const std::string& type) noexcept{return type == "rectangle";},
 ns::rectangle_marker),
```

There are two things to observe. First, the class member being mapped,
here `height`, can be any member, we don't actually use it. Instead,  
we use the function object `ns::rectangle_marker` to ouput the value
"rectangle" with the key "type". Second, the function argument in
this position cannot be a lambda expression (at least until C++20), 
because jsoncons uses it in an unevaluated context, so it is
provided as a variable containing a lambda expression instead.
See [convenience macros](ref/json_type_traits/convenience-macros.md)
for full details.   

<div id="G13"/>

#### Convert JSON numbers to/from boost multiprecision numbers

```
#include <jsoncons/json.hpp>
#include <boost/multiprecision/cpp_dec_float.hpp>

namespace jsoncons 
{
    template <typename Json,typename Backend>
    struct json_type_traits<Json,boost::multiprecision::number<Backend>>
    {
        using multiprecision_type = boost::multiprecision::number<Backend>;

        static bool is(const Json& val) noexcept
        {
            if (!(val.is_string() && val.semantic_tag() == semantic_tag::bigdec))
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        static multiprecision_type as(const Json& val)
        {
            return multiprecision_type(val.template as<std::string>());
        }

        static Json to_json(multiprecision_type val)
        {
            return Json(val.str(), semantic_tag::bigdec);
        }
    };
}

int main()
{
    typedef boost::multiprecision::number<boost::multiprecision::cpp_dec_float_50> multiprecision_type;

    std::string s = "[100000000000000000000000000000000.1234]";
    auto options = json_options{}
        .lossless_number(true);
    json j = json::parse(s, options);

    multiprecision_type x = j[0].as<multiprecision_type>();

    std::cout << "(1) " << std::setprecision(std::numeric_limits<multiprecision_type>::max_digits10)
        << x << "\n";

    json j2(json_array_arg, {x});
    std::cout << "(2) " << j2[0].as<std::string>() << "\n";
}
```
Output:
```
(1) 100000000000000000000000000000000.1234
(2) 100000000000000000000000000000000.1234
```

### Construct

<div id="C1"/>

#### Construct a json object

Start with an empty json object and insert some name-value pairs,

```cpp
json image_sizing;
image_sizing.insert_or_assign("Resize To Fit",true);  // a boolean 
image_sizing.insert_or_assign("Resize Unit", "pixels");  // a string
image_sizing.insert_or_assign("Resize What", "long_edge");  // a string
image_sizing.insert_or_assign("Dimension 1",9.84);  // a double
image_sizing.insert_or_assign("Dimension 2",json::null());  // a null value
```

or use an object initializer-list,

```cpp
json file_settings( json_object_arg,{
        {"Image Format", "JPEG"},
        {"Color Space", "sRGB"},
        {"Limit File Size", true},
        {"Limit File Size To", 10000}
    });
};
```

<div id="C3"/>

#### Insert a value into a location after creating objects when missing object keys

Suppose you have

```cpp
json j; // empty object

std::vector<std::string> keys = {"foo","bar","baz"}; // vector of keys
```
and wish to construct:
```json
{"foo":{"bar":{"baz":"str"}}}
```

You can accomplish this in a loop as follows:

```cpp
json* ptr = &j;

for (const auto& item : keys)
{
    auto r = ptr->try_emplace(item, json());
    ptr = std::addressof(r.first->value());
}
*ptr = "str";
```

Since 0.162.0, you can also accomplish this with [jsonpointer::add](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/jsonpointer/add.md):

```cpp
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

using jsoncons::json;
namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    std::vector<std::string> keys = {"foo","bar","baz"};

    jsonpointer::json_pointer location;
    for (const auto& key : keys)
    {
        location /= key;
    }

    json j;
    jsonpointer::add(j, location, "str", true); // create_if_missing set to true

    std::cout << pretty_print(j) << "\n\n";
}
```

<div id="C2"/>

#### Construct a json array

```cpp
json color_spaces(json_array_arg); // an empty array
color_spaces.push_back("sRGB");
color_spaces.push_back("AdobeRGB");
color_spaces.push_back("ProPhoto RGB");
```

or use an array initializer-list,
```cpp
json image_formats(json_array_arg, {"JPEG","PSD","TIFF","DNG"});
```

<div id="C6"/>

#### Construct a json byte string

```cpp
#include <jsoncons/json.hpp>

namespace jc=jsoncons;

int main()
{
    std::vector<uint8_t> bytes = {'H','e','l','l','o'};

    // default suggested encoding (base64url)
    json j1(byte_string_arg, bytes);
    std::cout << "(1) "<< j1 << "\n\n";

    // base64 suggested encoding
    json j2(byte_string_arg, bytes, semantic_tag::base64);
    std::cout << "(2) "<< j2 << "\n\n";

    // base16 suggested encoding
    json j3(byte_string_arg, bytes, semantic_tag::base16);
    std::cout << "(3) "<< j3 << "\n\n";
}
```

Output:
```
(1) "SGVsbG8"

(2) "SGVsbG8="

(3) "48656C6C6F"
```
<div id="C7"/>

#### Construct multidimensional json arrays

Construct a 3-dimensional 4 x 3 x 2 json array with all elements initialized to 0.0:

```cpp
json j = json::make_array<3>(4, 3, 2, 0.0);
double val = 1.0;
for (std::size_t i = 0; i < a.size(); ++i)
{
    for (std::size_t j = 0; j < j[i].size(); ++j)
    {
        for (std::size_t k = 0; k < j[i][j].size(); ++k)
        {
            j[i][j][k] = val;
            val += 1.0;
        }
    }
}
std::cout << pretty_print(j) << '\n';
```
Output:
```json
[
    [
        [1.0,2.0],
        [3.0,4.0],
        [5.0,6.0]
    ],
    [
        [7.0,8.0],
        [9.0,10.0],
        [11.0,12.0]
    ],
    [
        [13.0,14.0],
        [15.0,16.0],
        [17.0,18.0]
    ],
    [
        [19.0,20.0],
        [21.0,22.0],
        [23.0,24.0]
    ]
]
```
<div id="C8"/>

#### Construct a json array that contains non-owning references to other json values (since 0.156.0)

```cpp
#include <jsoncons/json.hpp>
#include <iostream>

int main()
{
    std::string input = R"(
    {
      "machines": [
        {"id": 1, "state": "running"},
        {"id": 2, "state": "stopped"},
        {"id": 3, "state": "running"}
      ]
    }        
    )";

    json j = json::parse(input);

    json j_v(json_array_arg);
    for (const auto& item : j.at("machines").array_range())
    {
        if (item.at("state").as<std::string>() == "running")
        {
            j_v.emplace_back(json_const_pointer_arg, &item);
        }
    }

    std::cout << "\n(1)\n" << pretty_print(j_v) << "\n\n";

    for (const auto& item : j_v.array_range())
    {
        std::cout << "json type: " << item.type() << ", storage kind: " << item.storage() << "\n";
    }

    json j2 = deep_copy(j_v);

    std::cout << "\n(2)\n" << pretty_print(j2) << "\n\n";

    for (const auto& item : j2.array_range())
    {
        std::cout << "json type: " << item.type() << ", storage kind: " << item.storage() << "\n";
    }
}

```
Output:
```json
(1)
[
    {
        "id": 1,
        "state": "running"
    },
    {
        "id": 3,
        "state": "running"
    }
]

json type: object, storage kind: json const pointer
json type: object, storage kind: json const pointer

(2)
[
    {
        "id": 1,
        "state": "running"
    },
    {
        "id": 3,
        "state": "running"
    }
]

json type: object, storage kind: object
json type: object, storage kind: object
```

### Access

<div id="E1"/>

#### Use string_view to access the actual memory that's being used to hold a string

You can use `j.as<jsoncons::string_view>()`, e.g.
```cpp
json j = json::parse("\"Hello World\"");
auto sv = j.as<jsoncons::string_view>();
```
`jsoncons::string_view` supports the member functions of `std::string_view`, including `data()` and `size()`. 

If your compiler supports `std::string_view`, you can also use `j.as<std::string_view>()`.

<div id="E2"/>

#### Given a string in a json object that represents a decimal number, assign it to a double

```cpp
json j(json_objectarg, {
    {"price", "25.17"}
});

double price = j["price"].as<double>();
```

<div id="E3"/>
 
#### Retrieve a big integer that's been parsed as a string

If an integer exceeds the range of an `int64_t` or `uint64_t`, jsoncons parses it as a string 
with semantic tagging `bigint`.

```cpp
#include <jsoncons/json.hpp>
#include <iostream>
#include <iomanip>

using jsoncons::json;

int main()
{
    std::string input = "-18446744073709551617";

    json j = json::parse(input);

    // Access as string
    std::string s = j.as<std::string>();
    std::cout << "(1) " << s << "\n\n";

    // Access as double
    double d = j.as<double>();
    std::cout << "(2) " << std::setprecision(17) << d << "\n\n";

    // Access as jsoncons::bigint
    jsoncons::bigint bn = j.as<jsoncons::bigint>();
    std::cout << "(3) " << bn << "\n\n";

    // If your compiler supports extended integer types 
    __int128 i = j.as<__int128>();
    std::cout << "(4) " << i << "\n\n";
}
```
Output:
```
(1) -18446744073709551617

(2) -1.8446744073709552e+19

(3) -18446744073709551617

(4) -18446744073709551617
```

<div id="E4"/>

#### Look up a key, if found, return the value converted to type T, otherwise, return a default value of type T.
 
```cpp
json j(json_object_arg, {{"price", "25.17"}});

double price = j.get_value_or<double>("price", 25.00); // returns 25.17

double sale_price = j.get_value_or<double>("sale_price", 22.0); // returns 22.0
```

<div id="E5"/>
 
#### Retrieve a value in a hierarchy of JSON objects

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

int main()
{
    json j = json::parse(R"(
    {
       "application": "hiking",
       "reputons": [
       {
           "rater": "HikingAsylum",
           "assertion": "advanced",
           "rated": "Marilyn C",
           "rating": 0.90
         }
       ]
    }
    )");

    // Using index or `at` accessors
    std::string result1 = j["reputons"][0]["rated"].as<std::string>();
    std::cout << "(1) " << result1 << '\n';
    std::string result2 = j.at("reputons").at(0).at("rated").as<std::string>();
    std::cout << "(2) " << result2 << '\n';

    // Using JSON Pointer
    std::string result3 = jsonpointer::get(j, "/reputons/0/rated").as<std::string>();
    std::cout << "(3) " << result3 << '\n';

    // Using JSONPath
    json result4 = jsonpath::json_query(j, "$.reputons.0.rated");
    if (result4.size() > 0)
    {
        std::cout << "(4) " << result4[0].as<std::string>() << '\n';
    }
    json result5 = jsonpath::json_query(j, "$..0.rated");
    if (result5.size() > 0)
    {
        std::cout << "(5) " << result5[0].as<std::string>() << '\n';
    }
}
```

<div id="E6"/>
 
#### Retrieve a json value as a byte string

```cpp
#include <jsoncons/json.hpp>

namespace jc=jsoncons;

int main()
{
    json j;
    j["ByteString"] = json(byte_string_arg, std::vector<uint8_t>{ 'H','e','l','l','o' });
    j["EncodedByteString"] = json("SGVsbG8=", semantic_tag::base64);

    std::cout << "(1)\n";
    std::cout << pretty_print(j) << "\n\n";

    // Retrieve a byte string as a std::vector<uint8_t>
    std::vector<uint8_t> v = j["ByteString"].as<std::vector<uint8_t>>();

    // Retrieve a byte string from a text string containing base64 character values
    byte_string bytes2 = j["EncodedByteString"].as<byte_string>();
    std::cout << "(2) " << bytes2 << "\n\n";

    // Retrieve a byte string view  to access the memory that's holding the byte string
    byte_string_view bsv3 = j["ByteString"].as<byte_string_view>();
    std::cout << "(3) " << bsv3 << "\n\n";

    // Can't retrieve a byte string view of a text string 
    try
    {
        byte_string_view bsv4 = j["EncodedByteString"].as<byte_string_view>();
    }
    catch (const std::exception& e)
    {
        std::cout << "(4) "<< e.what() << "\n\n";
    }
}
```
Output:
```
(1)
{
    "ByteString": "SGVsbG8",
    "EncodedByteString": "SGVsbG8="
}

(2) 48 65 6c 6c 6f

(3) 48 65 6c 6c 6f

(4) Not a byte string
```

### Iterate

<div id="D1"/>

#### Iterate over a json array

```cpp
json j(json_array_arg, {1,2,3,4});

for (auto val : j.array_range())
{
    std::cout << val << '\n';
}
```

<div id="D2"/>

#### Iterate over a json object

```cpp
json j(json_object_arg, {
    {"author", "Haruki Murakami"},
    {"title", "Kafka on the Shore"},
    {"price", 25.17}
});

for (const auto& member : j.object_range())
{
    std::cout << member.key() << " => " << member.value() << '\n';
}

// or, since 0.176.0, using C++ 17 structured binding
for (const auto& [key, value] : j.object_range())
{
    std::cout << key << " => " << value << '\n';
}
```

### Modify

<div id="J1"/>

#### Insert a new value in an array at a specific position

```cpp
json cities(json_array_arg); // an empty array
cities.push_back("Toronto");  
cities.push_back("Vancouver");
// Insert "Montreal" at beginning of array
cities.insert(cities.array_range().begin(),"Montreal");  

std::cout << cities << '\n';
```
Output:
```
["Montreal","Toronto","Vancouver"]
```

<div id="J2"/>

#### Merge two json objects

[json::merge](ref/json/merge.md) inserts another json object's key-value pairs into a json object,
unless they already exist with an equivalent key.

[json::merge_or_update](ref/json/merge_or_update.md) inserts another json object's key-value pairs 
into a json object, or assigns them if they already exist.

The `merge` and `merge_or_update` functions perform only a one-level-deep shallow merge,
not a deep merge of nested objects.

```cpp
json another = json::parse(R"(
{
    "a" : "2",
    "c" : [4,5,6]
}
)");

json j = json::parse(R"(
{
    "a" : "1",
    "b" : [1,2,3]
}
)");

j.merge(std::move(another));
std::cout << pretty_print(j) << '\n';
```
Output:
```json
{
    "a": "1",
    "b": [1,2,3],
    "c": [4,5,6]
}
```

<div id="J3"/>

#### Erase an object with a specified key from an array

```cpp
int main()
{
    std::string input = R"(
[
    {
        "address": "ashdod",
        "email": "ron10@gmail.com",
        "first name": "ron",
        "id": "756746783",
        "last name": "cohen",
        "phone": "0526732996",
        "salary": 3000,
        "type": "manager"
    },
    {
        "address": "ashdod",
        "email": "nirlevy120@gmail.com",
        "first name": "nir",
        "id": "11884398",
        "last name": "levy",
        "phone": "0578198932",
        "salary": 4500,
        "type": "manager"
    }
]
    )";

    try
    {
        // Read from input 
        json instance = json::parse(input);
 
        // Locate the item to be erased
        auto it = std::find_if(instance.array_range().begin(), instance.array_range().end(), 
                               [](const json& item){return item.at("id") == "756746783";});
 
        // If found, erase it
        if (it != instance.array_range().end())
        {
            instance.erase(it);
        }

        std::cout << pretty_print(instance) << "\n\n";
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << '\n';    
    }
}
```
Output:
```json
[
    {
        "address": "ashdod",
        "email": "nirlevy120@gmail.com",
        "first name": "nir",
        "id": "11884398",
        "last name": "levy",
        "phone": "0578198932",
        "salary": 4500,
        "type": "manager"
    }
]
```

<div id="J4"/>

#### Iterating an array and erasing elements (since 0.168.6)

```cpp
#include <jsoncons/json.hpp>

using jsoncons::json;

int main()
{
    std::string input = R"(
        ["a","b","c","d","e","f"]
)";

    json j = json::parse(input);
    auto it = j.array_range().begin();
    while (it != j.array_range().end())
    {
        if (*it == "a" || *it == "c")
        {
            it = j.erase(it);
        }
        else
        {
            it++;
        }
    }

    std::cout << j << "\n\n";
}
```
Output:
```json
["b","d","e","f"]
```

<div id="J5"/>

#### Iterating an object and erasing members (since 0.168.6)

```cpp
#include <jsoncons/json.hpp>

using jsoncons::json;

int main()
{
    std::string input = R"(
        {"a":1, "b":2, "c":3, "d":4}
)";

    json j = json::parse(input);
    auto it = j.object_range().begin();
    while (it != j.object_range().end())
    {
        if (it->key() == "a" || it->key() == "c")
        {
            it = j.erase(it);
        }
        else
        {
            it++;
        }
    }

    std::cout << j << "\n\n";
}
```
Output:
```json
{"b":2,"d":4}
```

### Search and Replace
 
<div id="F1"/>

#### Search for and repace an object member key

You can rename object members with the built in filter [rename_object_key_filter](ref/rename_object_key_filter.md)

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

    // A filter can be passed to any function that takes a json_visitor ...
    std::cout << "(1) ";
    std::istringstream is(s);
    //json_reader reader(is, filter1);        // (until 0.164.0)
    json_stream_reader reader(is, filter1);   // (since 0.164.0)
    reader.read();
    std::cout << '\n';

    // or 
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
 
<div id="F2"/>

#### Search for and replace a value

You can use [json_replace](ref/jsonpath/json_replace.md) in the `jsonpath` extension

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

// For brevity
using jsoncons::json;
namespace jsonpath = jsoncons::jsonpath;
 
int main()
{
    std::string data = R"(
      { "books": [ 
          { "author": "Nigel Rees",
            "title": "Sayings of the Century",
            "isbn": "0048080489",
            "price": 8.95
          },
          { "author": "Evelyn Waugh",
            "title": "Sword of Honour",
            "isbn": "0141193557",
            "price": 12.99
          },
          { "author": "Herman Melville",
            "title": "Moby Dick",
            "isbn": "0553213113",
            "price": 8.99
          }
        ]
      }
    )";

    json j = json::parse(data);

    // Change the price of "Moby Dick" from $8.99 to $10
    jsonpath::json_replace(j,"$.books[?(@.isbn == '0553213113')].price",10.0);

    // Increase the price of "Sayings of the Century" by $1
    auto f = [](const std::string& /*location*/, json& value) 
    {
        value = value.as<double>() + 1.0;
    };
    jsonpath::json_replace(j, "$.books[?(@.isbn == '0048080489')].price", f); // (since 0.161.0)

    std::cout << pretty_print(j) << '\n';
}
```
Output:
```json
{
    "books": [
        {
            "author": "Nigel Rees",
            "isbn": "0048080489",
            "price": 9.95,
            "title": "Sayings of the Century"
        },
        {
            "author": "Evelyn Waugh",
            "isbn": "0141193557",
            "price": 12.99,
            "title": "Sword of Honour"
        },
        {
            "author": "Herman Melville",
            "isbn": "0553213113",
            "price": 10.0,
            "title": "Moby Dick"
        }
    ]
}
```

<div id="F3"/>

#### Update JSON in place

Suppose you have a JSON text, and need to replace one or more strings
found at a relative location path,  
but are not allowed to modify anything else in the original text.

```cpp
#include <jsoncons/json.hpp>

using namespace jsoncons;

class string_locator : public jsoncons::default_json_visitor
{
    char* data_;
    std::size_t length_;
    std::vector<std::string> path_;
    std::string from_;
    std::vector<std::string> current_;
    std::vector<std::size_t> positions_;
public:
    using jsoncons::default_json_visitor::string_view_type;

    string_locator(char* data, std::size_t length,
                   const std::vector<std::string>& path,
                   const std::string& from)
        : data_(data), length_(length),
          path_(path), from_(from)
    {
    }

    const std::vector<std::size_t>& positions() const
    {
        return positions_;
    }
private:
    bool visit_begin_object(semantic_tag, const ser_context&, std::error_code&) override
    {
        current_.emplace_back();
        return true;
    }

    bool visit_end_object(const ser_context&, std::error_code&) override
    {
        current_.pop_back();
        return true;
    }

    bool visit_key(const string_view_type& key, const ser_context&, std::error_code&) override
    {
        current_.back() = key;
        return true;
    }

    bool visit_string(const string_view_type& value,
                      jsoncons::semantic_tag,
                      const jsoncons::ser_context& context,
                      std::error_code&) override
    {
        if (path_.size() <= current_.size() && std::equal(path_.rbegin(), path_.rend(), current_.rbegin()))
        {
            if (value == from_)
            {
                positions_.push_back(context.position()+1); // one past quote character

            }
        }
        return true;
    }
};

void update_json_in_place(std::string& input,
                          const std::vector<std::string>& path,
                          const std::string& from,
                          const std::string& to)
{
    string_locator locator(input.data(), input.size(), path, from);
    //jsoncons::json_reader reader(input, locator);        // (until 0.164.0)
    jsoncons::json_string_reader reader(input, locator);   // (since 0.164.0)
    reader.read();

    for (auto it = locator.positions().rbegin(); it != locator.positions().rend(); ++it)
    {
        input.replace(*it, from.size(), to);
    }
}

int main()
{
    std::string input = R"(
{
    "Cola" : {"Type":"Drink", "Price": 10.99},"Water" : {"Type":"Drink"}, "Extra" : {"Cola" : {"Type":"Drink", "Price": 8.99}}
}
    )";

    try
    {
        std::cout << "(original)\n" << input << "\n";
        update_json_in_place(input, {"Cola", "Type"}, "Drink", "SoftDrink");

        std::cout << "(updated)\n" << input << "\n";
    }
    catch (std::exception& e)
    {
        std::cout << e.what() << "\n";
    }
}
```
Output:
```json
(original)

{
    "Cola" : {"Type":"Drink", "Price": 10.99},"Water" : {"Type":"Drink"}, "Extra" : {"Cola" : {"Type":"Drink", "Price": 8.99}}
}

(updated)

{
    "Cola" : {"Type":"SoftDrink", "Price": 10.99},"Water" : {"Type":"Drink"}, "Extra" : {"Cola" : {"Type":"SoftDrink", "Price": 8.99}}
}
```

### Flatten and unflatten
 
<div id="H1"/> 

#### Flatten a json object with numberish keys to JSON Pointer/value pairs

```cpp
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

// for brevity
using jsoncons::json; 
namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    json input = json::parse(R"(
    {
        "discards": {
            "1000": "Record does not exist",
            "1004": "Queue limit exceeded",
            "1010": "Discarding timed-out partial msg"
        },
        "warnings": {
            "0": "Phone number missing country code",
            "1": "State code missing",
            "2": "Zip code missing"
        }
    }
    )");

    json flattened = jsonpointer::flatten(input);
    std::cout << "(1)\n" << pretty_print(flattened) << "\n";

    json unflattened1 = jsonpointer::unflatten(flattened);
    std::cout << "(2)\n" << pretty_print(unflattened1) << "\n";

    json unflattened2 = jsonpointer::unflatten(flattened,
        jsonpointer::unflatten_options::assume_object);
    std::cout << "(3)\n" << pretty_print(unflattened2) << "\n";
}
```
Output:
```
(1)
{
    "/discards/1000": "Record does not exist",
    "/discards/1004": "Queue limit exceeded",
    "/discards/1010": "Discarding timed-out partial msg",
    "/warnings/0": "Phone number missing country code",
    "/warnings/1": "State code missing",
    "/warnings/2": "Zip code missing"
}
(2)
{
    "discards": {
        "1000": "Record does not exist",
        "1004": "Queue limit exceeded",
        "1010": "Discarding timed-out partial msg"
    },
    "warnings": ["Phone number missing country code", "State code missing", "Zip code missing"]
}
(3)
{
    "discards": {
        "1000": "Record does not exist",
        "1004": "Queue limit exceeded",
        "1010": "Discarding timed-out partial msg"
    },
    "warnings": {
        "0": "Phone number missing country code",
        "1": "State code missing",
        "2": "Zip code missing"
    }
}
```
 
Note that unflattening a json object of JSON Pointer-value pairs
has no unique solution. An integer appearing in a path could be an 
array index or it could be an object key.
 
<div id="H2"/> 

#### Flatten a json object to JSONPath/value pairs

```cpp
#include <iostream>
#include <cassert>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

// for brevity
using jsoncons::json; 
namespace jsonpath = jsoncons::jsonpath;

int main()
{
    json input = json::parse(R"(
    {
       "application": "hiking",
       "reputons": [
           {
               "rater": "HikingAsylum",
               "assertion": "advanced",
               "rated": "Marilyn C",
               "rating": 0.90
            },
            {
               "rater": "HikingAsylum",
               "assertion": "intermediate",
               "rated": "Hongmin",
               "rating": 0.75
            }    
        ]
    }
    )");

    json result = jsonpath::flatten(input);

    std::cout << pretty_print(result) << "\n";

    json original = jsonpath::unflatten(result);
    assert(original == input);
}
```
Output:
```
{
    "$['application']": "hiking",
    "$['reputons'][0]['assertion']": "advanced",
    "$['reputons'][0]['rated']": "Marilyn C",
    "$['reputons'][0]['rater']": "HikingAsylum",
    "$['reputons'][0]['rating']": 0.9,
    "$['reputons'][1]['assertion']": "intermediate",
    "$['reputons'][1]['rated']": "Hongmin",
    "$['reputons'][1]['rater']": "HikingAsylum",
    "$['reputons'][1]['rating']": 0.75
}
```

