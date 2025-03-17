# JSONCONS

jsoncons is a C++, header-only library for constructing [JSON](http://www.json.org) and JSON-like
data formats such as [CBOR](http://cbor.io/). For each supported data format, it enables you
to work with the data in a number of ways:

- As a variant-like, allocator-aware, data structure, [basic_json](doc/ref/corelib/basic_json.md) 

- As a strongly typed C++ data structure that implements [json_type_traits](doc/ref/corelib/json_type_traits.md)

- With cursor-level access to a stream of parse events, somewhat analogous to StAX pull parsing and push serializing
  in the XML world.

Compared to other JSON libraries, jsoncons has been designed to handle very large JSON texts. At its heart are
SAX-style parsers and serializers. It supports reading an entire JSON text in memory in a variant-like structure.
But it also supports efficient access to the underlying data using StAX-style pull parsing and push serializing.
And it supports incremental parsing into a user's preferred form, using
information about user types provided by specializations of [json_type_traits](doc/ref/corelib/json_type_traits.md).

The [jsoncons data model](doc/ref/corelib/data-model.md) supports the familiar JSON types - nulls,
booleans, numbers, strings, arrays, objects - plus byte strings. In addition, jsoncons 
supports semantic tagging of datetimes, epoch times, big integers, 
big decimals, big floats and binary encodings. This allows it to preserve these type semantics when parsing 
JSON-like data formats such as CBOR that have them.

jsoncons is distributed under the [Boost Software License](http://www.boost.org/users/license.html). 

jsoncons is free but welcomes support to sustain its development. If you find this library helpful, please consider making a [one time donation](https://paypal.me/jsoncons?locale.x=en_US)
or becoming a [:heart: sponsor](https://github.com/sponsors/danielaparker). 

As the `jsoncons` library has evolved, names have sometimes changed. To ease transition, jsoncons deprecates the 
old names but continues to support many of them. The deprecated names can be suppressed by defining the macro 
`JSONCONS_NO_DEPRECATED`, and doing so is recommended for new code.

## Extensions

- [bson](doc/ref/bson/bson.md) implements decode from and encode to the [Binary JSON](http://bsonspec.org/) data format.
- [cbor](doc/ref/cbor/cbor.md) implements decode from and encode to the IETF standard [Concise Binary Object Representation](http://cbor.io/) data format.
  In addition it supports tags for [stringref](http://cbor.schmorp.de/stringref) and tags for [typed arrays](https://tools.ietf.org/html/rfc8746). 
- [csv](doc/ref/csv/csv.md) implements decode from and encode to CSV files.
- [jmespath](doc/ref/jmespath/jmespath.md) implements [JMESPath](https://jmespath.org/), a query language for transforming JSON documents into other JSON documents.  
- [jsonpatch](doc/ref/jsonpatch/jsonpatch.md) implements the IETF standard [JavaScript Object Notation (JSON) Patch](https://tools.ietf.org/html/rfc6902)
- [mergepatch](doc/ref/mergepatch/mergepatch.md) implements the IETF standard [JSON Merge Patch](https://datatracker.ietf.org/doc/html/rfc7386)
- [jsonpath](doc/ref/jsonpath/jsonpath.md) implements [Stefan Goessner's JSONPath](http://goessner.net/articles/JsonPath/).  It also supports search and replace using JSONPath expressions.
- [jsonpointer](doc/ref/jsonpointer/jsonpointer.md) implements the IETF standard [JavaScript Object Notation (JSON) Pointer](https://tools.ietf.org/html/rfc6901)
- [jsonschema](doc/ref/jsonschema/jsonschema.md) implements Drafts 4, 6, 7, 2019-9 and 2020-12 of the [JSON Schema Specification](https://json-schema.org/specification) (since 0.174.0)
- [msgpack](doc/ref/msgpack/msgpack.md) implements decode from and encode to the [MessagePack](http://msgpack.org/index.html) data format.
- [ubjson](doc/ref/ubjson/ubjson.md) implements decode from and encode to the [Universal Binary JSON Specification](http://ubjson.org/) data format.

## What users say

_"Apache Kvrocks consistently utilizes jsoncons to offer support for JSON data structures to users. We find the development experience with jsoncons outstanding!"_

_"I have been using your library in my native language – R – and have created an R package making it easy for (a) JMESpath and JSONpath queries on JSON strings or R objects and (b) for other R developers to link to your library."_

_"I’m using your library for an external interface to pass data, as well as using the conversions from csv to json, which are really helpful for converting data for use in javascript"_

_"Verified that, for my needs in JSON and CBOR, it is working perfectly"_

_"the JSONPath feature of this library, it's great"_

_"We use JMESPath implementation quite extensively"_ 

_"We love your JSON Schema validator. We are using it in ER/Studio our data modelling tool to parse JSON Schema files so we can create entity relations models from them."_

_"the serialization lib of choice with its beautiful mappings and ease of use"_

_"really good"_ _"awesome project"_ _"very solid and very dependable"_ _"my team loves it"_ _"Your repo rocks!!!!!"_

## Mentions on the web

[Get started with HealthImaging image sets and image frames using an AWS SDK](https://docs.aws.amazon.com/healthimaging/latest/devguide/example_medical-imaging_Scenario_ImageSetsAndFrames_section.html)

[RubyGems.org](https://rubygems.org/gems/jsoncons/versions/0.1.3?locale=en)&nbsp;&nbsp;&nbsp;[rjsoncons](https://cran.rstudio.com/web/packages/rjsoncons/index.html)

## Get jsoncons

You can use the [vcpkg](https://github.com/Microsoft/vcpkg) platform library manager to install the [jsoncons package](https://github.com/microsoft/vcpkg/tree/master/ports/jsoncons).

Or, download the [latest release](https://github.com/danielaparker/jsoncons/releases) and unpack the zip file. Copy the directory `include/jsoncons` to your `include` directory. If you wish to use extensions, copy `include/jsoncons_ext` as well. 

Or, download the latest code on [main](https://github.com/danielaparker/jsoncons/archive/main.zip).

## How to use it

- [Quick guide](http://danielaparker.github.io/jsoncons)
- [Examples](doc/Examples.md)
- [Reference](doc/Reference.md)
- [Ask questions and suggest ideas for new features](https://github.com/danielaparker/jsoncons/discussions)

The library requires a C++ Compiler with C++11 support. In addition the library defines `jsoncons::endian`,
`jsoncons::basic_string_view`, `jsoncons::optional`, and `jsoncons::span`, which will be typedefed to
their standard library equivalents if detected. Otherwise they will be typedefed to internal, C++11 compatible, implementations.

The library uses exceptions and in some cases [std::error_code](https://en.cppreference.com/w/cpp/error/error_code)'s to report errors. Apart from `jsoncons::assertion_error`,
all jsoncons exception classes implement the [jsoncons::json_error](doc/ref/corelib/json_error.md) interface.
If exceptions are disabled or if the compile time macro `JSONCONS_NO_EXCEPTIONS` is defined, throws become calls to `std::terminate`.

## Benchmarks

[json_benchmarks](https://github.com/danielaparker/json_benchmarks) provides some measurements about how `jsoncons` compares to other `json` libraries.

- [JSONTestSuite and JSON_checker test suites](https://danielaparker.github.io/json_benchmarks/) 

- [Performance benchmarks with text and integers](https://github.com/danielaparker/json_benchmarks/blob/master/report/performance.md)

- [Performance benchmarks with text and doubles](https://github.com/danielaparker/json_benchmarks/blob/master/report/performance_fp.md)

[JSONPath Comparison](https://cburgmer.github.io/json-path-comparison/) shows how jsoncons JsonPath compares with other implementations

## Examples

[Working with JSON data](#E1)  

[Working with CBOR data](#E2)  

<div id="E1"/> 

### Working with JSON data

For the examples below you need to include some header files and initialize a string of JSON data:

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <iostream>

using namespace jsoncons; // for convenience

std::string data = R"(
    {
       "application": "hiking",
       "reputons": [
       {
           "rater": "HikingAsylum",
           "assertion": "advanced",
           "rated": "Marilyn C",
           "rating": 0.90,
           "generated": 1514862245
         }
       ]
    }
)";
```

jsoncons allows you to work with the data in a number of ways:

- As a variant-like data structure, [basic_json](doc/ref/corelib/basic_json.md) 

- As a strongly typed C++ data structure that implements [json_type_traits](doc/ref/corelib/json_type_traits.md)

- With [cursor-level access](doc/ref/corelib/basic_json_cursor.md) to a stream of parse events

#### As a variant-like data structure

```cpp
int main()
{
    // Parse the string of data into a json value
    json j = json::parse(data);

    // Does object member reputons exist?
    std::cout << "(1) " << std::boolalpha << j.contains("reputons") << "\n\n";

    // Get a reference to reputons array 
    const json& v = j["reputons"]; 

    // Iterate over reputons array 
    std::cout << "(2)\n";
    for (const auto& item : v.array_range())
    {
        // Access rated as string and rating as double
        std::cout << item["rated"].as<std::string>() << ", " << item["rating"].as<double>() << "\n";
    }
    std::cout << "\n";

    // Select all "rated" with JSONPath
    std::cout << "(3)\n";
    json result = jsonpath::json_query(j,"$..rated");
    std::cout << pretty_print(result) << "\n\n";

    // Serialize back to JSON
    std::cout << "(4)\n" << pretty_print(j) << "\n\n";
}
```
Output:
```
(1) true

(2)
Marilyn C, 0.9

(3)
[
    "Marilyn C"
]

(4)
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

#### As a strongly typed C++ data structure

jsoncons supports transforming JSON texts into C++ data structures. 
The functions [decode_json](doc/ref/corelib/decode_json.md) and [encode_json](doc/ref/corelib/encode_json.md) 
convert strings or streams of JSON data to C++ data structures and back. 
Decode and encode work for all C++ classes that have 
[json_type_traits](doc/ref/corelib/json_type_traits.md) 
defined. jsoncons already supports many types in the standard library, 
and your own types will be supported too if you specialize `json_type_traits`
in the `jsoncons` namespace. 

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
                       const std::optional<std::chrono::seconds>& generated = 
                           std::optional<std::chrono::seconds>(),
                       const std::optional<std::chrono::seconds>& expires = 
                           std::optional<std::chrono::seconds>())
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
This example makes use of the convenience macros `JSONCONS_ENUM_TRAITS`,
`JSONCONS_N_CTOR_GETTER_TRAITS`, and `JSONCONS_ALL_CTOR_GETTER_TRAITS` to specialize the 
[json_type_traits](doc/ref/corelib/json_type_traits.md) for the enum type
`ns::hiking_experience`, the class `ns::hiking_reputon` (with some non-mandatory members), and the class
`ns::hiking_reputation` (with all mandatory members.)
The macro `JSONCONS_ENUM_TRAITS` generates the code from
the enum identifiers, and the macros `JSONCONS_N_CTOR_GETTER_TRAITS`
and `JSONCONS_ALL_CTOR_GETTER_TRAITS` 
generate the code from the get functions and a constructor. 
These macro declarations must be placed outside any namespace blocks.

See [examples](doc/Examples.md#G0) for other ways of specializing `json_type_traits`.

#### With cursor-level access

A typical pull parsing application will repeatedly process the `current()` 
event and call `next()` to advance to the next event, until `done()` 
returns `true`.

```cpp
int main()
{
    json_string_cursor cursor(data);
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
begin_object
key: application
string_value: hiking
key: reputons
begin_array
begin_object
key: rater
string_value: HikingAsylum
key: assertion
string_value: advanced
key: rated
string_value: Marilyn C
key: rating
double_value: 0.9
key: generated
uint64_value: 1514862245
end_object
end_array
end_object
```

You can apply a filter to a cursor using the pipe syntax (e.g., `cursor | filter1 | filter2 | ...`)

```cpp
int main()
{
    std::string name;
    auto filter = [&](const staj_event& ev, const ser_context&) -> bool
    {
        if (ev.event_type() == staj_event_type::key)
        {
            name = ev.get<std::string>();
            return false;
        }
        if (name == "rated")
        {
            name.clear();
            return true;
        }
        return false;
    };

    json_string_cursor cursor(data);
    auto filtered_c = cursor | filter;

    for (; !filtered_c.done(); filtered_c.next())
    {
        const auto& event = filtered_c.current();
        switch (event.event_type())
        {
            case staj_event_type::string_value:
                // Or std::string_view, if C++17
                std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << "\n";
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
Marilyn C
```

<div id="E2"/> 

### Working with CBOR data

For the examples below you need to include some header files and initialize a buffer of CBOR data:

```cpp
#include <iomanip>
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

using namespace jsoncons; // for convenience

const std::vector<uint8_t> data = {
    0x9f, // Start indefinte length array
      0x83, // Array of length 3
        0x63, // String value of length 3
          0x66,0x6f,0x6f, // "foo" 
        0x44, // Byte string value of length 4
          0x50,0x75,0x73,0x73, // 'P''u''s''s'
        0xc5, // Tag 5 (bigfloat)
          0x82, // Array of length 2
            0x20, // -1
            0x03, // 3   
      0x83, // Another array of length 3
        0x63, // String value of length 3
          0x62,0x61,0x72, // "bar"
        0xd6, // Expected conversion to base64
        0x44, // Byte string value of length 4
          0x50,0x75,0x73,0x73, // 'P''u''s''s'
        0xc4, // Tag 4 (decimal fraction)
          0x82, // Array of length 2
            0x38, // Negative integer of length 1
              0x1c, // -29
            0xc2, // Tag 2 (positive bignum)
              0x4d, // Byte string value of length 13
                0x01,0x8e,0xe9,0x0f,0xf6,0xc3,0x73,0xe0,0xee,0x4e,0x3f,0x0a,0xd2,
    0xff // "break"
};
```

jsoncons allows you to work with the CBOR data similarly to JSON data:

- As a variant-like data structure, [basic_json](doc/ref/corelib/basic_json.md) 

- As a strongly typed C++ data structure that implements [json_type_traits](doc/ref/corelib/json_type_traits.md)

- With [cursor-level access](doc/ref/cbor/basic_cbor_cursor.md) to a stream of parse events

#### As a variant-like data structure

```cpp
int main()
{
    // Parse the CBOR data into a json value
    json j = cbor::decode_cbor<json>(data);

    // Pretty print
    std::cout << "(1)\n" << pretty_print(j) << "\n\n";

    // Iterate over rows
    std::cout << "(2)\n";
    for (const auto& row : j.array_range())
    {
        std::cout << row[1].as<jsoncons::byte_string>() << " (" << row[1].tag() << ")\n";
    }
    std::cout << "\n";

    // Select the third column with JSONPath
    std::cout << "(3)\n";
    json result = jsonpath::json_query(j,"$[*][2]");
    std::cout << pretty_print(result) << "\n\n";

    // Serialize back to CBOR
    std::vector<uint8_t> buffer;
    cbor::encode_cbor(j, buffer);
    std::cout << "(4)\n" << byte_string_view(buffer) << "\n\n";
}
```
Output:
```
(1)
[
    ["foo", "UHVzcw", "0x3p-1"],
    ["bar", "UHVzcw==", "1.23456789012345678901234567890"]
]

(2)
50,75,73,73 (n/a)
50,75,73,73 (base64)

(3)
[
    "0x3p-1",
    "1.23456789012345678901234567890"
]

(4)
82,83,63,66,6f,6f,44,50,75,73,73,c5,82,20,03,83,63,62,61,72,d6,44,50,75,73,73,c4,82,38,1c,c2,4d,01,8e,e9,0f,f6,c3,73,e0,ee,4e,3f,0a,d2
```

#### As a strongly typed C++ data structure

```cpp
int main()
{
    // Parse the string of data into a std::vector<std::tuple<std::string,jsoncons::byte_string,std::string>> value
    auto val = cbor::decode_cbor<std::vector<std::tuple<std::string,jsoncons::byte_string,std::string>>>(data);

    std::cout << "(1)\n";
    for (const auto& row : val)
    {
        std::cout << std::get<0>(row) << ", " << std::get<1>(row) << ", " << std::get<2>(row) << "\n";
    }
    std::cout << "\n";

    // Serialize back to CBOR
    std::vector<uint8_t> buffer;
    cbor::encode_cbor(val, buffer);
    std::cout << "(2)\n" << byte_string_view(buffer) << "\n\n";
}
```
Output:
```
(1)
foo, 50,75,73,73, 0x3p-1
bar, 50,75,73,73, 1.23456789012345678901234567890

(2)
82,9f,63,66,6f,6f,44,50,75,73,73,66,30,78,33,70,2d,31,ff,9f,63,62,61,72,44,50,75,73,73,78,1f,31,2e,32,33,34,35,36,37,38,39,30,31,32,33,34,35,36,37,38,39,30,31,32,33,34,35,36,37,38,39,30,ff
```

Note that when decoding the bigfloat and decimal fraction into a `std::string`, we lose the semantic information
that the variant like data structure preserved with a tag, so serializing back to CBOR produces a text string.

#### With cursor-level access

A typical pull parsing application will repeatedly process the `current()` 
event and call `next()` to advance to the next event, until `done()` 
returns `true`.

```cpp
int main()
{
    cbor::cbor_bytes_cursor cursor(data);
    for (; !cursor.done(); cursor.next())
    {
        const auto& event = cursor.current();
        switch (event.event_type())
        {
            case staj_event_type::begin_array:
                std::cout << event.event_type() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::end_array:
                std::cout << event.event_type() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::begin_object:
                std::cout << event.event_type() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::end_object:
                std::cout << event.event_type() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::key:
                // Or std::string_view, if supported
                std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::string_value:
                // Or std::string_view, if supported
                std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::byte_string_value:
                std::cout << event.event_type() << ": " << event.get<jsoncons::span<const uint8_t>>() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::null_value:
                std::cout << event.event_type() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::bool_value:
                std::cout << event.event_type() << ": " << std::boolalpha << event.get<bool>() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::int64_value:
                std::cout << event.event_type() << ": " << event.get<int64_t>() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::uint64_value:
                std::cout << event.event_type() << ": " << event.get<uint64_t>() << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::half_value:
            case staj_event_type::double_value:
                std::cout << event.event_type() << ": "  << event.get<double>() << " " << "(" << event.tag() << ")\n";
                break;
            default:
                std::cout << "Unhandled event type " << event.event_type() << " " << "(" << event.tag() << ")\n";
                break;
        }
    }
}
```
Output:
```
begin_array (n/a)
begin_array (n/a)
string_value: foo (n/a)
byte_string_value: 50,75,73,73 (n/a)
string_value: 0x3p-1 (bigfloat)
end_array (n/a)
begin_array (n/a)
string_value: bar (n/a)
byte_string_value: 50,75,73,73 (base64)
string_value: 1.23456789012345678901234567890 (bigdec)
end_array (n/a)
end_array (n/a)
```

You can apply a filter to a cursor using the pipe syntax, 

```cpp
int main()
{
    auto filter = [&](const staj_event& ev, const ser_context&) -> bool
    {
        return (ev.tag() == semantic_tag::bigdec) || (ev.tag() == semantic_tag::bigfloat);  
    };

    cbor::cbor_bytes_cursor cursor(data);
    auto filtered_c = cursor | filter;

    for (; !filtered_c.done(); filtered_c.next())
    {
        const auto& event = filtered_c.current();
        switch (event.event_type())
        {
            case staj_event_type::string_value:
                // Or std::string_view, if supported
                std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << " " << "(" << event.tag() << ")\n";
                break;
            default:
                std::cout << "Unhandled event type " << event.event_type() << " " << "(" << event.tag() << ")\n";
                break;
        }
    }
}
```
Output:
```
string_value: 0x3p-1 (bigfloat)
string_value: 1.23456789012345678901234567890 (bigdec)
```

## Supported compilers

jsoncons requires a compiler with minimally C++11 support. It is tested in continuous integration on [Github Actions](https://github.com/danielaparker/jsoncons/actions) and [circleci](https://app.circleci.com/pipelines/circleci/EFpnYcrBiZEvYvns3VF4vT).
[UndefinedBehaviorSanitizer (UBSan)](http://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html) diagnostics are enabled for selected gcc and clang builds.
Since v0.151.0, it is integrated with [Google OSS-fuzz](https://github.com/google/oss-fuzz), with coverage for all parsers and encoders.

| Compiler                | Version                            | Standard     | Architecture | Operating System | CI Service     |  
|-------------------------|------------------------------------|--------------|--------------|------------------|----------------|
| Visual Studio           | vs2019                             | default      | x86, x64     | Windows 11       | GitHub Actions |
|                         | vs2022                             | default      | x86, x64     | Windows 11       | GitHub Actions |
| Visual Studio - clang   | vs2019                             | default      | x86, x64     | Windows 11       | GitHub Actions |
|                         | vs2022                             | default      | x86, x64     | Windows 11       | GitHub Actions |
| g++                     | 6, 7, 8, 9, 10, 11, 12             | default      | x64          | Ubuntu           | circleci       |
| g++                     | 12                                 | c++20        | x64          | Ubuntu           | GitHub Actions |
| clang                   | 3.9, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15     | default      | x64          | Ubuntu           | circleci       |
| clang                   | 14                                 | c++20        | x64          | Ubuntu           | GitHub Actions |
| clang xcode             | 11, 12, 13                         | default      | x64          | OSX 11           | GitHub Actions |
| clang xcode             | 13, 14                             | default      | x64          | OSX 12           | GitHub Actions |

## Building the test suite and examples with CMake

[CMake](https://cmake.org/) is a cross-platform build tool that generates makefiles and solutions for the compiler environment of your choice. On Windows you can download a [Windows Installer package](https://cmake.org/download/). On Linux it is usually available as a package, e.g., on Ubuntu,
```
sudo apt-get install cmake
```
Once cmake is installed, you can build and run the unit tests from the jsoncons directory,

On Windows:
```
> mkdir build
> cd build
> cmake .. -DJSONCONS_BUILD_TESTS=On
> cmake --build .
> ctest -C Debug --output-on-failure
```

On UNIX:
```
$ mkdir build
$ cd build
$ cmake .. -DJSONCONS_BUILD_TESTS=On
$ cmake --build .
$ ctest --output-on-failure
```

## Acknowledgements

jsoncons uses the PVS-Studio static analyzer, provided free for open source projects.

A big thanks to the comp.lang.c++ community for help with implementation details. 

The jsoncons platform dependent binary configuration draws on to the excellent MIT licensed [tinycbor](https://github.com/intel/tinycbor).

Thanks to Milo Yip, author of [RapidJSON](http://rapidjson.org/), for raising the quality of JSON libraries across the board, by publishing [the benchmarks](https://github.com/miloyip/nativejson-benchmark), and contacting this project (among others) to share the results.

The jsoncons implementation of the Grisu3 algorithm for printing floating-point numbers follows Florian Loitsch's MIT licensed [grisu3_59_56 implementation](http://florian.loitsch.com/publications), with minor modifications. 

The macro `JSONCONS_ALL_MEMBER_TRAITS` follows the approach taken by Martin York's [ThorsSerializer](https://github.com/Loki-Astari/ThorsSerializer)

The jsoncons implementations of BSON decimal128 to and from string,
and ObjectId to and from string, are based on the Apache 2 licensed [libbson](https://github.com/mongodb/mongo-c-driver/tree/master/src/libbson).

Special thanks to our [contributors](https://github.com/danielaparker/jsoncons/blob/master/acknowledgements.md)
 
