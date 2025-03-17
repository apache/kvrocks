## ubjson extension

The ubjson extension implements encode to and decode from the [Universal Binary JSON Specification](http://ubjson.org/) data format.
You can either parse into or serialize from a variant-like data structure, [basic_json](../basic_json.md), or your own
data structures, using [json_type_traits](../json_type_traits.md).

[decode_ubjson](decode_ubjson.md)

[basic_ubjson_cursor](basic_ubjson_cursor.md)

[encode_ubjson](encode_ubjson.md)

[basic_ubjson_encoder](basic_ubjson_encoder.md)

[ubjson_options](ubjson_options.md)

#### Mappings between UBJSON and jsoncons data items

UBJSON data item           | jsoncons data item|jsoncons tag  
---------------------------|---------------|------------------
 null                      | null          |                  
 true or false             | bool          |                  
 uint8_t or integer        | int64         |                  
 uint8_t or integer        | uint64        |                  
 float 32 or float 64      | double        |                  
 string                    | string        |                  
 high precision number type| string        | bigint           
 high precision number type| string        | bigdec           
 array of uint8_t          | byte_string   |                  
 array                     | array         |                  
 object                    | object        |                  

## Examples

### Working with UBJSON data

For the examples below you need to include some header files and initialize a buffer of UBJSON data:

```cpp
#include <iomanip>
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/ubjson/ubjson.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

using namespace jsoncons; // for convenience

const std::vector<uint8_t> data =
{0x5b,0x23,0x55,0x05, // [ # i 5
0x44, // float64
    0x40,0x3d,0xf8,0x51,0xeb,0x85,0x1e,0xb8, // 29.97
0x44, // float64
    0x40,0x3f,0x21,0x47,0xae,0x14,0x7a,0xe1, // 31.13
0x64, // float32
    0x42,0x86,0x00,0x00, // 67.0
0x44, // float64
    0x40,0x00,0xe7,0x6c,0x8b,0x43,0x95,0x81, // 2.113
0x44, // float64
    0x40,0x37,0xe3,0x8e,0xf3,0x4d,0x6a,0x16 // 23.8889
};
```

jsoncons allows you to work with the UBJSON data similarly to JSON data:

- As a variant-like data structure, [basic_json](doc/ref/corelib/basic_json.md) 

- As a strongly typed C++ data structure that implements [json_type_traits](../json_type_traits.md) 

- With [cursor-level access](doc/ref/ubjson/basic_ubjson_cursor.md) to a stream of parse events

#### As a variant-like data structure

```cpp
int main()
{
    std::cout << std::dec;
    std::cout << std::setprecision(15);

    // Parse the UBJSON data into a json value
    json j = ubjson::decode_ubjson<json>(data);

    // Pretty print
    std::cout << "(1)\n" << pretty_print(j) << "\n\n";

    // Iterate over rows
    std::cout << "(2)\n";
    for (const auto& item : j.array_range())
    {
        std::cout << item.as<double>() << " (" << item.tag() << ")\n";
    }
    std::cout << "\n";

    // Select all values less than 30 with JSONPath
    std::cout << "(3)\n";
    json result = jsonpath::json_query(j,"$[?(@ < 30)]");
    std::cout << pretty_print(result) << "\n";
}
```
Output:
```
(1)
[
    29.97,
    31.13,
    67.0,
    2.113,
    23.8889
]

(2)
29.97 (n/a)
31.13 (n/a)
67 (n/a)
2.113 (n/a)
23.8889 (n/a)

(3)
[
    29.97,
    2.113,
    23.8889
]
```

#### As a strongly typed C++ data structure

```cpp
int main()
{
    // Parse the UBJSON data into a std::vector<double> value
    auto val = ubjson::decode_ubjson<std::vector<double>>(data);

    for (auto item : val)
    {
        std::cout << item << "\n";
    }
}
```
Output:
```
29.97
31.13
67
2.113
23.8889
```

#### With cursor-level access

```cpp
int main()
{
    ubjson::ubjson_bytes_cursor cursor(data);
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
double_value: 29.97 (n/a)
double_value: 31.13 (n/a)
double_value: 67 (n/a)
double_value: 2.113 (n/a)
double_value: 23.8889 (n/a)
end_array (n/a)
```

You can apply a filter to a cursor using the pipe syntax, for example,

```cpp
int main()
{
    auto filter = [&](const staj_event& ev, const ser_context&) -> bool
    {
        return (ev.event_type() == staj_event_type::double_value) && (ev.get<double>() < 30.0);  
    };

    ubjson::ubjson_bytes_cursor cursor(data);

    auto filtered_c = cursor | filter;
    for (; !filtered_c.done(); filtered_c.next())
    {
        const auto& event = filtered_c.current();
        switch (event.event_type())
        {
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
double_value: 29.97 (n/a)
double_value: 2.113 (n/a)
double_value: 23.8889 (n/a)
```

### encode/decode UBJSON from/to basic_json

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/ubjson/ubjson.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

using namespace jsoncons;

int main()
{
    ojson j1 = ojson::parse(R"(
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

    // Encode a basic_json value to UBJSON
    std::vector<uint8_t> data;
    ubjson::encode_ubjson(j1, data);

    // Decode UBJSON to a basic_json value
    ojson j2 = ubjson::decode_ubjson<ojson>(data);
    std::cout << "(1)\n" << pretty_print(j2) << "\n\n";

    // Accessing the data items 

    const ojson& reputons = j2["reputons"];

    std::cout << "(2)\n";
    for (auto element : reputons.array_range())
    {
        std::cout << element.at("rated").as<std::string>() << ", ";
        std::cout << element.at("rating").as<double>() << "\n";
    }
    std::cout << '\n';

    // Get a UBJSON value for a nested data item with jsonpointer
    std::error_code ec;
    const auto& rated = jsonpointer::get(j2, "/reputons/0/rated", ec);
    if (!ec)
    {
        std::cout << "(3) " << rated.as_string() << "\n";
    }

    std::cout << '\n';
}
```
Output:
```
(1)
{
    "application": "hiking",
    "reputons": [
        {
            "rater": "HikingAsylum",
            "assertion": "advanced",
            "rated": "Marilyn C",
            "rating": 0.9
        }
    ]
}

(2)
Marilyn C, 0.9

(3) Marilyn C
```

### encode/decode UBJSON from/to your own data structures

```cpp
#include <cassert>
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/ubjson/ubjson.hpp>

namespace ns {

    enum class hiking_experience {beginner,intermediate,advanced};

    class hiking_reputon
    {
        std::string rater_;
        hiking_experience assertion_;
        std::string rated_;
        double rating_;
    public:
        hiking_reputon(const std::string& rater,
                       hiking_experience assertion,
                       const std::string& rated,
                       double rating)
            : rater_(rater), assertion_(assertion), rated_(rated), rating_(rating)
        {
        }

        const std::string& rater() const {return rater_;}
        hiking_experience assertion() const {return assertion_;}
        const std::string& rated() const {return rated_;}
        double rating() const {return rating_;}

        friend bool operator==(const hiking_reputon& lhs, const hiking_reputon& rhs)
        {
            return lhs.rater_ == rhs.rater_ && lhs.assertion_ == rhs.assertion_ && 
                   lhs.rated_ == rhs.rated_ && lhs.rating_ == rhs.rating_;
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

        friend bool operator==(const hiking_reputation& lhs, const hiking_reputation& rhs)
        {
            return (lhs.application_ == rhs.application_) && (lhs.reputons_ == rhs.reputons_);
        }

        friend bool operator!=(const hiking_reputation& lhs, const hiking_reputation& rhs)
        {
            return !(lhs == rhs);
        };
    };

} // namespace ns

// Declare the traits. Specify which data members need to be serialized.
JSONCONS_ENUM_TRAITS(ns::hiking_experience, beginner, intermediate, advanced)
JSONCONS_ALL_CTOR_GETTER_TRAITS(ns::hiking_reputon, rater, assertion, rated, rating)
JSONCONS_ALL_CTOR_GETTER_TRAITS(ns::hiking_reputation, application, reputons)

int main()
{
    ns::hiking_reputation val("hiking", { ns::hiking_reputon{"HikingAsylum",ns::hiking_experience::advanced,"Marilyn C",0.90} });

    // Encode a ns::hiking_reputation value to UBJSON
    std::vector<uint8_t> data;
    ubjson::encode_ubjson(val, data);

    // Decode UBJSON to a ns::hiking_reputation value
    ns::hiking_reputation val2 = ubjson::decode_ubjson<ns::hiking_reputation>(data);

    assert(val2 == val);
}
```

