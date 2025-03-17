// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/ubjson/ubjson.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

#include "sample_types.hpp"
#include <jsoncons/json.hpp>

#include <string>
#include <iomanip>
#include <cassert>

using namespace jsoncons;

void to_from_ubjson_using_basic_json()
{
    ojson j1 = ojson::parse(R"(
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
    auto const& rated = jsonpointer::get(j2, "/reputons/0/rated", ec);
    if (!ec)
    {
        std::cout << "(3) " << rated.as_string() << "\n";
    }

    std::cout << '\n';
}

void to_from_ubjson_using_example_type()
{
    ns::hiking_reputation val("hiking", { ns::hiking_reputon{"HikingAsylum",ns::hiking_experience::advanced,"Marilyn C",0.90} });

    // Encode a ns::hiking_reputation value to UBJSON
    std::vector<uint8_t> data;
    ubjson::encode_ubjson(val, data);

    // Decode UBJSON to a ns::hiking_reputation value
    ns::hiking_reputation val2 = ubjson::decode_ubjson<ns::hiking_reputation>(data);

    assert(val2 == val);
}

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

void working_with_ubjson_1()
{
    std::cout << std::dec;
    std::cout << std::setprecision(15);

    // Parse the string of data into a json value
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

void working_with_ubjson_2()
{
    // Parse the string of data into a std::vector<double> value
    auto val = ubjson::decode_ubjson<std::vector<double>>(data);

    for (auto item : val)
    {
        std::cout << item << "\n";
    }
}

void working_with_ubjson_3()
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

void working_with_ubjson_4()
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

int main()
{
    std::cout << "\nubjson examples\n\n";

    to_from_ubjson_using_basic_json();
    std::cout << "\n";
    to_from_ubjson_using_example_type();
    std::cout << "\n";
    working_with_ubjson_1();
    std::cout << "\n";
    working_with_ubjson_2();
    std::cout << "\n";
    working_with_ubjson_3();
    std::cout << "\n";
    working_with_ubjson_4();

    std::cout << '\n';
}

