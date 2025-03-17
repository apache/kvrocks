// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

#include <jsoncons/json.hpp>

#include "sample_types.hpp"
#include <string>
#include <iomanip>
#include <cassert>

using namespace jsoncons;

void encode_to_cbor_buffer()
{
    std::vector<uint8_t> buffer;
    cbor::cbor_bytes_encoder encoder(buffer);

    encoder.begin_array(); // Indefinite length array
    encoder.string_value("cat");
    std::vector<uint8_t> purr = {'p','u','r','r'};
    encoder.byte_string_value(purr);
    std::vector<uint8_t> hiss = {'h','i','s','s'};
    encoder.byte_string_value(hiss, semantic_tag::base64); // suggested conversion to base64
    encoder.int64_value(1431027667, semantic_tag::epoch_second);
    encoder.end_array();
    encoder.flush();

    std::cout << byte_string_view(buffer) << "\n\n";

/* 
    9f -- Start indefinte length array
      63 -- String value of length 3
        636174 -- "cat"
      44 -- Byte string value of length 4
        70757272 -- 'p''u''r''r'
      d6 - Expected conversion to base64
      44
        68697373 -- 'h''i''s''s'
      c1 -- Tag value 1 (seconds relative to 1970-01-01T00:00Z in UTC time)
        1a -- 32 bit unsigned integer
          554bbfd3 -- 1431027667
      ff -- "break" 
*/ 
}

void encode_to_cbor_stream()
{
    std::ostringstream os;
    cbor::cbor_stream_encoder encoder(os);

    encoder.begin_array(3); // array of length 3
    encoder.string_value("-18446744073709551617", semantic_tag::bigint);
    encoder.string_value("184467440737095516.16", semantic_tag::bigdec);
    encoder.int64_value(1431027667, semantic_tag::epoch_second);
    encoder.end_array();
    encoder.flush();

    std::cout << byte_string_view(os.str()) << "\n\n";

/*
    83 -- array of length 3
      c3 -- Tag 3 (negative bignum)
      49 -- Byte string value of length 9
        010000000000000000 -- Bytes content
      c4 -- Tag 4 (decimal fraction)
        82 -- Array of length 2
          21 -- -2 (exponent)
          c2 Tag 2 (positive bignum)
          49 -- Byte string value of length 9
            010000000000000000
      c1 -- Tag 1 (seconds relative to 1970-01-01T00:00Z in UTC time)
        1a -- 32 bit unsigned integer
          554bbfd3 -- 1431027667
*/
}

void cbor_reputon_example()
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

    // Encode a basic_json value to a CBOR value
    std::vector<uint8_t> data;
    cbor::encode_cbor(j1, data);

    // Decode a CBOR value to a basic_json value
    ojson j2 = cbor::decode_cbor<ojson>(data);
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

    // Get a CBOR value for a nested data item with jsonpointer
    std::error_code ec;
    auto const& rated = jsonpointer::get(j2, "/reputons/0/rated", ec);
    if (!ec)
    {
        std::cout << "(3) " << rated.as_string() << "\n";
    }

    std::cout << '\n';
}

void decode_cbor_byte_string()
{
    // byte string of length 5
    std::vector<uint8_t> buf = {0x45,'H','e','l','l','o'};
    json j = cbor::decode_cbor<json>(buf);

    auto bytes = j.as<byte_string>();

    // byte_string to ostream displays as hex
    std::cout << "(1) "<< bytes << "\n\n";

    // byte string value to JSON text becomes base64url
    std::cout << "(2) " << j << '\n';
}

void decode_byte_string_with_encoding_hint()
{
    // semantic tag indicating expected conversion to base64
    // followed by byte string of length 5
    std::vector<uint8_t> buf = {0xd6,0x45,'H','e','l','l','o'};
    json j = cbor::decode_cbor<json>(buf);

    auto bytes = j.as<byte_string>();

    // byte_string to ostream displays as hex
    std::cout << "(1) "<< bytes << "\n\n";

    // byte string value to JSON text becomes base64
    std::cout << "(2) " << j << '\n';
}

void encode_cbor_byte_string()
{
    // construct byte string value
    json j(byte_string{'H','e','l','l','o'});

    std::vector<uint8_t> buf;
    cbor::encode_cbor(j, buf);

    std::cout << byte_string_view(buf) << "\n\n";

    json j2 = cbor::decode_cbor<json>(buf);
    std::cout << "(2) " << j2 << '\n';
}

void encode_byte_string_with_encoding_hint()
{
    // construct byte string value
     json j1(byte_string_arg, byte_string{'H','e','l','l','o'}, semantic_tag::base64);

    std::vector<uint8_t> buf;
    cbor::encode_cbor(j1, buf);

    std::cout << byte_string_view(buf) << "\n\n";

    json j2 = cbor::decode_cbor<json>(buf);
    std::cout << "(2) " << j2 << '\n';
}

void query_cbor()
{
    // Construct a json array of numbers
    json j(json_array_arg);

    j.emplace_back(5.0);

    j.emplace_back(0.000071);

    j.emplace_back("-18446744073709551617",semantic_tag::bigint);

    j.emplace_back("1.23456789012345678901234567890", semantic_tag::bigdec);

    j.emplace_back("0x3p-1", semantic_tag::bigfloat);

    // Encode to JSON
    std::cout << "(1)\n";
    std::cout << pretty_print(j);
    std::cout << "\n\n";

    // as<std::string>() and as<double>()
    std::cout << "(2)\n";
    std::cout << std::dec << std::setprecision(15);
    for (const auto& item : j.array_range())
    {
        std::cout << item.as<std::string>() << ", " << item.as<double>() << "\n";
    }
    std::cout << "\n";

    // Encode to CBOR
    std::vector<uint8_t> v;
    cbor::encode_cbor(j,v);

    std::cout << "(3)\n" << byte_string_view(v) << "\n\n";
/*
    85 -- Array of length 5     
      fa -- float 
        40a00000 -- 5.0
      fb -- double 
        3f129cbab649d389 -- 0.000071
      c3 -- Tag 3 (negative bignum)
        49 -- Byte string value of length 9
          010000000000000000
      c4 -- Tag 4 (decimal fraction)
        82 -- Array of length 2
          38 -- Negative integer of length 1
            1c -- -29
          c2 -- Tag 2 (positive bignum)
            4d -- Byte string value of length 13
              018ee90ff6c373e0ee4e3f0ad2
      c5 -- Tag 5 (bigfloat)
        82 -- Array of length 2
          20 -- -1
          03 -- 3   
*/

    // Decode back to json
    json other = cbor::decode_cbor<json>(v);
    assert(other == j);

    // Query with JSONPath
    std::cout << "(4)\n";
    json result = jsonpath::json_query(other,"$[?(@ < 1.5)]");
    std::cout << pretty_print(result) << "\n\n";
}

void encode_cbor_with_packed_strings()
{
    ojson j = ojson::parse(R"(
[
     {
       "name" : "Cocktail",
       "count" : 417,
       "rank" : 4
     },
     {
       "rank" : 4,
       "count" : 312,
       "name" : "Bath"
     },
     {
       "count" : 691,
       "name" : "Food",
       "rank" : 4
     }
  ]
)");

    auto options = cbor::cbor_options{}
        .pack_strings(true);
    std::vector<uint8_t> buf;

    cbor::encode_cbor(j, buf, options);

    std::cout << byte_string_view(buf)<< "\n\n";

/*
    d90100 -- tag (256)
      83 -- array(3)
        a3 -- map(3)
          64 -- text string (4)
            6e616d65 -- "name"
          68 -- text string (8)
            436f636b7461696c -- "Cocktail"
          65 -- text string (5)
            636f756e74 -- "count"
            1901a1 -- unsigned(417)
          64 -- text string (4)
            72616e6b -- "rank"
            04 -- unsigned(4)
        a3 -- map(3)
          d819 -- tag(25)
            03 -- unsigned(3)
          04 -- unsigned(4)
          d819 -- tag(25)
            02 -- unsigned(2)
            190138 -- unsigned(312)
          d819 -- tag(25)
            00 -- unsigned(0)
          64 -- text string(4)
            42617468 -- "Bath"
        a3 -- map(3)
          d819 -- tag(25)
            02 -- unsigned(2)
          1902b3 -- unsigned(691)
          d819 -- tag(25)
            00 -- unsigned(0)
          64 - text string(4)
            466f6f64 -- "Food"
          d819 -- tag(25)
            03 -- unsigned(3)
            04 -- unsigned(4)
*/

    ojson j2 = cbor::decode_cbor<ojson>(buf);
    assert(j2 == j);
}

void decode_cbor_with_packed_strings()
{
    std::vector<uint8_t> v = {0xd9,0x01,0x00, // tag(256)
      0x85,                 // array(5)
         0x63,              // text(3)
            0x61,0x61,0x61, // "aaa"
         0xd8, 0x19,        // tag(25)
            0x00,           // unsigned(0)
         0xd9, 0x01,0x00,   // tag(256)
            0x83,           // array(3)
               0x63,        // text(3)
                  0x62,0x62,0x62, // "bbb"
               0x63,        // text(3)
                  0x61,0x61,0x61, // "aaa"
               0xd8, 0x19,  // tag(25)
                  0x01,     // unsigned(1)
         0xd9, 0x01,0x00,   // tag(256)
            0x82,           // array(2)
               0x63,        // text(3)
                  0x63,0x63,0x63, // "ccc"
               0xd8, 0x19,  // tag(25)
                  0x00,     // unsigned(0)
         0xd8, 0x19,        // tag(25)
            0x00           // unsigned(0)
    };

    ojson j = cbor::decode_cbor<ojson>(v);

    std::cout << pretty_print(j) << "\n";
}

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

void working_with_cbor1()
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

void working_with_cbor2()
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

void working_with_cbor3()
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
                std::cout << event.event_type() << ": " << event.get<jsoncons::byte_string_view>() << " " << "(" << event.tag() << ")\n";
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

void working_with_cbor4()
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

void ext_type_example()
{
    // Create some CBOR
    std::vector<uint8_t> buffer;
    cbor::cbor_bytes_encoder encoder(buffer);

    std::vector<uint8_t> bstr = {'f','o','o','b','a','r'};
    encoder.byte_string_value(bstr, 274); // byte string with tag 274
    encoder.flush();

    std::cout << "(1)\n" << byte_string_view(buffer) << "\n\n";

    /*
        d9, // tag
            01,12, // 274
        46, // byte string, length 6
            66,6f,6f,62,61,72 // 'f','o','o','b','a','r'         
    */ 

    json j = cbor::decode_cbor<json>(buffer);

    std::cout << "(2)\n" << pretty_print(j) << "\n\n";
    std::cout << "(3) " << j.tag() << "("  << j.ext_tag() << ")\n\n";

    // Get byte string as a std::vector<uint8_t>
    auto bstr2 = j.as<std::vector<uint8_t>>();

    std::vector<uint8_t> buffer2;
    cbor::encode_cbor(j, buffer2);
    std::cout << "(4)\n" << byte_string_view(buffer2.data(),buffer2.size()) << "\n";
}

void duration_example1()
{
    auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto time = std::chrono::duration_cast<std::chrono::seconds>(duration);

    std::vector<uint8_t> data;
    cbor::encode_cbor(time, data);

    /*
      c1, // Tag 1 (epoch time)
        1a, // 32 bit unsigned integer
          5f,23,29,18 // 1596139800
    */

    std::cout << "CBOR bytes:\n" << jsoncons::byte_string_view(data) << "\n\n";

    auto seconds = cbor::decode_cbor<std::chrono::seconds>(data);
    std::cout << "Time since epoch (seconds): " << seconds.count() << "\n";
}

void duration_example2()
{
    auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto time = std::chrono::duration_cast<std::chrono::duration<double>>(duration);

    std::vector<uint8_t> data;
    cbor::encode_cbor(time, data);

    /*
      c1, // Tag 1 (epoch time)
        fb,  // Double
          41,d7,c8,ca,46,1c,0f,87 // 1596139800.43845
    */

    std::cout << "CBOR bytes:\n" << jsoncons::byte_string_view(data) << "\n\n";

    auto seconds = cbor::decode_cbor<std::chrono::duration<double>>(data);
    std::cout << "Time since epoch (seconds): " << seconds.count() << "\n";

    auto milliseconds = cbor::decode_cbor<std::chrono::milliseconds>(data);
    std::cout << "Time since epoch (milliseconds): " << milliseconds.count() << "\n";
}

void encode_with_raw_cbor_tags()   // (since 1.2.0)
{
    std::vector<uint8_t> data;
    cbor::cbor_bytes_encoder encoder(data);
    encoder.begin_array(1);
    encoder.uint64_value_with_tag(10, 0xC1);
    encoder.end_array();
    encoder.flush();

    cbor::cbor_bytes_cursor cursor(data);
    assert(cursor.current().event_type() == jsoncons::staj_event_type::begin_array);
    cursor.next();
    assert(cursor.raw_tag() == 0xC1);
    assert(cursor.current().get<uint64_t>() == 10);
}

int main()
{
    std::cout << "\ncbor examples\n\n";

    encode_byte_string_with_encoding_hint();
    encode_cbor_byte_string();
    encode_to_cbor_stream();
    cbor_reputon_example();
    encode_cbor_with_packed_strings();

    decode_cbor_with_packed_strings();

    decode_cbor_byte_string();
    decode_byte_string_with_encoding_hint();

    working_with_cbor3();
    std::cout << "\n";
    working_with_cbor4();
    std::cout << "\n";
    working_with_cbor1();
    std::cout << "\n";
    working_with_cbor2();
    std::cout << '\n';
    encode_to_cbor_buffer();
    query_cbor();

    ext_type_example();
    duration_example1();
    duration_example2();
    
    encode_with_raw_cbor_tags();
}

