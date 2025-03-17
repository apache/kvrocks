// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/bson/bson.hpp>

#include <jsoncons/json.hpp>

#include <string>
#include <iomanip>
#include <cassert>

using jsoncons::json;
using jsoncons::ojson;
namespace bson = jsoncons::bson; // for brevity

void encode_to_bson()
{
    std::vector<uint8_t> buffer;
    bson::bson_bytes_encoder encoder(buffer);
    encoder.begin_array(); // The total number of bytes comprising 
                          // the bson document will be calculated
    encoder.string_value("cat");
    std::vector<uint8_t> purr = {'p','u','r','r'};
    encoder.byte_string_value(purr); // default subtype is user defined
    // or encoder.byte_string_value(purr, 0x80);
    encoder.int64_value(1431027667, jsoncons::semantic_tag::epoch_second);
    encoder.end_array();
    encoder.flush();

    std::cout << jsoncons::byte_string_view(buffer) << "\n\n";

/* 
    23000000 -- Total number of bytes comprising the document (35 bytes) 
      02 -- UTF-8 string
        3000 -- "0"
        04000000 -- number bytes in the string (including trailing byte)
          636174  -- "cat"
            00 -- trailing byte
      05 -- binary
        3100 -- "1"
        04000000 -- number of bytes
        80 -- subtype
        70757272 -- 'P','u','r','r'
      09 -- datetime
      3200 -- "2"
        d3bf4b55 -- 1431027667
      00 
*/ 
}

void subtype_example()
{
    // Create some bson
    std::vector<uint8_t> buffer;
    bson::bson_bytes_encoder encoder(buffer);
    encoder.begin_object(); // The total number of bytes comprising 
                            // the bson document will be calculated
    encoder.key("Hello");
    encoder.string_value("World");
    encoder.key("Data");
    std::vector<uint8_t> bstr = {'f','o','o','b','a','r'};
    encoder.byte_string_value(bstr); // default subtype is user defined
    // or encoder.byte_string_value(bstr, 0x80); 
    encoder.end_object();
    encoder.flush();

    std::cout << "(1)\n" << jsoncons::byte_string_view(buffer) << "\n";

    /*
        0x27,0x00,0x00,0x00, // Total number of bytes comprising the document (40 bytes) 
            0x02, // URF-8 string
                0x48,0x65,0x6c,0x6c,0x6f, // Hello
                0x00, // trailing byte 
            0x06,0x00,0x00,0x00, // Number bytes in string (including trailing byte)
                0x57,0x6f,0x72,0x6c,0x64, // World
                0x00, // trailing byte
            0x05, // binary
                0x44,0x61,0x74,0x61, // Data
                0x00, // trailing byte
            0x06,0x00,0x00,0x00, // number of bytes
                0x80, // subtype
                0x66,0x6f,0x6f,0x62,0x61,0x72,
        0x00
    */

    ojson j = bson::decode_bson<ojson>(buffer);

    std::cout << "(2)\n" << pretty_print(j) << "\n\n";
    std::cout << "(3) " << j["Data"].tag() << "("  << j["Data"].ext_tag() << ")\n\n";

    // Get binary value as a std::vector<uint8_t>
    auto bstr2 = j["Data"].as<std::vector<uint8_t>>();
    assert(bstr2 == bstr);

    std::vector<uint8_t> buffer2;
    bson::encode_bson(j, buffer2);
    assert(buffer2 == buffer);
}

void int32_example()
{
    ojson j(jsoncons::json_object_arg);
    j.try_emplace("a", -123); // int32
    j.try_emplace("c", 0); // int32
    j.try_emplace("b", 123); // int32

    std::vector<char> buffer;
    bson::encode_bson(j, buffer);
    std::cout << jsoncons::byte_string_view(buffer) << "\n\n";
}

void int64_example()
{
    std::map<std::string, int64_t> m{ {"a", 100000000000000ULL} };

    std::vector<char> buffer;
    bson::encode_bson(m, buffer);
    std::cout << jsoncons::byte_string_view(buffer) << "\n\n";
}

void double_example()
{
    std::map<std::string, double> m{ {"a", 123.4567} };

    std::vector<char> buffer;
    bson::encode_bson(m, buffer);
    std::cout << jsoncons::byte_string_view(buffer) << "\n\n";
}

void bool_example()
{
    std::map<std::string, bool> m{ {"a", true} };

    std::vector<char> buffer;
    bson::encode_bson(m, buffer);
    std::cout << jsoncons::byte_string_view(buffer) << "\n\n";
}

void array_example()
{
    json a(jsoncons::json_array_arg);
    a.push_back("hello");
    a.push_back("world");

    json j(jsoncons::json_object_arg);
    j["array"] = std::move(a);

    std::vector<char> buffer;
    bson::encode_bson(j, buffer);

    std::cout << jsoncons::byte_string_view(buffer) << "\n\n";
}

void utf8_string_example()
{
    json j;
    j.try_emplace("hello", "world");

    std::vector<char> buffer;
    bson::encode_bson(j, buffer);
    std::cout << jsoncons::byte_string_view(buffer) << "\n\n";
}

void null_example()
{
    json j;
    j.try_emplace("hello", json::null()); 

    std::vector<char> buffer;
    bson::encode_bson(j, buffer);
    std::cout << jsoncons::byte_string_view(buffer) << "\n\n";
}

void duration_example1()
{
    auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto time = std::chrono::duration_cast<std::chrono::milliseconds>(duration);

    json j;
    j.try_emplace("time", time);

    auto milliseconds = j["time"].as<std::chrono::milliseconds>();
    std::cout << "Time since epoch (milliseconds): " << milliseconds.count() << "\n\n";
    auto seconds = j["time"].as<std::chrono::seconds>();
    std::cout << "Time since epoch (seconds): " << seconds.count() << "\n\n";

    std::vector<uint8_t> data;
    bson::encode_bson(j, data);

    std::cout << "BSON bytes:\n" << jsoncons::byte_string_view(data) << "\n\n";

/*
    13,00,00,00, // document has 19 bytes
      09, // UTC datetime
        74,69,6d,65,00, // "time"
        ea,14,7f,96,73,01,00,00, // 1595957777642
    00 // terminating null    
*/
}

void binary_example1()
{
    json j;
    std::vector<uint8_t> bstr = { '1', '2', '3', '4' };
    j.try_emplace("binary", jsoncons::byte_string_arg, bstr); // default subtype is user defined
    // or j.try_emplace("binary", byte_string_arg, bstr, 0x80);

    std::vector<char> buffer;
    bson::encode_bson(j, buffer);
    std::cout << jsoncons::byte_string_view(buffer) << "\n\n";
}

void binary_example2()
{
    std::vector<uint8_t> input = { 0x13,0x00,0x00,0x00, // Document has 19 bytes
                                  0x05, // Binary data
                                  0x70,0x44,0x00, // "pD"
                                  0x05,0x00,0x00,0x00, // Length is 5
                                  0x80, // Subtype is 128
                                  0x48,0x65,0x6c,0x6c,0x6f, // 'H','e','l','l','o'
                                  0x00 // terminating null
    };

    json j = bson::decode_bson<json>(input);
    std::cout << "JSON:\n" << pretty_print(j) << "\n\n";

    std::cout << "tag: " << j["pD"].tag() << "\n";
    std::cout << "ext_tag: " << j["pD"].ext_tag() << "\n";
    auto bytes = j["pD"].as<std::vector<uint8_t>>();
    std::cout << "binary data: " << jsoncons::byte_string_view{ bytes } << "\n";
}

void decode_decimal128()
{
    std::vector<uint8_t> input = {
        0x18,0x00,0x00,0x00, // Document has 24 bytes
        0x13,                // 128-bit decimal floating point
        0x61,0x00,           // "a"
        0x01,0x00,0x00,0x00,
        0x00,0x00,0x00,0x00, // 1E-6176
        0x00,0x00,0x00,0x00,
        0x00,0x00,0x00,0x00, 
        0x00                 // terminating null
    };

    json j = bson::decode_bson<json>(input);

    std::cout << "(1) " << j << "\n\n";
    std::cout << "(2) " << j.at("a").tag() << "\n\n";

    std::vector<uint8_t> output;
    bson::encode_bson(j, output);
    assert(output == input);
}

void encode_decimal128()
{
    json j;

    j.try_emplace("a", "1E-6176", jsoncons::semantic_tag::float128);
    // or j["a"] = json("1E-6176", jsoncons::semantic_tag::float128);

    std::cout << "(1) " << j << "\n\n";
    std::cout << "(2) " << j.at("a").tag() << "\n\n";

    std::vector<char> output;
    bson::encode_bson(j, output);
    std::cout << "(3) " << jsoncons::byte_string_view(output) << "\n\n";
    /*
        18,00,00,00,          // document has 24 bytes
          13,                 // 128-bit decimal floating point
            13,00,            // "a"
            01,00,00,00,
            00,00,00,00,      // 1E-6176
            00,00,00,00,
            00,00,00,00, 
        00                    // terminating null    
    */
}

void regex_example()
{
    std::vector<uint8_t> input = {
        0x16,0x00,0x00,0x00,            // Document has 22 bytes
        0x0B,                           // Regular expression
        0x72,0x65,0x67,0x65,0x78,0x00,  // "regex"
        0x5E,0x61,0x62,0x63,0x64,0x00,  // "^abcd"
        0x69,0x6C,0x78,0x00,            // "ilx"
        0x00                            // terminating null
    };

    json j = bson::decode_bson<json>(input);

    std::cout << "(1) " << j << "\n\n";
    std::cout << "(2) " << j.at("regex").tag() << "\n\n";

    std::vector<uint8_t> output;
    bson::encode_bson(j, output);
    assert(output == input);
}

void oid_example()
{
    std::vector<uint8_t> input = {
        0x16,0x00,0x00,0x00,            // Document has 22 bytes
        0x07,                           // ObjectId
        0x6F,0x69,0x64,0x00,            // "oid"
        0x12,0x34,0x56,0x78,0x90,0xAB,  
        0xCD,0xEF,0x12,0x34,0xAB,0xCD,  // (byte*12)
        0x00                            // terminating null
    };

    json j = bson::decode_bson<json>(input);

    std::cout << "(1) " << j << "\n\n";
    std::cout << "(2) " << j.at("oid").tag() << "\n\n";

    std::vector<uint8_t> output;
    bson::encode_bson(j, output);
    assert(output == input);
}

int main()
{
    std::cout << "\nbson examples\n\n";
    encode_to_bson();
    subtype_example();
    null_example();
    bool_example();
    int32_example();
    int64_example();
    double_example();
    utf8_string_example();
    array_example();
    duration_example1();
    binary_example1();
    binary_example2();
    decode_decimal128();
    encode_decimal128();
    regex_example();
    oid_example();
    std::cout << '\n';
}

