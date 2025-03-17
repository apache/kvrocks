// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/msgpack/msgpack.hpp>
#include <jsoncons/json.hpp>

#include <string>

// For brevity
using jsoncons::json;
using jsoncons::ojson;
namespace msgpack = jsoncons::msgpack;

void example1()
{
ojson j1 = ojson::parse(R"(
[
  { "category": "reference",
    "author": "Nigel Rees",
    "title": "Sayings of the Century",
    "price": 8.95
  },
  { "category": "fiction",
    "author": "Evelyn Waugh",
    "title": "Sword of Honour",
    "price": 12.99
  }
]
)");

    std::vector<uint8_t> v;
    msgpack::encode_msgpack(j1, v);

    ojson j2 = msgpack::decode_msgpack<ojson>(v);

    std::cout << pretty_print(j2) << '\n';

    json j3 = msgpack::decode_msgpack<json>(v);

    std::cout << pretty_print(j3) << '\n';

    std::cout << '\n';

    //wjson j4 = msgpack::decode_msgpack<wjson>(v);

    //std::wcout << pretty_print(j4) << '\n';

    //std::cout << '\n';
}

void example2()
{
    ojson j1;
    j1["zero"] = 0;
    j1["one"] = 1;
    j1["two"] = 2;
    j1["null"] = jsoncons::null_type();
    j1["true"] = true;
    j1["false"] = false;
    j1["max int64_t"] = (std::numeric_limits<int64_t>::max)();
    j1["max uint64_t"] = (std::numeric_limits<uint64_t>::max)();
    j1["min int64_t"] = (std::numeric_limits<int64_t>::lowest)();
    j1["max int32_t"] = (std::numeric_limits<int32_t>::max)();
    j1["max uint32_t"] = (std::numeric_limits<uint32_t>::max)();
    j1["min int32_t"] = (std::numeric_limits<int32_t>::lowest)();
    j1["max int16_t"] = (std::numeric_limits<int16_t>::max)();
    j1["max uint16_t"] = (std::numeric_limits<uint16_t>::max)();
    j1["min int16_t"] = (std::numeric_limits<int16_t>::lowest)();
    j1["max int8_t"] = (std::numeric_limits<int8_t>::max)();
    j1["max uint8_t"] = (std::numeric_limits<uint8_t>::max)();
    j1["min int8_t"] = (std::numeric_limits<int8_t>::lowest)();
    j1["max double"] = (std::numeric_limits<double>::max)();
    j1["min double"] = (std::numeric_limits<double>::lowest)();
    j1["max float"] = (std::numeric_limits<float>::max)();
    j1["zero float"] = 0.0;
    j1["min float"] = (std::numeric_limits<float>::lowest)();
    j1["Key too long for small string optimization"] = "String too long for small string optimization";

    std::vector<uint8_t> v;
    msgpack::encode_msgpack(j1, v);

    ojson j2 = msgpack::decode_msgpack<ojson>(v);

    std::cout << pretty_print(j2) << '\n';

    std::cout << '\n';
}

void ext_example()
{
    std::vector<uint8_t> input = {

        0x82, // map, length 2
          0xa5, // string, length 5
            'H','e','l','l','o',
          0xa5, // string, length 5
            'W','o','r','l','d',
          0xa4, // string, length 4
             'D','a','t','a',
          0xc7, // ext8 format code
            0x06, // length 6
            0x07, // type
              'f','o','o','b','a','r'
    };

    ojson j = msgpack::decode_msgpack<ojson>(input);

    std::cout << "(1)\n" << pretty_print(j) << "\n\n";
    std::cout << "(2) " << j["Data"].tag() << "("  << j["Data"].ext_tag() << ")\n\n";
    
    // Get ext value as a std::vector<uint8_t>
    auto v = j["Data"].as<std::vector<uint8_t>>(); 

    std::cout << "(3)\n";
    std::cout << jsoncons::byte_string_view(v) << "\n\n";

    std::vector<uint8_t> output;
    msgpack::encode_msgpack(j,output);
    assert(output == input);
}

void duration_example1()
{
    std::vector<uint8_t> data = {
        0xd6, // fixext 4 stores an integer and a byte array whose length is 4 bytes
        0xff, // timestamp
        0x5a,0x4a,0xf6,0xa5 // 1514862245
    };
    auto seconds = msgpack::decode_msgpack<std::chrono::seconds>(data);
    std::cout << "Seconds elapsed since 1970-01-01 00:00:00 UTC: " << seconds.count() << "\n";
}

void duration_example2()
{
    auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto dur_nano = std::chrono::duration_cast<std::chrono::nanoseconds>(duration);

    std::vector<uint8_t> data;
    msgpack::encode_msgpack(dur_nano, data);

    std::cout << "MessagePack bytes:\n" << jsoncons::byte_string_view(data) << "\n\n";

    /*
        d7, ff, // timestamp 64
        e3,94,56,e0, // nanoseconds in 30-bit unsigned int
        5f,22,b6,8b // seconds in 34-bit unsigned int         
    */ 

    auto nanoseconds = msgpack::decode_msgpack<std::chrono::nanoseconds>(data);
    std::cout << "nanoseconds elapsed since 1970-01-01 00:00:00 UTC: " << nanoseconds.count() << "\n";

    auto milliseconds = msgpack::decode_msgpack<std::chrono::milliseconds>(data);
    std::cout << "milliseconds elapsed since 1970-01-01 00:00:00 UTC: " << milliseconds.count() << "\n";

    auto seconds = msgpack::decode_msgpack<std::chrono::seconds>(data);
    std::cout << "seconds elapsed since 1970-01-01 00:00:00 UTC: " << seconds.count() << "\n";
}

void duration_example3()
{
    std::vector<uint8_t> input = {
        0xc7,0x0c,0xff, // timestamp 96
        0x3b,0x9a,0xc9,0xff, // 999999999 nanoseconds in 32-bit unsigned int
        0xff,0xff,0xff,0xff,0x7c,0x55,0x81,0x7f // -2208988801 seconds in 64-bit signed int
    };

    auto milliseconds = msgpack::decode_msgpack<std::chrono::milliseconds>(input);
    std::cout << "milliseconds elapsed since 1970-01-01 00:00:00 UTC: " << milliseconds.count() << "\n";

    auto seconds = msgpack::decode_msgpack<std::chrono::seconds>(input);
    std::cout << "seconds elapsed since 1970-01-01 00:00:00 UTC: " << seconds.count() << "\n";
}

int main()
{
    std::cout << "\nmsgpack examples\n\n";
    example1();
    example2();
    ext_example();
    duration_example1();
    duration_example2();
    duration_example3();
    std::cout << '\n';
}

