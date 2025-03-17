// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/msgpack/msgpack.hpp>
#include <jsoncons/json.hpp>

#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <limits>
#include <catch/catch.hpp>

using namespace jsoncons;
using namespace jsoncons::msgpack;

TEST_CASE("msgpack timestamp tests")
{
    SECTION("test 1")
    {
        std::vector<uint8_t> u = {0xce,0x5a,0x4a,0xf6,0xa5};
        uint64_t expected = decode_msgpack<uint64_t>(u);
        CHECK(expected == 1514862245);

        std::vector<uint8_t> input = {0xd6,0xff,0x5a,0x4a,0xf6,0xa5};
        auto r = decode_msgpack<uint64_t>(input);

        CHECK(expected == r);

        std::vector<uint8_t> output;
        json j = decode_msgpack<json>(input);
        encode_msgpack(j,output);
        CHECK(output == input);
    }
    SECTION("test 2")
    {
        std::vector<uint64_t> expected = {1514862245,678901234};

        std::vector<uint8_t> input = {0xd7,0xff,0xa1,0xdc,0xd7,0xc8,0x5a,0x4a,0xf6,0xa5};
        auto r = decode_msgpack<json>(input);
        std::cout << pretty_print(r) << "\n\n";

        //CHECK(expected == r);
    }
    SECTION("test 3")
    {
        std::vector<int64_t> expected = {-int64_t(2208988801),999999999};

        std::vector<uint8_t> input = {0xc7,0x0c,0xff,0x3b,0x9a,0xc9,0xff,0xff,0xff,0xff,0xff,0x7c,0x55,0x81,0x7f};
        auto r = decode_msgpack<json>(input);
        std::cout << pretty_print(r) << "\n\n";

        //CHECK(expected == r);

        //std::vector<uint8_t> output;
        //json j = decode_msgpack<json>(input);
        //encode_msgpack(j,output);
        //CHECK(output == input);
    }
    SECTION("test 4")
    {
        std::vector<uint64_t> expected = {2147483648,1};

        std::vector<uint8_t> input = {0xd7,0xff,0x00,0x00,0x00,0x04,0x80,0x00,0x00,0x00};
        auto r = decode_msgpack<json>(input);
        std::cout << r << "\n\n";

        //CHECK(expected == r);

        //std::vector<uint8_t> output;
        //json j = decode_msgpack<json>(input);
        //encode_msgpack(j,output);
        //CHECK(output == input);
    }

    SECTION("test 5")
    {
        std::vector<int64_t> expected = {-int64_t(2208988801),999999999};

        std::vector<uint8_t> input = {
            0xc7,0x0c,0xff, // timestamp 96
            0x3b,0x9a,0xc9,0xff, // 999999999 nanoseconds in 32-bit unsigned int
            0xff,0xff,0xff,0xff,0x7c,0x55,0x81,0x7f // -2208988801 seconds in 64-bit signed int
        };

        auto j = decode_msgpack<json>(input);
        std::cout << "j: " << j << "\n\n";
        auto milliseconds = j.as<std::chrono::milliseconds>();
        std::cout << "milliseconds elapsed since 1970-01-01 00:00:00 UTC: " << milliseconds.count() << "\n";

        std::vector<uint8_t> data;
        msgpack::encode_msgpack(milliseconds, data);
        std::cout << "MessagePack bytes:\n" << jsoncons::byte_string_view(data) << "\n\n";
        auto j2 = decode_msgpack<json>(data);
        std::cout << "j2: " << j2 << "\n\n";
    }
}

