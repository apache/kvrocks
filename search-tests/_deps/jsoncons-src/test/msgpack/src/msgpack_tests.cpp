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

TEST_CASE("msgpack_test")
{
    json j1;
    j1["zero"] = 0;
    j1["one"] = 1;
    j1["two"] = 2;
    j1["null"] = null_type();
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
    j1["String too long for small string optimization"] = "String too long for small string optimization"; 

    json ja(json_array_arg);
    ja.push_back(0);
    ja.push_back(1);
    ja.push_back(2);
    ja.push_back(null_type());
    ja.push_back(true);
    ja.push_back(false);
    ja.push_back((std::numeric_limits<int64_t>::max)());
    ja.push_back((std::numeric_limits<uint64_t>::max)());
    ja.push_back((std::numeric_limits<int64_t>::lowest)());
    ja.push_back((std::numeric_limits<int32_t>::max)());
    ja.push_back((std::numeric_limits<uint32_t>::max)());
    ja.push_back((std::numeric_limits<int32_t>::lowest)());
    ja.push_back((std::numeric_limits<int16_t>::max)());
    ja.push_back((std::numeric_limits<uint16_t>::max)());
    ja.push_back((std::numeric_limits<int16_t>::lowest)());
    ja.push_back((std::numeric_limits<int8_t>::max)());
    ja.push_back((std::numeric_limits<uint8_t>::max)());
    ja.push_back((std::numeric_limits<int8_t>::lowest)());
    ja.push_back((std::numeric_limits<double>::max)());
    ja.push_back((std::numeric_limits<double>::lowest)());
    ja.push_back((std::numeric_limits<float>::max)());
    ja.push_back(0.0);
    ja.push_back((std::numeric_limits<float>::lowest)());
    ja.push_back("String too long for small string optimization");

    j1["An array"] = ja; 

    std::vector<uint8_t> v;
    encode_msgpack(j1, v);

    // from bytes
    json j2 = decode_msgpack<json>(v);
    CHECK(j2 == j1);

    // from pair of iterators
    json j3 = decode_msgpack<json>(v.begin(), v.end());
    CHECK(j3 == j1);
} 

TEST_CASE("msgpack_test2")
{
    wjson j1;
    j1[L"zero"] = 0;
    j1[L"one"] = 1;
    j1[L"two"] = 2;
    j1[L"null"] = null_type();
    j1[L"true"] = true;
    j1[L"false"] = false;
    j1[L"max int64_t"] = (std::numeric_limits<int64_t>::max)();
    j1[L"max uint64_t"] = (std::numeric_limits<uint64_t>::max)();
    j1[L"min int64_t"] = (std::numeric_limits<int64_t>::lowest)();
    j1[L"max int32_t"] = (std::numeric_limits<int32_t>::max)();
    j1[L"max uint32_t"] = (std::numeric_limits<uint32_t>::max)();
    j1[L"min int32_t"] = (std::numeric_limits<int32_t>::lowest)();
    j1[L"max int16_t"] = (std::numeric_limits<int16_t>::max)();
    j1[L"max uint16_t"] = (std::numeric_limits<uint16_t>::max)();
    j1[L"min int16_t"] = (std::numeric_limits<int16_t>::lowest)();
    j1[L"max int8_t"] = (std::numeric_limits<int8_t>::max)();
    j1[L"max uint8_t"] = (std::numeric_limits<uint8_t>::max)();
    j1[L"min int8_t"] = (std::numeric_limits<int8_t>::lowest)();
    j1[L"max double"] = (std::numeric_limits<double>::max)();
    j1[L"min double"] = (std::numeric_limits<double>::lowest)();
    j1[L"max float"] = (std::numeric_limits<float>::max)();
    j1[L"zero float"] = 0.0;
    j1[L"min float"] = (std::numeric_limits<float>::lowest)();
    j1[L"S"] = L"S";
    j1[L"String too long for small string optimization"] = L"String too long for small string optimization";

    wjson ja(json_array_arg);
    ja.push_back(0);
    ja.push_back(1);
    ja.push_back(2);
    ja.push_back(null_type());
    ja.push_back(true);
    ja.push_back(false);
    ja.push_back((std::numeric_limits<int64_t>::max)());
    ja.push_back((std::numeric_limits<uint64_t>::max)());
    ja.push_back((std::numeric_limits<int64_t>::lowest)());
    ja.push_back((std::numeric_limits<int32_t>::max)());
    ja.push_back((std::numeric_limits<uint32_t>::max)());
    ja.push_back((std::numeric_limits<int32_t>::lowest)());
    ja.push_back((std::numeric_limits<int16_t>::max)());
    ja.push_back((std::numeric_limits<uint16_t>::max)());
    ja.push_back((std::numeric_limits<int16_t>::lowest)());
    ja.push_back((std::numeric_limits<int8_t>::max)());
    ja.push_back((std::numeric_limits<uint8_t>::max)());
    ja.push_back((std::numeric_limits<int8_t>::lowest)());
    ja.push_back((std::numeric_limits<double>::max)());
    ja.push_back((std::numeric_limits<double>::lowest)());
    ja.push_back((std::numeric_limits<float>::max)());
    ja.push_back(0.0);
    ja.push_back((std::numeric_limits<float>::lowest)());
    ja.push_back(L"S");
    ja.push_back(L"String too long for small string optimization");

    j1[L"An array"] = ja;

    std::vector<uint8_t> v;
    encode_msgpack(j1, v);

    //wjson j2 = decode_msgpack<wjson>(v);

    //CHECK(j2 == j1);
}

// Includes test cases from https://github.com/kawanet/msgpack-test-suite, MIT license

TEST_CASE("msgpack bin tests")
{
    SECTION("[]")
    {
        std::vector<uint8_t> expected;

        std::vector<uint8_t> input1 = {0xc4,0x00};
        auto v1 = decode_msgpack<std::vector<uint8_t>>(input1);
        CHECK(expected == v1);

        std::vector<uint8_t> input2 = {0xc5,0x00,0x00};
        auto v2 = decode_msgpack<std::vector<uint8_t>>(input2);
        CHECK(expected == v2);

        std::vector<uint8_t> input3 = {0xc6,0x00,0x00,0x00,0x00};
        auto v3 = decode_msgpack<std::vector<uint8_t>>(input3);
        CHECK(expected == v3);

        std::vector<uint8_t> output1;
        encode_msgpack(byte_string_view(v1),output1);
        CHECK(output1 == input1);
    }
    SECTION("[1]")
    {
        std::vector<uint8_t> expected = {1};

        std::vector<uint8_t> input1 = {0xc4,0x01,0x01};
        auto v1 = decode_msgpack<std::vector<uint8_t>>(input1);
        CHECK(expected == v1);

        std::vector<uint8_t> input2 = {0xc5,0x00,0x01,0x01};
        auto v2 = decode_msgpack<std::vector<uint8_t>>(input2);
        CHECK(expected == v2);

        std::vector<uint8_t> input3 = {0xc6,0x00,0x00,0x00,0x01,0x01};
        auto v3 = decode_msgpack<std::vector<uint8_t>>(input3);
        CHECK(expected == v3);

        std::vector<uint8_t> output1;
        encode_msgpack(byte_string_view(v1),output1);
        CHECK(output1 == input1);
    }
    SECTION("[0,255]")
    {
        std::vector<uint8_t> expected = {0,255};

        std::vector<uint8_t> input1 = {0xc4,0x02,0x00,0xff};
        auto v1 = decode_msgpack<std::vector<uint8_t>>(input1);
        CHECK(expected == v1);

        std::vector<uint8_t> input2 = {0xc5,0x00,0x02,0x00,0xff};
        auto v2 = decode_msgpack<std::vector<uint8_t>>(input2);
        CHECK(expected == v2);

        std::vector<uint8_t> input3 = {0xc6,0x00,0x00,0x00,0x02,0x00,0xff};
        auto v3 = decode_msgpack<std::vector<uint8_t>>(input3);
        CHECK(expected == v3);

        std::vector<uint8_t> output1;
        encode_msgpack(byte_string_view(v1),output1);
        CHECK(output1 == input1);
    }
}

TEST_CASE("msgpack ext tests")
{
    SECTION("fixext1, 1, [0x10]")
    {
        std::vector<uint8_t> expected = {0x10};

        std::vector<uint8_t> input = {0xd4,0x01,0x10};
        auto v = decode_msgpack<std::vector<uint8_t>>(input);
        CHECK(expected == v);

        auto j = decode_msgpack<json>(input);
        std::vector<uint8_t> output;
        encode_msgpack(j, output);
        CHECK(output == input);
    }
    SECTION("fixext2, 2, [20,21]")
    {
        std::vector<uint8_t> expected = {0x20,0x21};

        std::vector<uint8_t> input = {0xd5,0x02,0x20,0x21};
        auto v = decode_msgpack<std::vector<uint8_t>>(input);
        CHECK(expected == (v));

        auto j = decode_msgpack<json>(input);
        std::vector<uint8_t> output;
        encode_msgpack(j, output);
        CHECK(output == input);
    }

    SECTION("fixext4, 3, [0x30,0x31,0x32,0x33]")
    {
        std::vector<uint8_t> expected = {0x30,0x31,0x32,0x33};

        std::vector<uint8_t> input = {0xd6,0x03,0x30,0x31,0x32,0x33};
        auto v = decode_msgpack<std::vector<uint8_t>>(input);
        CHECK(expected == v);

        auto j = decode_msgpack<json>(input);
        std::vector<uint8_t> output;
        encode_msgpack(j, output);
        CHECK(output == input);
    }

    SECTION("fixext8, 4, [0x40,0x41,0x42,0x43,0x44,0x45,0x46,0x47]")
    {
        std::vector<uint8_t> expected = {0x40,0x41,0x42,0x43,0x44,0x45,0x46,0x47};

        std::vector<uint8_t> input = {0xd7,0x04,0x40,0x41,0x42,0x43,0x44,0x45,0x46,0x47};
        auto v = decode_msgpack<std::vector<uint8_t>>(input);
        CHECK(expected == v);

        auto j = decode_msgpack<json>(input);
        std::vector<uint8_t> output;
        encode_msgpack(j, output);
        CHECK(output == input);
    }

    SECTION("fixext16, 5, [0x50,0x51,0x52,0x53,0x54,0x55,0x56,0x57,0x58,0x59,0x5a,0x5b,0x5c,0x5d,0x5e,0x5f]")
    {
        std::vector<uint8_t> expected = {0x50,0x51,0x52,0x53,0x54,0x55,0x56,0x57,0x58,0x59,0x5a,0x5b,0x5c,0x5d,0x5e,0x5f};

        std::vector<uint8_t> input = {0xd8,0x05,0x50,0x51,0x52,0x53,0x54,0x55,0x56,0x57,0x58,0x59,0x5a,0x5b,0x5c,0x5d,0x5e,0x5f};
        auto v = decode_msgpack<std::vector<uint8_t>>(input);
        CHECK(expected == v);

        auto j = decode_msgpack<json>(input);
        std::vector<uint8_t> output;
        encode_msgpack(j, output);
        CHECK(output == input);
    }

    SECTION("ext, size 0")
    {
        std::vector<uint8_t> expected = {};

        // ext8
        std::vector<uint8_t> input1 = {0xc7,0x00,0x06}; 
        auto v = decode_msgpack<std::vector<uint8_t>>(input1);
        CHECK(expected == v);

        auto j1 = decode_msgpack<json>(input1);
        std::vector<uint8_t> output1;
        encode_msgpack(j1, output1);
        CHECK(output1 == input1);

        // ext16
        std::vector<uint8_t> input2 = {0xc8,0x00,0x00,0x06};
        auto v2 = decode_msgpack<std::vector<uint8_t>>(input2);
        CHECK(expected == v2);

        auto j2 = decode_msgpack<json>(input2);
        std::vector<uint8_t> output2;
        encode_msgpack(j2, output2);
        CHECK(output2 == input1);

        // ext32
        std::vector<uint8_t> input3 = {0xc9,0x00,0x00,0x00,0x00,0x06};
        auto v3 = decode_msgpack<std::vector<uint8_t>>(input3);
        CHECK(expected == v3);

        auto j3 = decode_msgpack<json>(input3);
        std::vector<uint8_t> output3;
        encode_msgpack(j3, output3);
        CHECK(output3 == input1);
    }

    SECTION("ext, size 3")
    {
        std::vector<uint8_t> expected = {0x70,0x71,0x72};

        // ext8
        std::vector<uint8_t> input1 = {0xc7,0x03,0x07,0x70,0x71,0x72}; 
        auto v = decode_msgpack<std::vector<uint8_t>>(input1);
        CHECK(expected == v);

        auto j1 = decode_msgpack<json>(input1);
        std::vector<uint8_t> output1;
        encode_msgpack(j1, output1);
        CHECK(output1 == input1);

        // ext16
        std::vector<uint8_t> input2 = {0xc8,0x00,0x03,0x07,0x70,0x71,0x72};
        auto v2 = decode_msgpack<std::vector<uint8_t>>(input2);
        CHECK(expected == v2);

        auto j2 = decode_msgpack<json>(input2);
        std::vector<uint8_t> output2;
        encode_msgpack(j2, output2);
        CHECK(output2 == input1);

        // ext32
        std::vector<uint8_t> input3 = {0xc9,0x00,0x00,0x00,0x03,0x07,0x70,0x71,0x72};
        auto v3 = decode_msgpack<std::vector<uint8_t>>(input3);
        CHECK(expected == v3);

        auto j3 = decode_msgpack<json>(input3);
        std::vector<uint8_t> output3;
        encode_msgpack(j3, output3);
        CHECK(output3 == input1);
    }
}

namespace { namespace ns {

    struct Person
    {
        std::string name;
    };

}}

JSONCONS_ALL_MEMBER_TRAITS(ns::Person, name)

TEST_CASE("encode_msgpack overloads")
{
    SECTION("json, stream")
    {
        json person;
        person.try_emplace("name", "John Smith");

        std::string s;
        std::stringstream ss(s);
        msgpack::encode_msgpack(person, ss);
        json other = msgpack::decode_msgpack<json>(ss);
        CHECK(other == person);
    }
    SECTION("custom, stream")
    {
        ns::Person person{"John Smith"};

        std::string s;
        std::stringstream ss(s);
        msgpack::encode_msgpack(person, ss);
        ns::Person other = msgpack::decode_msgpack<ns::Person>(ss);
        CHECK(other.name == person.name);
    }
}

#if (defined(__GNUC__) || defined(__clang__)) && defined(JSONCONS_HAS_INT128) 
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
TEST_CASE("msgpack json constructor __int64 tests")
{
    SECTION("test 1")
    {
        json j1("-18446744073709551617", semantic_tag::bigint);

        __int128 val1 = j1.as<__int128>();

        std::vector<uint8_t> expected = {0xb5,0x2d,0x31,0x38,0x34,0x34,0x36,0x37,0x34,0x34,0x30,0x37,0x33,0x37,0x30,0x39,0x35,0x35,0x31,0x36,0x31,0x37};
        std::vector<uint8_t> data;
        msgpack::encode_msgpack(val1, data);
        CHECK(expected == data);

        auto val2 = msgpack::decode_msgpack<__int128>(data);

        CHECK((val2 == val1));
    }
}
TEST_CASE("msgpack json constructor unsigned __int64 tests")
{
    SECTION("test 1")
    {
        json j1("18446744073709551616", semantic_tag::bigint);

        auto val1 = j1.as<unsigned __int128>();

        std::vector<uint8_t> expected = {0xb4,0x31,0x38,0x34,0x34,0x36,0x37,0x34,0x34,0x30,0x37,0x33,0x37,0x30,0x39,0x35,0x35,0x31,0x36,0x31,0x36};
        std::vector<uint8_t> data;
        msgpack::encode_msgpack(val1, data);
        CHECK(expected == data);

        auto val2 = msgpack::decode_msgpack<unsigned __int128>(data);

        CHECK((val2 == val1));
    }
}
#pragma GCC diagnostic pop
#endif

