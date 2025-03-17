// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/bson/bson.hpp>
#include <jsoncons/json.hpp>

#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <fstream>
#include <catch/catch.hpp>

using namespace jsoncons;

namespace
{
    std::vector<char> read_bytes(const std::string& filename)
    {
        std::vector<char> bytes;
        std::ifstream is(filename, std::ifstream::binary);
        REQUIRE(is);
        is.seekg (0, is.end);
        std::streamoff length = is.tellg();
        is.seekg (0, is.beg);
        bytes.resize(static_cast<std::size_t>(length));
        is.read(bytes.data(), static_cast<std::size_t>(length));
        is.close();
        return bytes;
    }
}

// Test data is from https://github.com/mongodb/mongo-c-driver/tree/master/src/libbson

TEST_CASE("bson c test suite")
{
    SECTION("utf8")
    {
        std::string in_file = "./bson/input/test11.bson";
        std::vector<char> bytes = read_bytes(in_file);

        json j;
        j.try_emplace("hello", "world"); 

        std::vector<char> bytes2;
        REQUIRE_NOTHROW(bson::encode_bson(j, bytes2));
        //std::cout << byte_string_view(bytes2) << "\n\n";
        //std::cout << byte_string_view(bytes) << "\n\n";
        CHECK(bytes2 == bytes);

        auto b2 = bson::decode_bson<json>(bytes);
        CHECK(b2 == j);
    }
    SECTION("null")
    {
        std::string in_file = "./bson/input/test18.bson";
        std::vector<char> bytes = read_bytes(in_file);

        json j;
        j.try_emplace("hello", null_type()); 

        std::vector<char> bytes2;
        REQUIRE_NOTHROW(bson::encode_bson(j, bytes2));
        //std::cout << byte_string_view(bytes2) << "\n\n";
        //std::cout << byte_string_view(bytes) << "\n\n";
        CHECK(bytes2 == bytes);

        auto b2 = bson::decode_bson<json>(bytes);
        CHECK(b2 == j);
    }
    SECTION("bool")
    {
        std::string in_file = "./bson/input/test19.bson";
        std::vector<char> bytes = read_bytes(in_file);

        std::vector<char> bytes2;
        std::map<std::string, bool> m = { {"bool", true} };
        REQUIRE_NOTHROW(bson::encode_bson(m, bytes2));
        CHECK(bytes2 == bytes);

        auto m2 = bson::decode_bson<std::map<std::string, bool>>(bytes);
        CHECK(m2 == m);
    }
    SECTION("double")
    {
        std::string in_file = "./bson/input/test20.bson";
        std::vector<char> bytes = read_bytes(in_file);

        std::vector<char> bytes2;
        std::map<std::string, double> m = { {"double", 123.4567} };
        REQUIRE_NOTHROW(bson::encode_bson(m, bytes2));
        CHECK(bytes2 == bytes);

        auto m2 = bson::decode_bson<std::map<std::string, double>>(bytes);
        CHECK(m2 == m);
    }
    SECTION("document")
    {
        std::string in_file = "./bson/input/test21.bson";
        std::vector<char> bytes = read_bytes(in_file);

        json b;
        b.try_emplace("document", json()); 

        std::vector<char> bytes2;
        REQUIRE_NOTHROW(bson::encode_bson(b, bytes2));
        //std::cout << byte_string_view(bytes2) << "\n\n";
        //std::cout << byte_string_view(bytes) << "\n\n";
        CHECK(bytes2 == bytes);

        auto b2 = bson::decode_bson<json>(bytes);
        CHECK(b2 == b);
    }
    SECTION("oid")
    {
        std::string in_file = "./bson/input/test22.bson";
        std::vector<char> input = read_bytes(in_file);

        json j = bson::decode_bson<json>(input);
        bson::oid_t oid("1234567890abcdef1234abcd");

        std::string s;
        to_string(oid, s);
        CHECK(j.at("oid").as<std::string>() == s);
        CHECK(j.at("oid").tag() == semantic_tag::id);

        std::vector<char> output;
        bson::encode_bson(j, output);

        CHECK(output == input);
    }
    SECTION("array")
    {
        std::string in_file = "./bson/input/test23.bson";
        std::vector<char> bytes = read_bytes(in_file);

        std::vector<char> bytes2;
        ojson a(json_array_arg);
        a.push_back("hello");
        a.push_back("world");

        ojson b;
        b["array"] = std::move(a);

        REQUIRE_NOTHROW(bson::encode_bson(b, bytes2));
        CHECK(bytes2 == bytes);

        auto b2 = bson::decode_bson<ojson>(bytes);
        CHECK(b2 == b);
    }
    SECTION("binary")
    {
        std::string in_file = "./bson/input/test24.bson";
        std::vector<char> bytes = read_bytes(in_file);

        std::vector<char> bytes2;
        std::vector<uint8_t> bstr = {'1', '2', '3', '4'};

        json b;
        b.try_emplace("binary", byte_string_arg, bstr, 0x80);

        REQUIRE_NOTHROW(bson::encode_bson(b, bytes2));
        //std::cout << byte_string_view(bytes2) << "\n\n";
        //std::cout << byte_string_view(bytes) << "\n\n";
        CHECK(bytes2 == bytes);

        auto b2 = bson::decode_bson<json>(bytes);
        CHECK(b2 == b);
    }
    SECTION("binary (jsoncons default)")
    {
        std::string in_file = "./bson/input/test24.bson";
        std::vector<char> bytes = read_bytes(in_file);

        std::vector<char> bytes2;
        std::vector<uint8_t> bstr = {'1', '2', '3', '4'};

        json b;
        b.try_emplace("binary", byte_string_arg, bstr); // default subtype is user defined

        REQUIRE_NOTHROW(bson::encode_bson(b, bytes2));
        //std::cout << byte_string_view(bytes2) << "\n\n";
        //std::cout << byte_string_view(bytes) << "\n\n";
        CHECK(bytes2 == bytes);

        auto b2 = bson::decode_bson<json>(bytes);
        CHECK(b2 == b);
    }
    SECTION("undefined")
    {
        std::string in_file = "./bson/input/test25.bson";
        std::vector<char> input = read_bytes(in_file);

        json j = bson::decode_bson<json>(input);
        //std::cout << j << "\n";

        CHECK(j.at("undefined") == json::null());
        CHECK(j.at("undefined").tag() == semantic_tag::undefined);

        std::vector<char> output;
        bson::encode_bson(j, output);

        CHECK(output == input);
    }
    SECTION("time")
    {
        std::string in_file = "./bson/input/test26.bson";
        std::vector<char> input = read_bytes(in_file);

        json j = bson::decode_bson<json>(input);

        int64_t expected = 1234567890000; // milliseconds
        CHECK(expected == j.at("time_t").as<int64_t>());
        CHECK(j.at("time_t").tag() == semantic_tag::epoch_milli);

        std::vector<char> output;
        bson::encode_bson(j, output);

        CHECK(output == input);
    }
    SECTION("regex")
    {
        std::string in_file = "./bson/input/test27.bson";
        std::vector<char> input = read_bytes(in_file);

        json j = bson::decode_bson<json>(input);
        //std::cout << j << "\n";

        std::string expected = "/^abcd/ilx";
        CHECK(expected == j.at("regex").as<std::string>());
        CHECK(j.at("regex").tag() == semantic_tag::regex);

        std::vector<char> output;
        bson::encode_bson(j, output);

        CHECK(output == input);
    }
    SECTION("code")
    {
        std::string in_file = "./bson/input/test29.bson";
        std::vector<char> input = read_bytes(in_file);

        json j = bson::decode_bson<json>(input);

        std::string expected = "var a = {};";
        CHECK(expected == j.at("code").as<std::string>());
        CHECK(j.at("code").tag() == semantic_tag::code);

        std::vector<char> output;
        bson::encode_bson(j, output);

        CHECK(output == input);
    }
    SECTION("int32")
    {
        std::string in_file = "./bson/input/test33.bson";
        std::vector<char> bytes = read_bytes(in_file);

        std::vector<char> bytes2;
        ojson j(json_object_arg);
        j.try_emplace("a", -123);
        j.try_emplace("c", 0);
        j.try_emplace("b", 123);
        REQUIRE_NOTHROW(bson::encode_bson(j, bytes2));
        CHECK(bytes2 == bytes);

        auto j2 = bson::decode_bson<ojson>(bytes);
        CHECK(j2 == j);
    }
    SECTION("int64")
    {
        uint8_t b;

        std::string in_file = "./bson/input/test34.bson";
        std::vector<char> bytes = read_bytes(in_file);

        bytes_source source(bytes);

        uint8_t buf[sizeof(int64_t)]; 

        source.read(buf, sizeof(int32_t));
        auto doc_size = binary::little_to_native<int32_t>(buf, sizeof(buf));
        REQUIRE(doc_size == 16);

        REQUIRE(source.read(&b, 1) == 1);
        REQUIRE(b == 0x12); // 64-bit integer
        std::string s;
        while (source.read(&b, 1) == 1 && b != 0)
        {
            s.push_back(b);
        }
        REQUIRE(s == std::string("a"));
        source.read(buf, sizeof(int64_t));
        auto val = binary::little_to_native<int64_t>(buf, sizeof(int64_t));
        CHECK(val == 100000000000000ULL);
        REQUIRE(source.read(&b, 1) == 1);
        CHECK(b == 0);
        CHECK(source.eof());

        std::vector<char> bytes2;
        std::map<std::string, int64_t> m{ {"a", val} };
        REQUIRE_NOTHROW(bson::encode_bson(m, bytes2));
        CHECK(bytes2 == bytes);

        auto m2 = bson::decode_bson<std::map<std::string, int64_t>>(bytes);
        CHECK(m2 == m);
    }
    SECTION("decimal128")
    {
        std::string in_file = "./bson/input/test58.bson";
        std::vector<char> input = read_bytes(in_file);

        json j = bson::decode_bson<json>(input);
        std::cout << j << "\n";
        std::cout << j.tag() << "\n";
        bson::decimal128_t dec(0,1);
        char buf[bson::decimal128_limits::buf_size];
        auto rc = bson::decimal128_to_chars(buf,buf+sizeof(buf),dec);
        //std::cout << "128: " << std::string(buf,rc.ptr) << "\n"; 
        CHECK(j.at("a") == json(std::string(buf,rc.ptr)));
        CHECK(j.at("a").tag() == semantic_tag::float128);

        std::vector<char> output;
        bson::encode_bson(j, output);

        CHECK(output == input);
    }
    SECTION("document")
    {
        std::string in_file = "./bson/input/test21.bson";
        std::vector<char> bytes = read_bytes(in_file);

        json b;
        b.try_emplace("document", json()); 

        std::vector<char> bytes2;
        REQUIRE_NOTHROW(bson::encode_bson(b, bytes2));
        //std::cout << byte_string_view(bytes2) << "\n\n";
        //std::cout << byte_string_view(bytes) << "\n\n";
        CHECK(bytes2 == bytes);

        auto b2 = bson::decode_bson<json>(bytes);
        CHECK(b2 == b);
    }
    SECTION("oid")
    {
        std::string in_file = "./bson/input/test22.bson";
        std::vector<char> input = read_bytes(in_file);

        json j = bson::decode_bson<json>(input);
        bson::oid_t oid("1234567890abcdef1234abcd");

        std::string s;
        to_string(oid, s);
        CHECK(j.at("oid").as<std::string>() == s);
        CHECK(j.at("oid").tag() == semantic_tag::id);

        std::vector<char> output;
        bson::encode_bson(j, output);

        CHECK(output == input);
    }
    SECTION("regex")
    {
        std::string in_file = "./bson/input/test27.bson";
        std::vector<char> input = read_bytes(in_file);

        json j = bson::decode_bson<json>(input);
        //std::cout << j << "\n";

        std::string expected = "/^abcd/ilx";

        std::vector<char> output;
        bson::encode_bson(j, output);

        CHECK(output == input);
    }
}

