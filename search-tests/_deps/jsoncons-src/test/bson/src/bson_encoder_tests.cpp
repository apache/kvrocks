// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/bson/bson.hpp>
#include <jsoncons/json.hpp>

#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <limits>
#include <catch/catch.hpp>

using namespace jsoncons;

namespace {

    void test_equal(const std::vector<uint8_t>& v, const std::vector<uint8_t>& expected)
    {
        REQUIRE(v.size() == expected.size());

        for (std::size_t i = 0; i < v.size(); ++i)
        {
            CHECK(v[i] == expected[i]);
        }
    }

    void check_equal(const std::vector<uint8_t>& v, const std::vector<uint8_t>& expected)
    {
        test_equal(v, expected);
        JSONCONS_TRY
        {
            json j = bson::decode_bson<json>(v);
            std::vector<uint8_t> u;
            bson::encode_bson(j, u);
            test_equal(v,u);
        }
        JSONCONS_CATCH (const std::exception& e)
        {
            std::cout << e.what() << '\n';
        }
    }
}

TEST_CASE("serialize to bson")
{
    SECTION("array")
    {
        std::vector<uint8_t> v;
        bson::bson_bytes_encoder encoder(v);

        encoder.begin_array();
        encoder.int64_value((std::numeric_limits<int64_t>::max)());
        encoder.uint64_value((uint64_t)(std::numeric_limits<int64_t>::max)());
        encoder.double_value((std::numeric_limits<double>::max)());
        encoder.bool_value(true);
        encoder.bool_value(false);
        encoder.null_value();
        encoder.string_value("Pussy cat");
        std::vector<uint8_t> purr = {'h','i','s','s'};
        encoder.byte_string_value(purr); // default subtype is user defined
        // encoder.byte_string_value(purr, 0x80);
        encoder.end_array();
        encoder.flush();

        std::vector<uint8_t> bson = {0x4e,0x00,0x00,0x00,
                                     0x12, // int64
                                     0x30, // '0'
                                     0x00, // terminator
                                     0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x7f,
                                     0x12, // int64
                                     0x31, // '1'
                                     0x00, // terminator
                                     0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x7f,
                                     0x01, // double
                                     0x32, // '2'
                                     0x00, // terminator
                                     0xff,0xff,0xff,0xff,0xff,0xff,0xef,0x7f,
                                     0x08, // bool
                                     0x33, // '3'
                                     0x00, // terminator
                                     0x01,
                                     0x08, // bool
                                     0x34, // '4'
                                     0x00, // terminator
                                     0x00,
                                     0x0a, // null
                                     0x35, // '5'
                                     0x00, // terminator
                                     0x02, // string
                                     0x36, // '6'
                                     0x00, // terminator
                                     0x0a,0x00,0x00,0x00, // string length
                                     'P','u','s','s','y',' ','c','a','t',
                                     0x00, // terminator
                                     0x05, // binary
                                     0x37, // '7'
                                     0x00, // terminator
                                     0x04,0x00,0x00,0x00, // byte string length
                                     0x80, // subtype
                                     'h','i','s','s',
                                     0x00 // terminator
                                     };

        check_equal(v,bson);

    }

    SECTION("object")
    {
        std::vector<uint8_t> v;
        bson::bson_bytes_encoder encoder(v);

        encoder.begin_object();
        encoder.key("0");
        encoder.int64_value((std::numeric_limits<int64_t>::max)());
        encoder.key("1");
        encoder.uint64_value((uint64_t)(std::numeric_limits<int64_t>::max)());
        encoder.key("2");
        encoder.double_value((std::numeric_limits<double>::max)());
        encoder.key("3");
        encoder.bool_value(true);
        encoder.key("4");
        encoder.bool_value(false);
        encoder.key("5");
        encoder.null_value();
        encoder.key("6");
        encoder.string_value("Pussy cat");
        encoder.key("7");
        std::vector<uint8_t> hiss = {'h','i','s','s'};
        encoder.byte_string_value(hiss);
        encoder.end_object();
        encoder.flush();

        std::vector<uint8_t> bson = {0x4e,0x00,0x00,0x00,
                                     0x12, // int64
                                     0x30, // '0'
                                     0x00, // terminator
                                     0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x7f,
                                     0x12, // int64
                                     0x31, // '1'
                                     0x00, // terminator
                                     0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x7f,
                                     0x01, // double
                                     0x32, // '2'
                                     0x00, // terminator
                                     0xff,0xff,0xff,0xff,0xff,0xff,0xef,0x7f,
                                     0x08, // bool
                                     0x33, // '3'
                                     0x00, // terminator
                                     0x01,
                                     0x08, // bool
                                     0x34, // '4'
                                     0x00, // terminator
                                     0x00,
                                     0x0a, // null
                                     0x35, // '5'
                                     0x00, // terminator
                                     0x02, // string
                                     0x36, // '6'
                                     0x00, // terminator
                                     0x0a,0x00,0x00,0x00, // string length
                                     'P','u','s','s','y',' ','c','a','t',
                                     0x00, // terminator
                                     0x05, // binary
                                     0x37, // '7'
                                     0x00, // terminator
                                     0x04,0x00,0x00,0x00, // byte string length
                                     0x80, // default subtype
                                     'h','i','s','s',
                                     0x00 // terminator
                                     };
        check_equal(v,bson);
    }

    SECTION("outer object")
    {
        std::vector<uint8_t> v;
        bson::bson_bytes_encoder encoder(v);

        encoder.begin_object();
        encoder.key("a");
        encoder.begin_object();
        encoder.key("0");
        encoder.int64_value((std::numeric_limits<int64_t>::max)());
        encoder.end_object();
        encoder.end_object();
        encoder.flush();

        std::vector<uint8_t> bson = {0x18,0x00,0x00,0x00,
                                     0x03, // object
                                     'a',
                                     0x00,
                                     0x10,0x00,0x00,0x00,
                                     0x12, // int64
                                     0x30, // '0'
                                     0x00, // terminator
                                     0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x7f,
                                     0x00, // terminator
                                     0x00 // terminator
                                     };
        check_equal(v,bson);
    }

    SECTION("outer array")
    {
        std::vector<uint8_t> v;
        bson::bson_bytes_encoder encoder(v);

        encoder.begin_array();
        encoder.begin_object();
        encoder.key("0");
        encoder.int64_value((std::numeric_limits<int64_t>::max)());
        encoder.end_object();
        encoder.end_array();
        encoder.flush();

        std::vector<uint8_t> bson = {0x18,0x00,0x00,0x00,
                                     0x03, // object
                                     '0',
                                     0x00,
                                     0x10,0x00,0x00,0x00,
                                     0x12, // int64
                                     0x30, // '0'
                                     0x00, // terminator
                                     0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x7f,
                                     0x00, // terminator
                                     0x00 // terminator
                                     };
        check_equal(v,bson);
    }
}

TEST_CASE("serialize object to bson")
{
    std::vector<uint8_t> v;
    bson::bson_bytes_encoder encoder(v);

    encoder.begin_object();
    encoder.key("null");
    encoder.null_value();
    encoder.end_object();
    encoder.flush();

    JSONCONS_TRY
    {
        json result = bson::decode_bson<json>(v);
    }
    JSONCONS_CATCH (const std::exception& e)
    {
        std::cout << e.what() << '\n';
    }
}

struct bson_bytes_encoder_reset_test_fixture
{
    std::vector<uint8_t> output1;
    std::vector<uint8_t> output2;
    bson::bson_bytes_encoder encoder;

    bson_bytes_encoder_reset_test_fixture() : encoder(output1) {}
    std::vector<uint8_t> bytes1() const {return output1;}
    std::vector<uint8_t> bytes2() const {return output2;}
};

struct bson_stream_encoder_reset_test_fixture
{
    std::ostringstream output1;
    std::ostringstream output2;
    bson::bson_stream_encoder encoder;

    bson_stream_encoder_reset_test_fixture() : encoder(output1) {}
    std::vector<uint8_t> bytes1() const {return bytes_of(output1);}
    std::vector<uint8_t> bytes2() const {return bytes_of(output2);}

private:
    static std::vector<uint8_t> bytes_of(const std::ostringstream& os)
    {
        auto str = os.str();
        auto data = reinterpret_cast<const uint8_t*>(str.data());
        std::vector<uint8_t> bytes(data, data + str.size());
        return bytes;
    }
};

TEMPLATE_TEST_CASE("test_bson_encoder_reset", "",
                   bson_bytes_encoder_reset_test_fixture,
                   bson_stream_encoder_reset_test_fixture)
{
    using fixture_type = TestType;
    fixture_type f;

    std::vector<uint8_t> expected_full = {
        0x0C, 0x00, 0x00, 0x00, // Document: 12 bytes
        0x10, // int32 field type
        0x62, 0x00, // "b" field name
        0x02, 0x00, 0x00, 0x00, // int32(2) field value
        0x00, // end of object marker
    };

    // Partially encode, reset, then fully encode to same sink.
    // Note that partial BSON output is empty when flushed due to the
    // unknown document byte length.
    f.encoder.begin_object(1);
    f.encoder.key("a");
    f.encoder.flush();
    CHECK(f.bytes1().empty());
    f.encoder.reset();
    f.encoder.begin_object(1);
    f.encoder.key("b");
    f.encoder.uint64_value(2);
    f.encoder.end_object();
    f.encoder.flush();
    CHECK(f.bytes1() == expected_full);

    // Reset and encode to different sink
    f.encoder.reset(f.output2);
    f.encoder.begin_object(1);
    f.encoder.key("b");
    f.encoder.uint64_value(2);
    f.encoder.end_object();
    f.encoder.flush();
    CHECK(f.bytes2() == expected_full);
}
