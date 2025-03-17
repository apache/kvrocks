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

TEST_CASE("serialize array to msgpack")
{
    std::vector<uint8_t> v;
    msgpack::msgpack_bytes_encoder encoder(v);
    //encoder.begin_object(1);
    encoder.begin_array(3);
    encoder.bool_value(true);
    encoder.bool_value(false);
    encoder.null_value();
    encoder.end_array();
    //encoder.end_object();
    encoder.flush();

    json result;
    REQUIRE_NOTHROW(result = msgpack::decode_msgpack<json>(v));
} 
 
TEST_CASE("serialize object to msgpack")
{
    SECTION("definite length")
    {
        std::vector<uint8_t> v;
        msgpack::msgpack_bytes_encoder encoder(v);
        encoder.begin_object(2);
        encoder.uint64_value(1);
        encoder.string_value("value1");
        encoder.uint64_value(2);
        encoder.string_value("value2");
        REQUIRE_NOTHROW(encoder.end_object());
        encoder.flush();
        json result; 
        REQUIRE_NOTHROW(result = msgpack::decode_msgpack<json>(v));
    }
}

TEST_CASE("Too many and too few items in MessagePack object or array")
{
    std::vector<uint8_t> v;
    msgpack::msgpack_bytes_encoder encoder(v);

    SECTION("Too many items in array")
    {
        CHECK(encoder.begin_array(3));
        CHECK(encoder.bool_value(true));
        CHECK(encoder.bool_value(false));
        CHECK(encoder.null_value());
        CHECK(encoder.begin_array(2));
        CHECK(encoder.string_value("cat"));
        CHECK(encoder.string_value("feline"));
        CHECK(encoder.end_array());
        REQUIRE_THROWS_WITH(encoder.end_array(), msgpack::msgpack_error_category_impl().message((int)msgpack::msgpack_errc::too_many_items).c_str());
        encoder.flush();
    }
    SECTION("Too few items in array")
    {
        CHECK(encoder.begin_array(5));
        CHECK(encoder.bool_value(true));
        CHECK(encoder.bool_value(false));
        CHECK(encoder.null_value());
        CHECK(encoder.begin_array(2));
        CHECK(encoder.string_value("cat"));
        CHECK(encoder.string_value("feline"));
        CHECK(encoder.end_array());
        REQUIRE_THROWS_WITH(encoder.end_array(), msgpack::msgpack_error_category_impl().message((int)msgpack::msgpack_errc::too_few_items).c_str());
        encoder.flush();
    }
    SECTION("Too many items in object")
    {
        CHECK(encoder.begin_object(3));
        CHECK(encoder.key("a"));
        CHECK(encoder.bool_value(true));
        CHECK(encoder.key("b"));
        CHECK(encoder.bool_value(false));
        CHECK(encoder.key("c"));
        CHECK(encoder.null_value());
        CHECK(encoder.key("d"));
        CHECK(encoder.begin_array(2));
        CHECK(encoder.string_value("cat"));
        CHECK(encoder.string_value("feline"));
        CHECK(encoder.end_array());
        REQUIRE_THROWS_WITH(encoder.end_object(), msgpack::msgpack_error_category_impl().message((int)msgpack::msgpack_errc::too_many_items).c_str());
        encoder.flush();
    }
    SECTION("Too few items in object")
    {
        CHECK(encoder.begin_object(5));
        CHECK(encoder.key("a"));
        CHECK(encoder.bool_value(true));
        CHECK(encoder.key("b"));
        CHECK(encoder.bool_value(false));
        CHECK(encoder.key("c"));
        CHECK(encoder.null_value());
        CHECK(encoder.key("d"));
        CHECK(encoder.begin_array(2));
        CHECK(encoder.string_value("cat"));
        CHECK(encoder.string_value("feline"));
        CHECK(encoder.end_array());
        REQUIRE_THROWS_WITH(encoder.end_object(), msgpack::msgpack_error_category_impl().message((int)msgpack::msgpack_errc::too_few_items).c_str());
        encoder.flush();
    }
}

struct msgpack_bytes_encoder_reset_test_fixture
{
    std::vector<uint8_t> output1;
    std::vector<uint8_t> output2;
    msgpack::msgpack_bytes_encoder encoder;

    msgpack_bytes_encoder_reset_test_fixture() : encoder(output1) {}
    std::vector<uint8_t> bytes1() const {return output1;}
    std::vector<uint8_t> bytes2() const {return output2;}
};

struct msgpack_stream_encoder_reset_test_fixture
{
    std::ostringstream output1;
    std::ostringstream output2;
    msgpack::msgpack_stream_encoder encoder;

    msgpack_stream_encoder_reset_test_fixture() : encoder(output1) {}
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

TEMPLATE_TEST_CASE("test_msgpack_encoder_reset", "",
                   msgpack_bytes_encoder_reset_test_fixture,
                   msgpack_stream_encoder_reset_test_fixture)
{
    using fixture_type = TestType;
    fixture_type f;

    std::vector<uint8_t> expected_partial =
        {
            0x92, // array(2)
                0xa3, // fixstr(3)
                    0x66, 0x6F, 0x6F // "foo"
                // second element missing
        };

    std::vector<uint8_t> expected_full =
        {
            0x92, // array(2)
                0xa3, // fixstr(3)
                    0x66, 0x6F, 0x6F, // "foo"
                0x2A // positive fixint(42)
        };

    std::vector<uint8_t> expected_partial_then_full(expected_partial);
    expected_partial_then_full.insert(expected_partial_then_full.end(),
                                      expected_full.begin(), expected_full.end());

    // Parially encode, reset, then fully encode to same sink
    f.encoder.begin_array(2);
    f.encoder.string_value("foo");
    f.encoder.flush();
    CHECK(f.bytes1() == expected_partial);
    f.encoder.reset();
    f.encoder.begin_array(2);
    f.encoder.string_value("foo");
    f.encoder.uint64_value(42);
    f.encoder.end_array();
    f.encoder.flush();
    CHECK(f.bytes1() == expected_partial_then_full);

    // Reset and encode to different sink
    f.encoder.reset(f.output2);
    f.encoder.begin_array(2);
    f.encoder.string_value("foo");
    f.encoder.uint64_value(42);
    f.encoder.end_array();
    f.encoder.flush();
    CHECK(f.bytes2() == expected_full);
}
