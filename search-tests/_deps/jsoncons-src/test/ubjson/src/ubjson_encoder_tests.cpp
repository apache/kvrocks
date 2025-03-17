// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/ubjson/ubjson.hpp>

#include <jsoncons/json.hpp>

#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <limits>
#include <catch/catch.hpp>

using namespace jsoncons;
using namespace jsoncons::ubjson;

namespace ns {
    class hiking_reputon
    {
        std::vector<double> x_;
    public:
        hiking_reputon(std::vector<double>&& x)
            : x_(std::move(x))
        {
        }
        const std::vector<double>& x() const {return x_;}
        friend bool operator==(const hiking_reputon& lhs, const hiking_reputon& rhs)
        {
            return lhs.x_ == rhs.x_;
        }
        friend bool operator!=(const hiking_reputon& lhs, const hiking_reputon& rhs)
        {
            return !(lhs == rhs);
        };
    };
} // namespace ns

// Declare the traits. Specify which data members need to be serialized.
JSONCONS_ALL_CTOR_GETTER_TRAITS(ns::hiking_reputon, x)

TEST_CASE("serialize array to ubjson")
{
    std::vector<uint8_t> v;
    ubjson::ubjson_bytes_encoder encoder(v);
    encoder.begin_array(3);
    encoder.bool_value(true);
    encoder.bool_value(false);
    encoder.null_value();
    encoder.end_array();
    encoder.flush();

    JSONCONS_TRY
    {
        json result = decode_ubjson<json>(v);
        std::cout << result << '\n';
    }
    JSONCONS_CATCH (const std::exception& e)
    {
        std::cout << e.what() << '\n';
    }
} 

TEST_CASE("Too many and too few items in UBJSON object or array")
{
    std::vector<uint8_t> v;
    ubjson::ubjson_bytes_encoder encoder(v);

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
        REQUIRE_THROWS_WITH(encoder.end_array(), ubjson_error_category_impl().message((int)ubjson_errc::too_many_items).c_str());
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
        REQUIRE_THROWS_WITH(encoder.end_array(), ubjson_error_category_impl().message((int)ubjson_errc::too_few_items).c_str());
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
        REQUIRE_THROWS_WITH(encoder.end_object(), ubjson_error_category_impl().message((int)ubjson_errc::too_many_items).c_str());
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
        REQUIRE_THROWS_WITH(encoder.end_object(), ubjson_error_category_impl().message((int)ubjson_errc::too_few_items).c_str());
        encoder.flush();
    }
}

TEST_CASE("serialize big array to ubjson")
{
    std::vector<double> x; x.resize(16777217);
    for (std::size_t i = 0; i < x.size(); ++i) { x[i] = (double)i; }
    ns::hiking_reputon val(std::move(x));

    // Encode a ns::hiking_reputation value to UBJSON
    std::vector<uint8_t> data;
    jsoncons::ubjson::encode_ubjson(val, data);

    auto options = jsoncons::ubjson::ubjson_options{}
        .max_items((std::numeric_limits<int32_t>::max)());
    ns::hiking_reputon val2 = jsoncons::ubjson::decode_ubjson<ns::hiking_reputon>(data, options);

    CHECK(val2 == val);
}

struct ubjson_bytes_encoder_reset_test_fixture
{
    std::vector<uint8_t> output1;
    std::vector<uint8_t> output2;
    ubjson::ubjson_bytes_encoder encoder;

    ubjson_bytes_encoder_reset_test_fixture() : encoder(output1) {}
    std::vector<uint8_t> bytes1() const {return output1;}
    std::vector<uint8_t> bytes2() const {return output2;}
};

struct ubjson_stream_encoder_reset_test_fixture
{
    std::ostringstream output1;
    std::ostringstream output2;
    ubjson::ubjson_stream_encoder encoder;

    ubjson_stream_encoder_reset_test_fixture() : encoder(output1) {}
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

TEMPLATE_TEST_CASE("test_ubjson_encoder_reset", "",
                   ubjson_bytes_encoder_reset_test_fixture,
                   ubjson_stream_encoder_reset_test_fixture)
{
    using fixture_type = TestType;
    fixture_type f;

    std::vector<uint8_t> expected_partial =
        {
            '[', '#', 'U', 2, // begin array, 2 elements
                'S', 'U', 3, 'f', 'o', 'o' // string(3) "foo"
                // second element missing
        };

    std::vector<uint8_t> expected_full =
        {
            '[', '#', 'U', 2, // begin array, 2 elements
                'S', 'U', 3, 'f', 'o', 'o', // string(3) "foo"
                'U', 42 // int8(42)
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
