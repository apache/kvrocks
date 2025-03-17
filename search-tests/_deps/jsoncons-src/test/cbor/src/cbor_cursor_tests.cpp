// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/cbor/cbor_cursor.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>

#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("cbor_cursor reputon test")
{
    ojson j = ojson::parse(R"(
    {
       "application": "hiking",
       "reputons": [
       {
           "rater": "HikingAsylum",
           "assertion": "advanced",
           "rated": "Marilyn C",
           "rating": 0.90
         }
       ]
    }
    )");

    std::vector<uint8_t> data;
    cbor::encode_cbor(j, data);

    SECTION("test 1")
    {
        cbor::cbor_bytes_cursor cursor(data);

        CHECK(cursor.current().event_type() == staj_event_type::begin_object);
        CHECK(cursor.current().size() == 2);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        CHECK(cursor.current().size() == 1);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_object);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::double_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_object);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_object);
        cursor.next();
        CHECK(cursor.done());
    }
}

TEST_CASE("cbor_cursor indefinite array of array test")
{
    std::vector<uint8_t> data = {0x82,0x83,0x63,0x66,0x6f,0x6f,0x44,0x50,0x75,0x73,0x73,0xc3,0x49,0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x83,0x63,0x62,0x61,0x72,0xd6,0x44,0x50,0x75,0x73,0x73,0xc4,0x82,0x21,0x19,0x6a,0xb3};

    SECTION("test 1")
    {
        cbor::cbor_bytes_cursor cursor(data);
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        CHECK(cursor.current().tag() == semantic_tag::none);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        CHECK(cursor.current().tag() == semantic_tag::none);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().tag() == semantic_tag::none);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::byte_string_value);
        CHECK(cursor.current().tag() == semantic_tag::none);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().tag() == semantic_tag::bigint);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        CHECK(cursor.current().tag() == semantic_tag::none);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        CHECK(cursor.current().tag() == semantic_tag::none);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().tag() == semantic_tag::none);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::byte_string_value);
        CHECK(cursor.current().tag() == semantic_tag::base64);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().tag() == semantic_tag::bigdec);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        CHECK(cursor.current().tag() == semantic_tag::none);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        CHECK(cursor.current().tag() == semantic_tag::none);
        cursor.next();
        CHECK(cursor.done());
    }
}

struct remove_mark_cbor_filter
{
    bool reject_next_ = false;

    bool operator()(const staj_event& event, const ser_context&) 
    {
        if (event.event_type()  == staj_event_type::key &&
            event.get<jsoncons::string_view>() == "mark")
        {
            reject_next_ = true;
            return false;
        }
        else if (reject_next_)
        {
            reject_next_ = false;
            return false;
        }
        else
        {
            return true;
        }
    }
};

TEST_CASE("cbor_cursor with filter tests")
{
    auto j = ojson::parse(R"(
    [
        {
            "enrollmentNo" : 100,
            "firstName" : "Tom",
            "lastName" : "Cochrane",
            "mark" : 55
        },
        {
            "enrollmentNo" : 101,
            "firstName" : "Catherine",
            "lastName" : "Smith",
            "mark" : 95
        },
        {
            "enrollmentNo" : 102,
            "firstName" : "William",
            "lastName" : "Skeleton",
            "mark" : 60
        }
    ]
    )");

    std::vector<uint8_t> data;
    cbor::encode_cbor(j, data);

    cbor::cbor_bytes_cursor cursor(data);

    auto filtered_c = cursor | remove_mark_cbor_filter();

    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::begin_array);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::begin_object);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::uint64_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::string_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::string_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::end_object);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::begin_object);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::uint64_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::string_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::string_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::end_object);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::begin_object);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::uint64_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::string_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::string_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::end_object);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::end_array);
    filtered_c.next();
    CHECK(filtered_c.done());
}

struct cbor_bytes_cursor_reset_test_traits
{
    using cursor_type = cbor::cbor_bytes_cursor;
    using input_type = std::vector<uint8_t>;

    static void set_input(input_type& input, input_type bytes) {input = bytes;}
};

struct cbor_stream_cursor_reset_test_traits
{
    using cursor_type = cbor::cbor_stream_cursor;

    // binary_stream_source::char_type is actually char, not uint8_t
    using input_type = std::istringstream;

    static void set_input(input_type& input, std::vector<uint8_t> bytes)
    {
        auto data = reinterpret_cast<const char*>(bytes.data());
        std::string s(data, bytes.size());
        input.str(s);
    }
};

TEMPLATE_TEST_CASE("cbor_cursor reset test", "",
                   cbor_bytes_cursor_reset_test_traits,
                   cbor_stream_cursor_reset_test_traits)
{
    using traits = TestType;
    using input_type = typename traits::input_type;
    using cursor_type = typename traits::cursor_type;
    using source_type = typename cursor_type::source_type;

    SECTION("keeping same source")
    {
        std::error_code ec;
        input_type input;
        traits::set_input(input, {
            0x63, 0x54, 0x6f, 0x6d, // text(3), "Tom"
            0x38, 0x63, // negative(99)
            0xf6 // null
        });
        source_type source(input);
        cursor_type cursor(std::move(source));

        REQUIRE_FALSE(cursor.done());
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().tag() == semantic_tag::none);
        CHECK(cursor.current().template get<std::string>() == std::string("Tom"));
        CHECK(cursor.current().template get<jsoncons::string_view>() ==
              jsoncons::string_view("Tom"));
        cursor.next();
        CHECK(cursor.done());

        cursor.reset();
        REQUIRE_FALSE(cursor.done());
        CHECK(cursor.current().event_type() == staj_event_type::int64_value);
        CHECK(cursor.current().tag() == semantic_tag::none);
        CHECK(cursor.current().template get<int>() == -100);
        cursor.next();
        CHECK(cursor.done());

        cursor.reset(ec);
        REQUIRE_FALSE(ec);
        REQUIRE_FALSE(cursor.done());
        CHECK(cursor.current().event_type() == staj_event_type::null_value);
        CHECK(cursor.current().tag() == semantic_tag::none);
        cursor.next(ec);
        REQUIRE_FALSE(ec);
        CHECK(cursor.done());
    }

    SECTION("with another source")
    {
        std::error_code ec;
        input_type input0;
        input_type input1;
        input_type input2;
        input_type input3;
        traits::set_input(input0, {});
        traits::set_input(input1, {0x63, 0x54, 0x6f, 0x6d}); // text(3), "Tom"
        traits::set_input(input2, {0xe0}); // invalid special
        traits::set_input(input3, {0x38, 0x63}); // negative(99)

        // Constructing cursor with blank input results in unexpected_eof
        // error because it eagerly parses the next event upon construction.
        cursor_type cursor(input0, ec);
        CHECK(ec == cbor::cbor_errc::unexpected_eof);
        CHECK_FALSE(cursor.done());

        // Reset to valid input1
        cursor.reset(input1);
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().tag() == semantic_tag::none);
        CHECK(cursor.current().template get<std::string>() == std::string("Tom"));
        CHECK(cursor.current().template get<jsoncons::string_view>() ==
              jsoncons::string_view("Tom"));
        ec = cbor::cbor_errc::success;
        REQUIRE_FALSE(cursor.done());
        cursor.next(ec);
        CHECK_FALSE(ec);
        CHECK(cursor.done());

        // Reset to invalid input2
        ec = cbor::cbor_errc::success;
        cursor.reset(input2, ec);
        CHECK(ec == cbor::cbor_errc::unknown_type);
        CHECK_FALSE(cursor.done());

        // Reset to valid input3
        ec = cbor::cbor_errc::success;
        cursor.reset(input3, ec);
        REQUIRE_FALSE(ec);
        CHECK(cursor.current().event_type() == staj_event_type::int64_value);
        CHECK(cursor.current().tag() == semantic_tag::none);
        CHECK(cursor.current().template get<int>() == -100);
        REQUIRE_FALSE(cursor.done());
        cursor.next(ec);
        CHECK_FALSE(ec);
        CHECK(cursor.done());
    }
}
