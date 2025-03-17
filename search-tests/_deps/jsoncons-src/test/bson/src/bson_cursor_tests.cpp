// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/bson/bson_cursor.hpp>
#include <jsoncons_ext/bson/bson.hpp>
#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>

#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("bson_cursor reputon test")
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
    bson::encode_bson(j, data);

    SECTION("test 1")
    {
        bson::bson_bytes_cursor cursor(data);

        CHECK(cursor.current().event_type() == staj_event_type::begin_object);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
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

TEST_CASE("bson_parser reset")
{
    std::vector<uint8_t> input1 = {
        0x0C, 0x00, 0x00, 0x00, // Document: 12 bytes
            0x10, // int32 field type
                0x61, 0x00, // "a" field name
                0x01, 0x00, 0x00, 0x00, // int32(1) field value
        0x00, // end of object
        0x0C, 0x00, 0x00, 0x00, // Document: 12 bytes
            0x10, // int32 field type
                0x62, 0x00, // "b" field name
                0x02, 0x00, 0x00, 0x00, // int32(2) field value
        0x00 // end of object
    };


    std::vector<uint8_t> input2 = {
        0x0C, 0x00, 0x00, 0x00, // Document: 12 bytes
            0x10, // int32 field type
                0x63, 0x00, // "c" field name
                0x03, 0x00, 0x00, 0x00, // int32(3) field value
        0x00 // end of object
    };

    json expected1 = json::parse(R"({"a":1})");
    json expected2 = json::parse(R"({"b":2})");
    json expected3 = json::parse(R"({"c":3})");

    json_decoder<json> destination;
    bson::basic_bson_parser<bytes_source> parser{ input1 };
    std::error_code ec;

    SECTION("keeping same source")
    {
        parser.parse(destination, ec);
        REQUIRE_FALSE(ec);
        CHECK(destination.get_result() == expected1);

        destination.reset();
        parser.reset();
        parser.parse(destination, ec);
        CHECK_FALSE(ec);
        CHECK(parser.stopped());
        // TODO: This fails: CHECK(parser.done());
        CHECK(destination.get_result() == expected2);
    }

    SECTION("with different source")
    {
        parser.parse(destination, ec);
        REQUIRE_FALSE(ec);
        CHECK(destination.get_result() == expected1);

        destination.reset();
        parser.reset(input2);
        parser.parse(destination, ec);
        CHECK_FALSE(ec);
        CHECK(parser.stopped());
        // TODO: This fails: CHECK(parser.done());
        CHECK(destination.get_result() == expected3);
    }
}

struct bson_bytes_cursor_reset_test_traits
{
    using cursor_type = bson::bson_bytes_cursor;
    using input_type = std::vector<uint8_t>;

    static void set_input(input_type& input, input_type bytes) {input = bytes;}
};

struct bson_stream_cursor_reset_test_traits
{
    using cursor_type = bson::bson_stream_cursor;

    // binary_stream_source::char_type is actually char, not uint8_t
    using input_type = std::istringstream;

    static void set_input(input_type& input, std::vector<uint8_t> bytes)
    {
        auto data = reinterpret_cast<const char*>(bytes.data());
        std::string s(data, bytes.size());
        input.str(s);
    }
};

template <typename CursorType>
void check_bson_cursor_document(std::string info, CursorType& cursor,
                                std::string expectedKey, int expectedValue)
{
    INFO(info);

    REQUIRE_FALSE(cursor.done());
    CHECK(cursor.current().event_type() == staj_event_type::begin_object);
    CHECK(cursor.current().tag() == semantic_tag::none);

    REQUIRE_FALSE(cursor.done());
    cursor.next();
    CHECK(cursor.current().event_type() == staj_event_type::key);
    CHECK(cursor.current().tag() == semantic_tag::none);
    CHECK(cursor.current().template get<std::string>() == expectedKey);
    CHECK(cursor.current().template get<jsoncons::string_view>() == expectedKey);

    REQUIRE_FALSE(cursor.done());
    cursor.next();
    CHECK(cursor.current().event_type() == staj_event_type::int64_value);
    CHECK(cursor.current().tag() == semantic_tag::none);
    CHECK(cursor.current().template get<int>() == expectedValue);

    REQUIRE_FALSE(cursor.done());
    cursor.next();
    CHECK(cursor.current().event_type() == staj_event_type::end_object);
    CHECK(cursor.current().tag() == semantic_tag::none);

    // Extra next() required to pop out of document state
    CHECK_FALSE(cursor.done());
    cursor.next();
    CHECK(cursor.done());
}

TEMPLATE_TEST_CASE("bson_cursor reset test", "",
                   bson_bytes_cursor_reset_test_traits,
                   bson_stream_cursor_reset_test_traits)
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
            0x0C, 0x00, 0x00, 0x00, // Document: 12 bytes
                0x10, // int32 field type
                    0x61, 0x00, // "a" field name
                    0x01, 0x00, 0x00, 0x00, // int32(1) field value
            0x00, // end of object
            0x0C, 0x00, 0x00, 0x00, // Document: 12 bytes
                0x10, // int32 field type
                    0x62, 0x00, // "b" field name
                    0x02, 0x00, 0x00, 0x00, // int32(2) field value
            0x00, // end of object
            0x0C, 0x00, 0x00, 0x00, // Document: 12 bytes
                0x10, // int32 field type
                    0x63, 0x00, // "c" field name
                    0x03, 0x00, 0x00, 0x00, // int32(3) field value
            0x00 // end of object
        });
        source_type source(input);
        cursor_type cursor(std::move(source));
        check_bson_cursor_document("first document", cursor, "a", 1);
        cursor.reset();
        check_bson_cursor_document("second document", cursor, "b", 2);
        cursor.reset(ec);
        check_bson_cursor_document("third document", cursor, "c", 3);
    }

    SECTION("with another source")
    {
        std::error_code ec;
        input_type input0;
        input_type input1;
        input_type input2;
        input_type input3;
        traits::set_input(input0, {});
        traits::set_input(input1, {
            0x0C, 0x00, 0x00, 0x00, // Document: 12 bytes
                0x10, // int32 field type
                    0x61, 0x00, // "a" field name
                    0x01, 0x00, 0x00, 0x00, // int32(1) field value
            0x00, // end of object
        });
        traits::set_input(input2, {
            0x09, 0x00, 0x00, 0x00, // Document: 9 bytes
                0x20, // invalid field type
                    0x62, 0x00, // "b" field name
                    0x00, // bogus field value
            0x00, // end of object
        });
        traits::set_input(input3, {
            0x0C, 0x00, 0x00, 0x00, // Document: 12 bytes
                0x10, // int32 field type
                    0x63, 0x00, // "c" field name
                    0x03, 0x00, 0x00, 0x00, // int32(3) field value
            0x00, // end of object
        });

        // Constructing cursor with blank input results in unexpected_eof
        // error because it eagerly parses the next event upon construction.
        cursor_type cursor(input0, ec);
        CHECK(ec == bson::bson_errc::unexpected_eof);
        CHECK_FALSE(cursor.done());

        // Reset to valid input1
        cursor.reset(input1);
        check_bson_cursor_document("first document", cursor, "a", 1);

        // Reset to invalid input2
        ec = bson::bson_errc::success;
        cursor.reset(input2, ec);
        REQUIRE_FALSE(ec);
        REQUIRE_FALSE(cursor.done());
        CHECK(cursor.current().event_type() == staj_event_type::begin_object);
        CHECK(cursor.current().tag() == semantic_tag::none);
        REQUIRE_FALSE(cursor.done());
        cursor.next(ec);
        REQUIRE_FALSE(ec);
        REQUIRE_FALSE(cursor.done());
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().tag() == semantic_tag::none);
        CHECK(cursor.current().template get<std::string>() == std::string("b"));
        CHECK(cursor.current().template get<jsoncons::string_view>() ==
              jsoncons::string_view("b"));
        cursor.next(ec);
        CHECK(ec == bson::bson_errc::unknown_type);
        CHECK_FALSE(cursor.done());

        // Reset to valid input3
        ec = bson::bson_errc::success;
        cursor.reset(input3, ec);
        check_bson_cursor_document("third document", cursor, "c", 3);
    }
}
