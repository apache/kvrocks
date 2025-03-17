// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/msgpack/msgpack_cursor.hpp>
#include <jsoncons_ext/msgpack/msgpack.hpp>

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>

#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("msgpack_cursor reputon test")
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
    msgpack::encode_msgpack(j, data);

    SECTION("test 1")
    {
        msgpack::msgpack_bytes_cursor cursor(data);

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
struct remove_mark_msgpack_filter
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


TEST_CASE("msgpack_cursor with filter tests")
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
    msgpack::encode_msgpack(j, data);

    msgpack::msgpack_bytes_cursor cursor(data);

    auto filtered_c = cursor | remove_mark_msgpack_filter();

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

TEST_CASE("msgpack_parser reset", "")
{
    std::vector<uint8_t> input1 = {
        0x92,0x01,0x02, // array(2), positive fixint(1), positive fixint(2)
        0x81,0xa1,0x63,0x04, // map(1), text(1), "c", positive fixint(4)
    };

    std::vector<uint8_t> input2 = {
        0x81,0xa1,0x65,0x06, // map(1), text(1), "e", positive fixint(6)
    };

    json expected1 = json::parse(R"([1,2])");
    json expected2 = json::parse(R"({"c":4})");
    json expected3 = json::parse(R"({"e":6})");

    json_decoder<json> destination;
    item_event_visitor_to_visitor_adaptor visitor{destination};
    msgpack::basic_msgpack_parser<bytes_source> parser{ input1 };
    std::error_code ec;

    SECTION("keeping same source")
    {
        parser.parse(visitor, ec);
        REQUIRE_FALSE(ec);
        CHECK(destination.get_result() == expected1);

        destination.reset();
        parser.reset();
        parser.parse(visitor, ec);
        CHECK_FALSE(ec);
        CHECK(parser.stopped());
        // TODO: This fails: CHECK(parser.done());
        CHECK(destination.get_result() == expected2);
    }

    SECTION("with different source")
    {
        parser.parse(visitor, ec);
        REQUIRE_FALSE(ec);
        CHECK(destination.get_result() == expected1);

        destination.reset();
        parser.reset(input2);
        parser.parse(visitor, ec);
        CHECK_FALSE(ec);
        CHECK(parser.stopped());
        // TODO: This fails: CHECK(parser.done());
        CHECK(destination.get_result() == expected3);
    }
}

struct msgpack_bytes_cursor_reset_test_traits
{
    using cursor_type = msgpack::msgpack_bytes_cursor;
    using input_type = std::vector<uint8_t>;

    static void set_input(input_type& input, input_type bytes) {input = bytes;}
};

struct msgpack_stream_cursor_reset_test_traits
{
    using cursor_type = msgpack::msgpack_stream_cursor;

    // binary_stream_source::char_type is actually char, not uint8_t
    using input_type = std::istringstream;

    static void set_input(input_type& input, std::vector<uint8_t> bytes)
    {
        auto data = reinterpret_cast<const char*>(bytes.data());
        std::string s(data, bytes.size());
        input.str(s);
    }
};

TEMPLATE_TEST_CASE("msgpack_cursor reset test", "",
                   msgpack_bytes_cursor_reset_test_traits,
                   msgpack_stream_cursor_reset_test_traits)
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
            0xa3, 0x54, 0x6f, 0x6d, // str(3), "Tom"
            0xd0, 0x9c, // int8(-100)
            0xc0 // nil
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
        traits::set_input(input1, {0xa3, 0x54, 0x6f, 0x6d}); // str(3), "Tom"
        traits::set_input(input2, {0xc1}); // never used
        traits::set_input(input3, {0xd0, 0x9c}); // int8(-100)

        // Constructing cursor with blank input results in unexpected_eof
        // error because it eagerly parses the next event upon construction.
        cursor_type cursor(input0, ec);
        CHECK(ec == msgpack::msgpack_errc::unexpected_eof);
        CHECK_FALSE(cursor.done());

        // Reset to valid input1
        cursor.reset(input1);
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().tag() == semantic_tag::none);
        CHECK(cursor.current().template get<std::string>() == std::string("Tom"));
        CHECK(cursor.current().template get<jsoncons::string_view>() ==
              jsoncons::string_view("Tom"));
        ec = msgpack::msgpack_errc::success;
        REQUIRE_FALSE(cursor.done());
        cursor.next(ec);
        CHECK_FALSE(ec);
        CHECK(cursor.done());

        // Reset to invalid input2
        ec = msgpack::msgpack_errc::success;
        cursor.reset(input2, ec);
        CHECK(ec == msgpack::msgpack_errc::unknown_type);
        CHECK_FALSE(cursor.done());

        // Reset to valid input3
        ec = msgpack::msgpack_errc::success;
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
