// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/ubjson/ubjson_cursor.hpp>
#include <jsoncons_ext/ubjson/ubjson.hpp>

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>

#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>

using namespace jsoncons;

TEST_CASE("ubjson_cursor reputon test")
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
    ubjson::encode_ubjson(j, data);

    SECTION("test 1")
    {
        ubjson::ubjson_bytes_cursor cursor(data);

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

struct remove_mark_ubjson_filter
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

TEST_CASE("ubjson_cursor with filter tests")
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
    ubjson::encode_ubjson(j, data);

    ubjson::ubjson_bytes_cursor cursor(data);

    auto filtered_c = cursor | remove_mark_ubjson_filter();

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

TEST_CASE("ubjson_parser reset", "")
{
    std::vector<uint8_t> input1 = {
        '[','U',0x01,'U',0x02,']', // array, uint8(1), uint8(2), end array
        '{','U',0x01,'c','U',0x04,'}' // map, "c", uint(4), end map
    };

    std::vector<uint8_t> input2 = {
        '{','U',0x01,'e','U',0x06,'}' // map, "e", uint(6), end map
    };

    json expected1 = json::parse(R"([1,2])");
    json expected2 = json::parse(R"({"c":4})");
    json expected3 = json::parse(R"({"e":6})");

    json_decoder<json> destination;
    ubjson::basic_ubjson_parser<bytes_source> parser{ input1 };
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

TEST_CASE("ubjson_parser with json_diagnostics_visitor", "")
{
    std::ostringstream os;
    json_diagnostics_visitor visitor(os, "  ");
    std::vector<uint8_t> input{
        '{',
            'U',3,'f','o','o',
            '[',
                'U',42,
                'Z',
            ']',
        '}'
    };
    ubjson::basic_ubjson_parser<bytes_source> parser(input);
    std::error_code ec;
    parser.parse(visitor, ec);
    CHECK_FALSE(ec);
    std::ostringstream expected;
    expected << "visit_begin_object"  << '\n'
             << "  visit_key:foo"     << '\n'
             << "  visit_begin_array" << '\n'
             << "    visit_uint64:42" << '\n'
             << "    visit_null"      << '\n'
             << "  visit_end_array"   << '\n'
             << "visit_end_object"    << '\n';
    CHECK(os.str() == expected.str());
}

struct ubjson_bytes_cursor_reset_test_traits
{
    using cursor_type = ubjson::ubjson_bytes_cursor;
    using input_type = std::vector<uint8_t>;

    static void set_input(input_type& input, input_type bytes) {input = bytes;}
};

struct ubjson_stream_cursor_reset_test_traits
{
    using cursor_type = ubjson::ubjson_stream_cursor;

    // binary_stream_source::char_type is actually char, not uint8_t
    using input_type = std::istringstream;

    static void set_input(input_type& input, std::vector<uint8_t> bytes)
    {
        auto data = reinterpret_cast<const char*>(bytes.data());
        std::string s(data, bytes.size());
        input.str(s);
    }
};

TEMPLATE_TEST_CASE("ubjson_cursor reset test", "",
                   ubjson_bytes_cursor_reset_test_traits,
                   ubjson_stream_cursor_reset_test_traits)
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
            'S', 'U', 3, 'T', 'o', 'm', // string(3) "Tom"
            'i', 0x9c, // int8(-100)
            'Z' // null
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
        traits::set_input(input1, {'S', 'U', 3, 'T', 'o', 'm'}); // string(3) "Tom"
        traits::set_input(input2, {'A'}); // invalid type
        traits::set_input(input3, {'i', 0x9c}); // int8(-100)

        // Constructing cursor with blank input results in unexpected_eof
        // error because it eagerly parses the next event upon construction.
        cursor_type cursor(input0, ec);
        CHECK(ec == ubjson::ubjson_errc::unexpected_eof);
        CHECK_FALSE(cursor.done());

        // Reset to valid input1
        cursor.reset(input1);
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().tag() == semantic_tag::none);
        CHECK(cursor.current().template get<std::string>() == std::string("Tom"));
        CHECK(cursor.current().template get<jsoncons::string_view>() ==
              jsoncons::string_view("Tom"));
        ec = ubjson::ubjson_errc::success;
        REQUIRE_FALSE(cursor.done());
        cursor.next(ec);
        CHECK_FALSE(ec);
        CHECK(cursor.done());

        // Reset to invalid input2
        ec = ubjson::ubjson_errc::success;
        cursor.reset(input2, ec);
        CHECK(ec == ubjson::ubjson_errc::unknown_type);
        CHECK_FALSE(cursor.done());

        // Reset to valid input3
        ec = ubjson::ubjson_errc::success;
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
