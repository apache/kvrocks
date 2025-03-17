// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/cbor/cbor_event_reader.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>

#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("cbor_event_reader reputon test")
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
        cbor::cbor_event_reader<bytes_source> reader(data);

        CHECK(reader.current().event_type() == staj_event_type::begin_object);
        CHECK(reader.current().size() == 2);
        reader.next();
        CHECK(reader.current().event_type() == staj_event_type::string_value);  // key
        reader.next();
        CHECK(reader.current().event_type() == staj_event_type::string_value);
        reader.next();
        CHECK(reader.current().event_type() == staj_event_type::string_value);  // key
        reader.next();
        CHECK(reader.current().event_type() == staj_event_type::begin_array);
        CHECK(reader.current().size() == 1);
        reader.next();
        CHECK(reader.current().event_type() == staj_event_type::begin_object);
        reader.next();
        CHECK(reader.current().event_type() == staj_event_type::string_value);  // key
        reader.next();
        CHECK(reader.current().event_type() == staj_event_type::string_value);
        reader.next();
        CHECK(reader.current().event_type() == staj_event_type::string_value);  // key
        reader.next();
        CHECK(reader.current().event_type() == staj_event_type::string_value);
        reader.next();
        CHECK(reader.current().event_type() == staj_event_type::string_value);  // key
        reader.next();
        CHECK(reader.current().event_type() == staj_event_type::string_value);
        reader.next();
        CHECK(reader.current().event_type() == staj_event_type::string_value);  // key
        reader.next();
        CHECK(reader.current().event_type() == staj_event_type::double_value);
        reader.next();
        CHECK(reader.current().event_type() == staj_event_type::end_object);
        reader.next();
        CHECK(reader.current().event_type() == staj_event_type::end_array);
        reader.next();
        CHECK(reader.current().event_type() == staj_event_type::end_object);
        reader.next();
        CHECK(reader.done());
    }
}

struct cbor_bytes_cursor2_reset_test_traits
{
    using event_reader_type = cbor::cbor_event_reader<bytes_source>;
    using input_type = std::vector<uint8_t>;

    static void set_input(input_type& input, input_type bytes) {input = bytes;}
};

struct cbor_stream_cursor2_reset_test_traits
{
    using event_reader_type = cbor::cbor_event_reader<binary_stream_source>;

    // binary_stream_source::char_type is actually char, not uint8_t
    using input_type = std::istringstream;

    static void set_input(input_type& input, std::vector<uint8_t> bytes)
    {
        auto data = reinterpret_cast<const char*>(bytes.data());
        std::string s(data, bytes.size());
        input.str(s);
    }
};

TEMPLATE_TEST_CASE("cbor_event_reader reset test", "",
                   cbor_bytes_cursor2_reset_test_traits,
                   cbor_stream_cursor2_reset_test_traits)
{
    using traits = TestType;
    using input_type = typename traits::input_type;
    using event_reader_type = typename traits::event_reader_type;
    using source_type = typename event_reader_type::source_type;

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
        event_reader_type reader(std::move(source));

        REQUIRE_FALSE(reader.done());
        CHECK(reader.current().event_type() == staj_event_type::string_value);
        CHECK(reader.current().tag() == semantic_tag::none);
        CHECK(reader.current().template get<std::string>() == std::string("Tom"));
        CHECK(reader.current().template get<jsoncons::string_view>() ==
              jsoncons::string_view("Tom"));
        reader.next();
        CHECK(reader.done());

        reader.reset();
        REQUIRE_FALSE(reader.done());
        CHECK(reader.current().event_type() == staj_event_type::int64_value);
        CHECK(reader.current().tag() == semantic_tag::none);
        CHECK(reader.current().template get<int>() == -100);
        reader.next();
        CHECK(reader.done());

        reader.reset(ec);
        REQUIRE_FALSE(ec);
        REQUIRE_FALSE(reader.done());
        CHECK(reader.current().event_type() == staj_event_type::null_value);
        CHECK(reader.current().tag() == semantic_tag::none);
        reader.next(ec);
        REQUIRE_FALSE(ec);
        CHECK(reader.done());
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

        // Constructing reader with blank input results in unexpected_eof
        // error because it eagerly parses the next event upon construction.
        event_reader_type reader(input0, ec);
        CHECK(ec == cbor::cbor_errc::unexpected_eof);
        CHECK_FALSE(reader.done());

        // Reset to valid input1
        reader.reset(input1);
        CHECK(reader.current().event_type() == staj_event_type::string_value);
        CHECK(reader.current().tag() == semantic_tag::none);
        CHECK(reader.current().template get<std::string>() == std::string("Tom"));
        CHECK(reader.current().template get<jsoncons::string_view>() ==
              jsoncons::string_view("Tom"));
        ec = cbor::cbor_errc::success;
        REQUIRE_FALSE(reader.done());
        reader.next(ec);
        CHECK_FALSE(ec);
        CHECK(reader.done());

        // Reset to invalid input2
        reader.reset(input2, ec);
        CHECK(ec == cbor::cbor_errc::unknown_type);
        CHECK_FALSE(reader.done());

        // Reset to valid input3
        ec = cbor::cbor_errc::success;
        reader.reset(input3, ec);
        REQUIRE_FALSE(ec);
        CHECK(reader.current().event_type() == staj_event_type::int64_value);
        CHECK(reader.current().tag() == semantic_tag::none);
        CHECK(reader.current().template get<int>() == -100);
        REQUIRE_FALSE(reader.done());
        reader.next(ec);
        CHECK_FALSE(ec);
        CHECK(reader.done());
    }
}

