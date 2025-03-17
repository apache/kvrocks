// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <jsoncons/json_cursor.hpp>
#include <jsoncons/json_decoder.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("jtaj_array_view tests")
{
    std::string s = R"(
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
    )";

    SECTION("test 1")
    {
        json_string_cursor cursor(s);

        auto view = staj_array<json>(cursor);

        auto it = view.begin();
        auto end = view.end();

        const auto& j1 = *it;
        REQUIRE(j1.is_object());
        CHECK(j1["firstName"].as<std::string>() == std::string("Tom"));
        ++it;
        const auto& j2 = *it;
        CHECK(j2["firstName"].as<std::string>() == std::string("Catherine"));
        ++it;
        const auto& j3 = *it;
        CHECK(j3["firstName"].as<std::string>() == std::string("William"));
        ++it;
        CHECK((it == end));
    }

    SECTION("filter test")
    {
        json_string_cursor cursor(s);

        bool author_next = false;
        auto filtered_c = cursor |
            [&](const staj_event& event, const ser_context&) -> bool
        {
            if (event.event_type() == staj_event_type::key &&
                event.get<jsoncons::string_view>() == "firstName")
            {
                author_next = true;
                return false;
            }
            if (author_next)
            {
                author_next = false;
                return true;
            }
            return false;
        };

        REQUIRE(!filtered_c.done());
        CHECK(filtered_c.current().event_type() == staj_event_type::string_value);
        CHECK(filtered_c.current().get<std::string>() == std::string("Tom"));
        filtered_c.next();
        REQUIRE(!filtered_c.done());
        CHECK(filtered_c.current().event_type() == staj_event_type::string_value);
        CHECK(filtered_c.current().get<std::string>() == std::string("Catherine"));
        filtered_c.next();
        REQUIRE(!filtered_c.done());
        CHECK(filtered_c.current().event_type() == staj_event_type::string_value);
        CHECK(filtered_c.current().get<std::string>() == std::string("William"));
        filtered_c.next();
        REQUIRE(filtered_c.done());
    }
}

TEST_CASE("object_iterator test")
{
    std::string s = R"(
        {
            "enrollmentNo" : 100,
            "firstName" : "Tom",
            "lastName" : "Cochrane",
            "mark" : 55              
        }
    )";

    SECTION("test 1")
    {
        std::istringstream is(s);
        json_stream_cursor cursor(is);
        auto view = staj_object<std::string,json>(cursor);

        auto it = view.begin();
        auto end = view.end();

        const auto& p1 = *it;
        CHECK(p1.second.as<int>() == 100);
        ++it;
        const auto& p2 = *it;
        CHECK(p2.second.as<std::string>() == std::string("Tom"));
        ++it;
        const auto& p3 = *it;
        CHECK(p3.second.as<std::string>() == std::string("Cochrane"));
        ++it;
        const auto& p4 = *it;
        CHECK(p4.second.as<int>() == 55);
        ++it;
        CHECK((it == end));
    }
}


