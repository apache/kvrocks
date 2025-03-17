// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <jsoncons_ext/jsonschema/common/format.hpp>

#include <jsoncons/json.hpp>
#include <jsoncons/utility/byte_string.hpp>

#include <fstream>
#include <iostream>
#include <regex>
#include <catch/catch.hpp>

namespace jsonschema = jsoncons::jsonschema;
 
TEST_CASE("IP4 format checker tests")
{
    SECTION("dotted quod tests")
    {
        CHECK(jsonschema::validate_ipv4_rfc2673(R"(208.116.0.0)"));
        CHECK(jsonschema::validate_ipv4_rfc2673(R"(208.116.0.0)"));
        CHECK_FALSE(jsonschema::validate_ipv4_rfc2673(R"(208.116.0)"));
    }
    SECTION("b tests")
    {
        CHECK(jsonschema::validate_ipv4_rfc2673(R"(b11010000011101)"));
    }
    SECTION("o tests")
    {
        CHECK(jsonschema::validate_ipv4_rfc2673(R"(064072)"));
    }
    SECTION("x tests")
    {
        CHECK(jsonschema::validate_ipv4_rfc2673(R"(xd074)"));
    }
}

TEST_CASE("IP6 format checker tests")
{
    SECTION("x:x:x:x:x:x:x:x")
    {
        CHECK(jsonschema::validate_ipv6_rfc2373(R"(FEDC:BA98:7654:3210:FEDC:BA98:7654:3210)"));
        CHECK(jsonschema::validate_ipv6_rfc2373(R"(1080:0:0:0:8:800:200C:417A)"));
        CHECK(jsonschema::validate_ipv6_rfc2373(R"(FF01:0:0:0:0:0:0:101)"));
        CHECK(jsonschema::validate_ipv6_rfc2373(R"(0:0:0:0:0:0:0:1)"));
        CHECK(jsonschema::validate_ipv6_rfc2373(R"(0:0:0:0:0:0:0:0)"));
    }
    SECTION("compressed")
    {
        CHECK(jsonschema::validate_ipv6_rfc2373(R"(1080::8:800:200C:417A)"));
        CHECK(jsonschema::validate_ipv6_rfc2373(R"(FF01::101)"));
        CHECK(jsonschema::validate_ipv6_rfc2373(R"(::1)"));
        CHECK(jsonschema::validate_ipv6_rfc2373(R"(::)"));
    }
    SECTION("x:x:x:x:x:x.d.d.d.d")
    {
        CHECK(jsonschema::validate_ipv6_rfc2373(R"(0:0:0:0:0:0:13.1.68.3)"));
        CHECK(jsonschema::validate_ipv6_rfc2373(R"(0:0:0:0:0:FFFF:129.144.52.38)"));
    }
    SECTION("compressed d.d.d.d")
    {
        CHECK(jsonschema::validate_ipv6_rfc2373(R"(::13.1.68.3)"));
        CHECK(jsonschema::validate_ipv6_rfc2373(R"(::FFFF:129.144.52.38)"));
    }
}

TEST_CASE("time tests")
{
    SECTION("full-time")
    {
        CHECK(jsonschema::validate_date_time_rfc3339("23:20:50.52Z", jsonschema::date_time_type::time));
        CHECK(jsonschema::validate_date_time_rfc3339("16:39:57-08:00", jsonschema::date_time_type::time));
        CHECK(jsonschema::validate_date_time_rfc3339("23:59:60Z", jsonschema::date_time_type::time));
        CHECK(jsonschema::validate_date_time_rfc3339("15:59:60-08:00", jsonschema::date_time_type::time));
        CHECK(jsonschema::validate_date_time_rfc3339("12:00:27.87+00:20", jsonschema::date_time_type::time));

        CHECK(jsonschema::validate_date_time_rfc3339("08:30:06.283185Z", jsonschema::date_time_type::time));
    }
}

TEST_CASE("date tests")
{
    SECTION("dates")
    {
        CHECK(jsonschema::validate_date_time_rfc3339("1985-04-12", jsonschema::date_time_type::date));
        CHECK(jsonschema::validate_date_time_rfc3339("1996-12-19", jsonschema::date_time_type::date));
        CHECK(jsonschema::validate_date_time_rfc3339("1990-12-31", jsonschema::date_time_type::date));
        CHECK(jsonschema::validate_date_time_rfc3339("2019-02-28", jsonschema::date_time_type::date));
        CHECK(jsonschema::validate_date_time_rfc3339("2020-02-28", jsonschema::date_time_type::date)); // 2020 not a leap year
        CHECK(jsonschema::is_leap_year(2024));
        CHECK(jsonschema::validate_date_time_rfc3339("2024-02-29", jsonschema::date_time_type::date));
        CHECK(jsonschema::validate_date_time_rfc3339("1937-01-01", jsonschema::date_time_type::date));
    }
}

TEST_CASE("date-time tests")
{
    SECTION("dates")
    {
        CHECK(jsonschema::validate_date_time_rfc3339("1985-04-12T23:20:50.52Z", jsonschema::date_time_type::date_time));
        CHECK(jsonschema::validate_date_time_rfc3339("1996-12-19T16:39:57-08:00", jsonschema::date_time_type::date_time));
        CHECK(jsonschema::validate_date_time_rfc3339("1990-12-31T23:59:60Z", jsonschema::date_time_type::date_time));
        CHECK(jsonschema::validate_date_time_rfc3339("1990-12-31T15:59:60-08:00", jsonschema::date_time_type::date_time));
        CHECK(jsonschema::validate_date_time_rfc3339("1937-01-01T12:00:27.87+00:20", jsonschema::date_time_type::date_time));
    }
}

TEST_CASE("email tests")
{
    SECTION("email")
    {
        //CHECK(jsonschema::validate_email_rfc5322("pete@silly.test"));
        CHECK(jsonschema::validate_email_rfc5322("joe.bloggs@example.com"));
        CHECK_FALSE(jsonschema::validate_email_rfc5322("te..st@example.com"));
    }
}
