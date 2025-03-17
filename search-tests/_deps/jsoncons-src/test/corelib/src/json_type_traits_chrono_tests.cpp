// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("json_type_traits chron tests")
{
    SECTION("test 1")
    {
        uint64_t time = 1000;

        json j(time, semantic_tag::epoch_second);

        auto val = j.as<std::chrono::seconds>(); 

        CHECK(val.count() == std::chrono::seconds::rep(time));
    }
    SECTION("test 2")
    {
        double time = 1000.100;

        json j(time, semantic_tag::epoch_second);

        auto val = j.as<std::chrono::duration<double>>();

        CHECK(val.count() == time);
    }
}

