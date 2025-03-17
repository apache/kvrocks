// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <jsoncons/json_filter.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <new>

using namespace jsoncons;

TEST_CASE("test_small_string")
{
    json s("ABCD");
    CHECK(s.storage_kind() == jsoncons::json_storage_kind::short_str);
    CHECK(s.as<std::string>() == std::string("ABCD"));

    json t(s);
    CHECK(t.storage_kind() == jsoncons::json_storage_kind::short_str);
    CHECK(t.as<std::string>() == std::string("ABCD"));

    json q;
    q = s;
    CHECK(q.storage_kind() == jsoncons::json_storage_kind::short_str);
    CHECK(q.as<std::string>() == std::string("ABCD"));
}


