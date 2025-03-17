// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/source_adaptor.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <new>
#include <fstream>

using namespace jsoncons;
 
TEST_CASE("buffer reader tests")
{
    SECTION("test 1")
    {
        json_source_adaptor<jsoncons::string_source<char>> reader{};
    }
}
 
TEST_CASE("json_source_adaptor constructor tests")
{
    SECTION("test 1")
    {
        json_source_adaptor<string_source<char>> source{string_source<char>()};
        CHECK(source.eof());
    }
}

