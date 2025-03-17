// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/detail/span.hpp>
#include <iostream>
#include <vector>
#include <catch/catch.hpp>

TEST_CASE("jsoncons::detail::span constructor tests")
{
    SECTION("jsoncons::detail::span()")
    {
        jsoncons::detail::span<const uint8_t> s;
        CHECK(s.empty());
    }
    SECTION("jsoncons::detail::span(pointer,size_type)")
    {
        std::vector<uint8_t> v = {1,2,3,4};
        jsoncons::detail::span<const uint8_t> s(v.data(), v.size());
        CHECK(s.size() == v.size());
        CHECK(s.data() == v.data());
    }
    SECTION("jsoncons::detail::span(C& c)")
    {
        using C = std::vector<uint8_t>;
        C c = {{1,2,3,4}};

        jsoncons::detail::span<const uint8_t> s(c);
        CHECK(s.size() == c.size());
        CHECK(s.data() == c.data());
    }
    SECTION("jsoncons::detail::span(C c[])")
    {
        double c[] = {1,2,3,4};

        jsoncons::detail::span<const double> s{ c };
        CHECK(s.size() == 4);
        CHECK(s.data() == c);
    }
    SECTION("jsoncons::detail::span(std::array)")
    {
        std::array<double,4> c = {{1,2,3,4}};

        jsoncons::detail::span<double> s(c);
        CHECK(s.size() == 4);
        CHECK(s.data() == c.data());
    }
}

