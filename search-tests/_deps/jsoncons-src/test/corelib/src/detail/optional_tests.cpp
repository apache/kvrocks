// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/detail/optional.hpp>
#include <jsoncons/json.hpp>
#include <catch/catch.hpp>

using jsoncons::detail::optional;
using jsoncons::json;

TEST_CASE("optional constructor tests")
{
    std::string input = R"(
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

    SECTION("optional<T>()")
    {
        optional<int> x;
        CHECK_FALSE(x.has_value());
        CHECK_FALSE(x);
    }
    SECTION("optional<T>(json)")
    {
        json j = json::parse(input);
        optional<json> x{j};
        CHECK(x.has_value());
        bool b(x);
        CHECK(b);

        json* p = x.operator->();
        REQUIRE(p);
        REQUIRE(p->size() == 3);
        json& ref = x.value();
        REQUIRE(ref.size() == 3);

        const auto& cref = *x;
        REQUIRE(cref.size() == 3);

        x = j[1];
        REQUIRE(x.has_value());
        const auto& cref2 = *x;
        REQUIRE(cref2.is_object());
        REQUIRE(cref2.size() == 4);
        CHECK(cref2["firstName"].as<std::string>() == std::string("Catherine"));
    }
    SECTION("optional<T>(int64_t) from const")
    {
        const int64_t val = 10;
        optional<int64_t> x{val};
        CHECK(x.has_value());
        bool b(x);
        CHECK(b);
        optional<int64_t> y;
        y = val;
    }
    SECTION("optional<T>(const optional<T>&)")
    {
        optional<int64_t> x(10);
        optional<int64_t> y(x);
        CHECK(y.has_value());
        bool b(y);
        CHECK(b);
    }

    SECTION("optional<T>(const optional<T>&) from const")
    {
        const optional<int64_t> x(10);
        optional<int64_t> y(x);
        CHECK(y.has_value());
        bool b(y);
        CHECK(b);
    }
}

TEST_CASE("optional swap tests")
{
    SECTION("with value and with value")
    {
        optional<std::vector<double>> a(std::vector<double>{1.0,2.0,3.0,4.0});
        optional<std::vector<double>> b(std::vector<double>{5.0,6.0,7.0,8.0});

        swap(a,b);
        CHECK(a.has_value());
        CHECK(b.has_value());
        CHECK(a.value()[0] == 5.0);
        CHECK(a.value()[1] == 6.0);
        CHECK(a.value()[2] == 7.0);
        CHECK(a.value()[3] == 8.0);
        CHECK(b.value()[0] == 1.0);
        CHECK(b.value()[1] == 2.0);
        CHECK(b.value()[2] == 3.0);
        CHECK(b.value()[3] == 4.0);
    }
    SECTION("with value and without value")
    {
        optional<std::vector<double>> a(std::vector<double>{1.0, 2.0, 3.0, 4.0});
        optional<std::vector<double>> b;

        swap(a, b);
        CHECK_FALSE(a.has_value());
        CHECK(b.has_value());
        CHECK(b.value()[0] == 1.0);
        CHECK(b.value()[1] == 2.0);
        CHECK(b.value()[2] == 3.0);
        CHECK(b.value()[3] == 4.0);
    }
    SECTION("without value and without value")
    {
        optional<std::vector<double>> a;
        optional<std::vector<double>> b;

        swap(a, b);
        CHECK_FALSE(a.has_value());
        CHECK_FALSE(b.has_value());
    }
}

TEST_CASE("optional&& assignment tests")
{
    SECTION("with value from with value")
    {
        optional<std::vector<double>> a(std::vector<double>{1.0,2.0,3.0,4.0});
        optional<std::vector<double>> b(std::vector<double>{5.0,6.0,7.0,8.0});

        a = std::move(b);
        CHECK(a.has_value());
        CHECK(a.value()[0] == 5.0);
        CHECK(a.value()[1] == 6.0);
        CHECK(a.value()[2] == 7.0);
        CHECK(a.value()[3] == 8.0);
    }
    SECTION("with value from no value")
    {
        optional<std::vector<double>> a(std::vector<double>{1.0,2.0,3.0,4.0});
        optional<std::vector<double>> b;

        a = std::move(b);
        CHECK_FALSE(a.has_value());
    }
}

