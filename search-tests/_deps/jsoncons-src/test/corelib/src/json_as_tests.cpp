// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <ctime>
#include <map>
#include <sstream>
#include <utility>
#include <vector>

#include <catch/catch.hpp>

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>

TEST_CASE("json integer as string")
{
    SECTION("0xabcdef")
    {
        jsoncons::json doc("0xabcdef");
        CHECK(doc.as<int32_t>() == 11259375);
    }
    SECTION("0x123456789")
    {
        jsoncons::json doc("0x123456789");
        CHECK(doc.as<int64_t>() == 4886718345);
    }
    SECTION("0XABCDEF")
    {
        jsoncons::json doc("0XABCDEF");
        CHECK(doc.as<uint32_t>() == 11259375u);
    }
    SECTION("0X123456789")
    {
        jsoncons::json doc("0X123456789");
        CHECK(doc.as<uint64_t>() == 4886718345);
    }
    SECTION("0x0")
    {
        jsoncons::json doc("0x0");
        CHECK(doc.as<int>() == 0);
    }
    SECTION("0777")
    {
        jsoncons::json doc("0777");
        CHECK(doc.as<int>() == 511);
    }
    SECTION("0b1001")
    {
        jsoncons::json doc("0b1001");
        CHECK(doc.as<int>() == 9);
    }
    SECTION("0B1001")
    {
        jsoncons::json doc("0B1001");
        CHECK(doc.as<int>() == 9);
    }
}

TEST_CASE("json::is_object on proxy")
{
    jsoncons::json root = jsoncons::json::parse(R"({"key":"value"})");

    CHECK_FALSE(root.contains("key1"));
    CHECK(root["key1"].is_object());
    CHECK(root.contains("key1"));
}

TEST_CASE("json::as<jsoncons::string_view>()")
{
    std::string s1("Short");
    jsoncons::json j1(s1);
    CHECK(j1.as<jsoncons::string_view>() == jsoncons::string_view(s1));

    std::string s2("String to long for short string");
    jsoncons::json j2(s2);
    CHECK(j2.as<jsoncons::string_view>() == jsoncons::string_view(s2));
}

TEST_CASE("json::as<jsoncons::bigint>()")
{
    SECTION("from integer")
    {
        jsoncons::json doc(-1000);
        CHECK(bool(doc.as<jsoncons::bigint>() == jsoncons::bigint(-1000)));
    }
    SECTION("from unsigned integer")
    {
        jsoncons::json doc(1000u);
        CHECK(bool(doc.as<jsoncons::bigint>() == jsoncons::bigint(1000u)));
    }
    SECTION("from double")
    {
        jsoncons::json doc(1000.0);
        CHECK(bool(doc.as<jsoncons::bigint>() == jsoncons::bigint(1000)));
    }
    SECTION("from bigint")
    {
        std::string s = "-18446744073709551617";
        jsoncons::json doc(s,  jsoncons::semantic_tag::bigint);
        CHECK(bool(doc.as<jsoncons::bigint>() == jsoncons::bigint::from_string(s)));
    }
}

#if (defined(__GNUC__) || defined(__clang__)) && defined(JSONCONS_HAS_INT128) 
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
TEST_CASE("json::as<__int128>()")
{
    std::string s1 = "-18446744073709551617";

    __int128 n;
    auto result = jsoncons::detail::to_integer_unchecked(s1.data(),s1.size(), n);
    REQUIRE(result.ec == jsoncons::detail::to_integer_errc());

    jsoncons::json doc(s1);

    __int128 val = doc.as<__int128>();

    std::string s2;
    jsoncons::detail::from_integer(val, s2);

    std::string s3;
    jsoncons::detail::from_integer(n, s3);

    CHECK((n == val));
}

TEST_CASE("json::as<unsigned __int128>()")
{
    std::string s1 = "18446744073709551616";

    unsigned __int128 n;

    auto result = jsoncons::detail::to_integer_unchecked(s1.data(),s1.size(), n);
    REQUIRE(result.ec == jsoncons::detail::to_integer_errc());

    jsoncons::json doc(s1);

    unsigned __int128 val = doc.as<unsigned __int128>();

    std::string s2;
    jsoncons::detail::from_integer(val, s2);
    std::string s3;
    jsoncons::detail::from_integer(n, s3);

    CHECK((n == val));
}
#pragma GCC diagnostic pop
#endif

TEST_CASE("as byte string tests")
{
    SECTION("byte_string_arg hint")
    {
        std::vector<uint8_t> v = {'H','e','l','l','o'};
        jsoncons::json doc(jsoncons::byte_string_arg, v, jsoncons::semantic_tag::base64);
        jsoncons::json sj(doc.as<std::string>());

        auto u = sj.as<std::vector<uint8_t>>(jsoncons::byte_string_arg,
                                             jsoncons::semantic_tag::base64);

        CHECK(u == v);
    }
    SECTION("as std::vector<char>")
    {
        std::vector<char> v = {'H','e','l','l','o'};
        jsoncons::json doc(jsoncons::byte_string_arg, v, jsoncons::semantic_tag::base64);

        auto u = doc.as<std::vector<char>>();

        CHECK(u == v);
    }
    SECTION("as<std::vector<char>>(jsoncons::byte_string_arg, ...")
    {
        std::vector<char> v = {'H','e','l','l','o'};
        jsoncons::json doc(jsoncons::byte_string_arg, v, jsoncons::semantic_tag::base64);
        jsoncons::json sj(doc.as<std::string>());

        auto u = sj.as<std::vector<char>>(jsoncons::byte_string_arg,
                                          jsoncons::semantic_tag::base64);

        CHECK(u == v);
    }
}

