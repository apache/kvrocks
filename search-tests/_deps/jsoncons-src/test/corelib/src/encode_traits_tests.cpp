// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons/json.hpp>

#include <vector>
#include <catch/catch.hpp>

using jsoncons::json_type_traits;
using jsoncons::json;
using jsoncons::wjson;
using jsoncons::decode_json;
using jsoncons::encode_json;

namespace encode_traits_tests {

    struct book
    {
        std::string author;
        std::string title;
        double price;

        book() = default;
        book(const book&) = default;
        book(book&&) = default;

        book(const std::string& author, const std::string& title, double price)
            : author(author), title(title), price(price)
        {
        }
    };
} // namespace encode_traits_tests

namespace ns = encode_traits_tests;

JSONCONS_ALL_MEMBER_TRAITS(ns::book,author,title,price)

TEST_CASE("decode_traits string tests")
{
    SECTION("test 1")
    {
        std::string s = "foo";

        std::wstring buf;
        encode_json(s,buf);

        auto s2 = decode_json<std::string>(buf);

        CHECK(s2 == s);
    }
    SECTION("test 2")
    {
        std::wstring s = L"foo";

        std::string buf;
        encode_json(s,buf);

        auto s2 = decode_json<std::wstring>(buf);

        CHECK(s2 == s);
    }
}

TEST_CASE("decode_traits vector of string tests")
{
    SECTION("test 1")
    {
        std::vector<std::string> v = {"foo","bar","baz"};

        std::wstring buf;
        encode_json(v,buf);

        auto v2 = decode_json<std::vector<std::string>>(buf);

        CHECK(v2 == v);
    }
    SECTION("test 2")
    {
        std::vector<std::wstring> v = {L"foo",L"bar",L"baz"};

        std::string buf;
        encode_json(v,buf);

        auto v2 = decode_json<std::vector<std::wstring>>(buf);

        CHECK(v2 == v);
    }
}

TEST_CASE("decode_traits std::pair tests")
{
    SECTION("test 1")
    {
        auto p = std::make_pair<int,std::string>(1,"foo");

        std::wstring buf;
        encode_json(p,buf);

        auto p2 = decode_json<std::pair<int,std::string>>(buf);

        CHECK(p2 == p);
    }
    SECTION("test 2")
    {
        auto p = std::make_pair<int,std::wstring>(1,L"foo");

        std::wstring buf;
        encode_json(p,buf);

        auto p2 = decode_json<std::pair<int,std::wstring>>(buf);

        CHECK(p2 == p);
    }
    SECTION("test 3")
    {
        ns::book book{"Haruki Murakami","Kafka on the Shore",25.17};

        auto p = std::make_pair(1,book);

        std::wstring buf;
        encode_json(p,buf);

        auto p2 = decode_json<std::pair<int,ns::book>>(buf);

        CHECK(p2.second.author == book.author);
        CHECK(p2.second.title == book.title);
        CHECK(p2.second.price == book.price);
    }
}

