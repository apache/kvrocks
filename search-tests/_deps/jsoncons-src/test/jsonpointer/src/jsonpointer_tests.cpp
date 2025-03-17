// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons/json.hpp>

#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <new>
#include <catch/catch.hpp>

using namespace jsoncons;
using namespace jsoncons::literals;

template <typename CharT,typename Policy,typename Allocator>
void check_contains(const basic_json<CharT,Policy,Allocator>& example, const std::string& pointer, bool expected)
{
    bool result = jsonpointer::contains(example,pointer);
    if (result != expected)
    {
        std::cout << "contains failed\n";
        std::cout << "    given: " << example << "\n";
        std::cout << "    pointer: " << pointer << "\n";
    }
    CHECK(expected == result);
}

template <typename CharT,typename Policy,typename Allocator>
void check_get(const basic_json<CharT,Policy,Allocator>& example, const std::string& pointer, const basic_json<CharT,Policy,Allocator>& expected)
{
    std::error_code ec;
    const auto& result = jsonpointer::get(example,pointer,ec);
    if (result != expected)
    {
        std::cout << "    given: " << example << "\n";
        std::cout << "    expected: " << expected << "\n";
        std::cout << "    pointer: " << pointer << "\n";
    }
    CHECK_FALSE(ec);
    CHECK(expected == result);
}

void check_insert_or_assign(json& example, const std::string& path, const json& value, const json& expected)
{
    std::error_code ec;
    jsonpointer::add(example, path, value, ec);
    CHECK_FALSE(ec);
    CHECK(expected == example);
}

void check_replace(json& example, const std::string& path, const json& value, const json& expected)
{
    std::error_code ec;
    jsonpointer::replace(example, path, value, ec);
    CHECK_FALSE(ec);
    CHECK(expected == example);
}

void check_remove(json& example, const std::string& path, const json& expected)
{
    std::error_code ec;
    jsonpointer::remove(example, path, ec);
    CHECK_FALSE(ec);
    CHECK(expected == example);
}

TEST_CASE("get_with_const_ref_test")
{
// Example from RFC 6901
const json example = json::parse(R"(
   {
      "foo": ["bar", "baz"],
      "": 0,
      "a/b": 1,
      "c%d": 2,
      "e^f": 3,
      "g|h": 4,
      "i\\j": 5,
      "k\"l": 6,
      " ": 7,
      "m~n": 8
   }
)");

    check_contains(example,"",true);
    check_contains(example,"/foo",true);
    check_contains(example,"/foo/0",true);
    check_contains(example,"/",true);
    check_contains(example,"/a~1b",true);
    check_contains(example,"/c%d",true);
    check_contains(example,"/e^f",true);
    check_contains(example,"/g|h",true);
    check_contains(example,"/i\\j",true);
    check_contains(example,"/k\"l",true);
    check_contains(example,"/ ",true);
    check_contains(example,"/m~0n",true);

    check_get(example,"",example);
    check_get(example,"/foo",json::parse("[\"bar\", \"baz\"]"));
    check_get(example,"/foo/0",json("bar"));
    check_get(example,"/",json(0));
    check_get(example,"/a~1b",json(1));
    check_get(example,"/c%d",json(2));
    check_get(example,"/e^f",json(3));
    check_get(example,"/g|h",json(4));
    check_get(example,"/i\\j",json(5));
    check_get(example,"/k\"l",json(6));
    check_get(example,"/ ",json(7));
    check_get(example,"/m~0n",json(8));
}

TEST_CASE("get_with_ref_test")
{
// Example from RFC 6901
json example = json::parse(R"(
   {
      "foo": ["bar", "baz"]
   }
)");

    std::error_code ec;
    json& result = jsonpointer::get(example,"/foo/0",ec);
    CHECK_FALSE(ec);

    result = "bat";

    //std::cout << example << '\n';
}

TEST_CASE("get_with_nonexistent_target")
{
    json example = R"(
        { "foo": "bar" }
    )"_json;

    check_contains(example,"/baz",false);
}

// insert_or_assign

TEST_CASE("test_add_object_member")
{
    json example = json::parse(R"(
    { "foo": "bar"}
    )");

    const json expected = json::parse(R"(
    { "foo": "bar", "baz" : "qux"}
    )");

    check_insert_or_assign(example,"/baz", json("qux"), expected);
}

TEST_CASE("test_add_array_element")
{
    json example = json::parse(R"(
    { "foo": [ "bar", "baz" ] }
    )");

    const json expected = json::parse(R"(
    { "foo": [ "bar", "qux", "baz" ] }
    )");

    check_insert_or_assign(example,"/foo/1", json("qux"), expected);
}

TEST_CASE("test_add_array_value")
{
    json example = json::parse(R"(
     { "foo": ["bar"] }
    )");

    const json expected = json::parse(R"(
    { "foo": ["bar", ["abc", "def"]] }
    )");

    check_insert_or_assign(example,"/foo/-", json(json_array_arg, {"abc", "def"}), expected);
}

// remove

TEST_CASE("test_remove_object_member")
{
    json example = json::parse(R"(
    { "foo": "bar", "baz" : "qux"}
    )");

    const json expected = json::parse(R"(
        { "foo": "bar"}
    )");

    check_remove(example,"/baz", expected);
}

TEST_CASE("test_remove_array_element")
{
    json example = json::parse(R"(
        { "foo": [ "bar", "qux", "baz" ] }
    )");

    const json expected = json::parse(R"(
        { "foo": [ "bar", "baz" ] }
    )");

    check_remove(example,"/foo/1", expected);
}

// replace

TEST_CASE("test_replace_object_value")
{
    json example = json::parse(R"(
        {
          "baz": "qux",
          "foo": "bar"
        }
    )");

    const json expected = json::parse(R"(
        {
          "baz": "boo",
          "foo": "bar"
        }
    )");

    check_replace(example,"/baz", json("boo"), expected);
}
TEST_CASE("test_replace_array_value")
{
    json example = json::parse(R"(
        { "foo": [ "bar", "baz" ] }
    )");

    const json expected = json::parse(R"(
        { "foo": [ "bar", "qux" ] }
    )");

    check_replace(example,"/foo/1", json("qux"), expected);
}

TEST_CASE("jsonpointer path tests")
{
    SECTION("/a~1b")
    {
        jsonpointer::json_pointer ptr("/a~1b");

        auto it = ptr.begin();
        auto end = ptr.end();

        CHECK(it != end);
        CHECK((*it++ == "a/b"));
        CHECK(it == end);
    }
    SECTION("/a~1b")
    {
        jsonpointer::json_pointer ptr("/m~0n");

        auto it = ptr.begin();
        auto end = ptr.end();

        CHECK(it != end);
        CHECK((*it++ == "m~n"));
        CHECK(it == end);
    }
    SECTION("/0/1")
    {
        jsonpointer::json_pointer ptr("/0/1");

        auto it = ptr.begin();
        auto end = ptr.end();

        CHECK(it != end);
        CHECK((*it++ == "0"));
        CHECK(it != end);
        CHECK((*it++ == "1"));
        CHECK(it == end);
    }
}

TEST_CASE("wjsonpointer path tests")
{
    SECTION("/a~1b")
    {
        jsonpointer::wjson_pointer ptr(L"/a~1b");

        auto it = ptr.begin();
        auto end = ptr.end();

        CHECK(it != end);
        CHECK((*it++ == L"a/b"));
        CHECK(it == end);
    }
}

TEST_CASE("jsonpointer concatenation")
{
    // Example from RFC 6901
    json example = json::parse(R"(
       {
          "a/b": ["bar", "baz"],
          "m~n": ["foo", "qux"]
       }
    )");

    SECTION("path append a/b")
    {
        jsonpointer::json_pointer ptr;
        ptr /= "a/b";
        ptr /= "0";

        auto it = ptr.begin();
        auto end = ptr.end();

        CHECK((*it++ == "a/b"));
        CHECK((*it++ == "0"));
        CHECK(it == end);

        std::error_code ec;
        json j = jsonpointer::get(example, ptr, ec);
        //std::cout << j << "\n";
        CHECK(j == json("bar"));
    }

    SECTION("concatenate two paths")
    {
        jsonpointer::json_pointer ptr1;
        ptr1 /= "m~n";
        jsonpointer::json_pointer ptr2;
        ptr2 /= "1";
        jsonpointer::json_pointer ptr = ptr1 + ptr2;

        auto it = ptr.begin();
        auto end = ptr.end();

        CHECK((*it++ == "m~n"));
        CHECK((*it++ == "1"));
        CHECK(it == end);

        json j = jsonpointer::get(example, ptr);
        CHECK(j == json("qux"));
        //std::cout << j << "\n";
    }
}

TEST_CASE("[jsonpointer] Inserting object after deleting it")
{
    ojson oj;

    std::error_code ec;

    jsonpointer::add( oj, "/test", ojson(), ec );
    CHECK(oj.size() == 1);

    jsonpointer::remove( oj, "/test", ec );
    CHECK(oj.size() == 0);

    jsonpointer::add( oj, "/t", ojson(), ec );
    CHECK(oj.size() == 1);
}

TEST_CASE("[jsonpointer] create_if_missing")
{
    SECTION("get from empty")
    {
        std::vector<std::string> keys = {"foo","bar","baz"};

        jsonpointer::json_pointer ptr;
        for (const auto& key : keys)
        {
            ptr /= key;
        }

        json doc;
        json result = jsonpointer::get(doc, ptr, true);

        json expected = json::parse(R"({"foo":{"bar":{"baz":{}}}})");

        CHECK(expected == doc);
        CHECK(result == json());
    }
    SECTION("get from non-empty")
    {
        std::vector<std::string> keys = {"foo","bar","baz"};

        jsonpointer::json_pointer ptr;
        for (const auto& key : keys)
        {
            ptr /= key;
        }

        json doc = json::parse(R"({"foo":{}})");
        json result = jsonpointer::get(doc, ptr, true);

        json expected = json::parse(R"({"foo":{"bar":{"baz":{}}}})");

        CHECK(expected == doc);
        CHECK(result == json());
    }
    SECTION("add into empty")
    {
        std::vector<std::string> keys = {"foo","bar","baz"};

        jsonpointer::json_pointer ptr;
        for (const auto& key : keys)
        {
            ptr /= key;
        }

        json doc;
        jsonpointer::add(doc, ptr, "str", true);

        json expected = json::parse(R"({"foo":{"bar":{"baz":"str"}}})");

        CHECK(expected == doc);
    }
    SECTION("add into non-empty")
    {
        std::vector<std::string> keys = {"foo","bar","baz"};

        jsonpointer::json_pointer ptr;
        for (const auto& key : keys)
        {
            ptr /= key;
        }

        json doc = json::parse(R"({"foo":{}})");
        jsonpointer::add(doc, ptr, "str", true);

        json expected = json::parse(R"({"foo":{"bar":{"baz":"str"}}})");

        CHECK(expected == doc);
    }
    SECTION("add_if_absent into empty")
    {
        std::vector<std::string> keys = { "foo","bar","baz" };

        jsonpointer::json_pointer ptr;
        for (const auto& key : keys)
        {
            ptr /= key;
        }

        json doc;
        jsonpointer::add_if_absent(doc, ptr, "str", true);
        json expected = json::parse(R"({"foo":{"bar":{"baz":"str"}}})");
        CHECK(expected == doc);
    }
    SECTION("add_if_absent into non-empty")
    {
        std::vector<std::string> keys = { "foo","bar","baz" };

        jsonpointer::json_pointer ptr;
        for (const auto& key : keys)
        {
            ptr /= key;
        }

        json doc = json::parse(R"({"foo":{}})");
        jsonpointer::add_if_absent(doc, ptr, "str", true);
        json expected = json::parse(R"({"foo":{"bar":{"baz":"str"}}})");
        CHECK(expected == doc);
    }
    SECTION("replace into empty")
    {
        std::vector<std::string> keys = {"foo","bar","baz"};

        jsonpointer::json_pointer ptr;
        for (const auto& key : keys)
        {
            ptr /= key;
        }

        json doc;
        jsonpointer::replace(doc, ptr, "str", true);

        json expected = json::parse(R"({"foo":{"bar":{"baz":"str"}}})");

        CHECK(expected == doc);
    }
    SECTION("replace into non-empty")
    {
        std::vector<std::string> keys = {"foo","bar","baz"};

        jsonpointer::json_pointer ptr;
        for (const auto& key : keys)
        {
            ptr /= key;
        }

        json doc = json::parse(R"({"foo":{}})");
        jsonpointer::replace(doc, ptr, "str", true);

        json expected = json::parse(R"({"foo":{"bar":{"baz":"str"}}})");

        CHECK(expected == doc);
    }
}

#if defined(JSONCONS_HAS_STATEFUL_ALLOCATOR) && JSONCONS_HAS_STATEFUL_ALLOCATOR == 1

#include <scoped_allocator>
#include <common/free_list_allocator.hpp>

template <typename T>
using MyScopedAllocator = std::scoped_allocator_adaptor<free_list_allocator<T>>;

using cust_json = basic_json<char,sorted_policy,MyScopedAllocator<char>>;

TEST_CASE("jsonpointer get with stateful allocator")
{
    MyScopedAllocator<char> alloc(1);

    // Example from RFC 6901
    const cust_json example = cust_json::parse(combine_allocators(alloc), R"(
       {
          "foo": ["bar", "baz"],
          "": 0,
          "a/b": 1,
          "c%d": 2,
          "e^f": 3,
          "g|h": 4,
          "i\\j": 5,
          "k\"l": 6,
          " ": 7,
          "m~n": 8
       }
    )", json_options{});

    check_contains(example,"",true);
    check_contains(example,"/foo",true);
    check_contains(example,"/foo/0",true);
    check_contains(example,"/",true);
    check_contains(example,"/a~1b",true);
    check_contains(example,"/c%d",true);
    check_contains(example,"/e^f",true);
    check_contains(example,"/g|h",true);
    check_contains(example,"/i\\j",true);
    check_contains(example,"/k\"l",true);
    check_contains(example,"/ ",true);
    check_contains(example,"/m~0n",true);

    check_get(example,"",example);
    check_get(example,"/foo", cust_json::parse(combine_allocators(alloc),
        jsoncons::string_view("[\"bar\", \"baz\"]"), json_options()));
    check_get(example,"/foo/0", cust_json("bar", semantic_tag::none, alloc));
    check_get(example,"/", cust_json(0));
    check_get(example,"/a~1b", cust_json(1));
    check_get(example,"/c%d", cust_json(2));
    check_get(example,"/e^f", cust_json(3));
    check_get(example,"/g|h", cust_json(4));
    check_get(example,"/i\\j", cust_json(5));
    check_get(example,"/k\"l", cust_json(6));
    check_get(example,"/ ", cust_json(7));
    check_get(example,"/m~0n", cust_json(8));
}
#endif

TEST_CASE("jsonpointer JSON Schema tests")
{
    SECTION("not a valid JSON-pointer (~ not escaped)")
    {
        std::error_code ec;
        auto jsonp = jsonpointer::json_pointer::parse("/foo/bar~", ec);
        CHECK(ec);
    }
    SECTION("not a valid JSON-pointer (URI Fragment Identifier) #1")
    {
        std::error_code ec;
        auto jsonp = jsonpointer::json_pointer::parse("#", ec);
        std::cout << jsonp.string() << "\n";
        CHECK(ec);
    }
    SECTION("not a valid JSON-pointer (some escaped, but not all) #1")
    {
        std::error_code ec;
        auto jsonp = jsonpointer::json_pointer::parse("/~0~", ec);
        std::cout << jsonp.string() << "\n";
        CHECK(ec);
    }
}


