// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <map>
#include <iterator>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("json(json_object_arg, first, last)")
{
    SECTION("copy map into json")
    {
        std::map<std::string,double> m = {{"c",1},{"b",2},{"a",3}};

        json j(json_object_arg, m.begin(),m.end());

        REQUIRE(j.size() == 3);
        auto it = j.object_range().begin();
        CHECK(it++->key() == "a");
        CHECK(it++->key() == "b");
        CHECK(it++->key() == "c");
    }
}

TEST_CASE("json insert(first,last) test")
{
    SECTION("copy map into json")
    {
        std::map<std::string,double> m1 = {{"f",4},{"e",5},{"d",6}};
        std::map<std::string,double> m2 = {{"c",1},{"b",2},{"a",3}};

        json j;
        j.insert(m1.begin(),m1.end());
        j.insert(m2.begin(),m2.end());

        //std::cout << j << "\n";

        REQUIRE(j.size() == 6);
        auto it = j.object_range().begin();
        CHECK(it++->key() == "a");
        CHECK(it++->key() == "b");
        CHECK(it++->key() == "c");
        CHECK(it++->key() == "d");
        CHECK(it++->key() == "e");
        CHECK(it++->key() == "f");
    }

    // Fails with xenial-armhf

    SECTION("move map into json")
    {
        std::map<std::string,double> m1 = {{"a",1},{"b",2},{"c",3}};
        std::map<std::string,double> m2 = {{"d",4},{"e",5},{"f",6}};

        json j;
        j.insert(std::make_move_iterator(m1.begin()),std::make_move_iterator(m1.end()));
        j.insert(std::make_move_iterator(m2.begin()),std::make_move_iterator(m2.end()));

        //std::cout << j << "\n";

        REQUIRE(j.size() == 6);
        auto it = j.object_range().begin();
        CHECK(it++->key() == "a");
        CHECK(it++->key() == "b");
        CHECK(it++->key() == "c");
        CHECK(it++->key() == "d");
        CHECK(it++->key() == "e");
        CHECK(it++->key() == "f");
    }
}

TEST_CASE("json as<T>")
{
    SECTION("empty object as string")
    {
        json j;
        std::string s = j.as<std::string>();
        CHECK("{}" == s);
    }

    SECTION("key not found")
    {
        JSONCONS_TRY
        {
            json j;
            std::string s = j["empty"].as<std::string>();
            CHECK(s == "{}");
        }
        JSONCONS_CATCH (const std::out_of_range& e)
        {
            CHECK(e.what() == std::string("Key not found: 'empty'"));
        }
    }
#ifdef __cpp_char8_t
    SECTION("Chinese characters as u8string")
    {
            jsoncons::json j;

            std::u8string s = u8"你好";
            j.try_emplace("hello", s);
            CHECK(j["hello"].as<std::u8string>() == s);
    }
#endif
}

TEST_CASE("parse_duplicate_names")
{
    json j1 = json::parse(R"({"first":1,"second":2,"third":3})");
    CHECK(3 == j1.size());
    CHECK(1 == j1["first"].as<int>());
    CHECK(2 == j1["second"].as<int>());
    CHECK(3 == j1["third"].as<int>());

    json j2 = json::parse(R"({"first":1,"second":2,"first":3})");
    CHECK(2 == j2.size());
    CHECK(1 == j2["first"].as<int>());
    CHECK(2 == j2["second"].as<int>());
}

TEST_CASE("test_erase_member")
{
    json o;
    o["key"] = "Hello";

    CHECK(o.size() == 1);
    o.erase("key");
    CHECK(o.size() == 0);

    json a;
    json b(json_object_arg);
    b["input-file"] = "config_file";
    json b_copy = b;

    a["b"] = std::move(b);

    CHECK(true == a["b"].is_object());
    CHECK(a["b"] == b_copy);
}

TEST_CASE("test_object_erase_range")
{
    json o;
    o["key1"] = "value1";
    o["key2"] = "value2";
    o["key3"] = "value3";
    o["key4"] = "value4";

    auto first = o.find("key2");
    auto last = o.find("key4");

    o.erase(first,last);
    
    CHECK(2 == o.size());
    CHECK(1 == o.count("key1"));
    CHECK(1 == o.count("key4"));
}

TEST_CASE("test_empty_object")
{
    json a;
    CHECK(a.size() == 0);
    CHECK(a.is_object());
    CHECK(a.is_object());

    json::object_iterator begin = a.object_range().begin();
    json::object_iterator end = a.object_range().end();

    for (json::object_iterator it = begin; it != end; ++it)
    {
        CHECK(false);
    }

    a["key"] = "Hello";
    CHECK(a.size() == 1);
    CHECK(a.is_object());
    CHECK(a.is_object());
}

TEST_CASE("test_const_empty_object")
{
    const json b;
    CHECK(b.size() == 0);
    CHECK(b.is_object());
    CHECK(b.is_object());

    json::const_object_iterator begin = b.object_range().begin();
    json::const_object_iterator end = b.object_range().end();

    for (json::const_object_iterator it = begin; it != end; ++it)
    {
        CHECK(false);
    }
}

TEST_CASE("test_empty_object_reserve")
{
    json c;
    CHECK(c.size() == 0);
    CHECK(c.is_object());
    CHECK(c.is_object());
    c.reserve(100);
    CHECK(c.capacity() == 100);
    c["key"] = "Hello";
    CHECK(c.size() == 1);
    CHECK(c.is_object());
    CHECK(c.is_object());
    CHECK(c.capacity() == 100);
}

TEST_CASE("test_empty_object_copy")
{
    json a;
    CHECK(a.size() == 0);
    CHECK(a.is_object());
    CHECK(a.is_object());

    json b = a;
    CHECK(b.size() == 0);
    CHECK(b.is_object());
    CHECK(b.is_object());
}

TEST_CASE("test_empty_object_move")
{
    json a;
    CHECK(a.size() == 0);
    CHECK(a.is_object());
    CHECK(a.is_object());

    json b = std::move(a);
    CHECK(b.size() == 0);
    CHECK(b.is_object());
    CHECK(b.is_object());
}

TEST_CASE("test_empty_object_copy_assignment")
{
    json a;
    CHECK(a.size() == 0);
    CHECK(a.is_object());
    CHECK(a.is_object());

    json b = json::make_array(10);
    CHECK(b.size() == 10);
    CHECK(b.is_array());
    CHECK(b.is_array());

    b = a;
    CHECK(b.size() == 0);
    CHECK(b.is_object());
    CHECK(b.is_object());

    json c;
    c["key"] = "value";
    CHECK(c.size() == 1);
    CHECK(c.is_object());
    CHECK(c.is_object());
    c = a;
    CHECK(c.size() == 0);
    CHECK(c.is_object());
    CHECK(c.is_object());
}

TEST_CASE("test_empty_object_move_assignment")
{
    json a;
    CHECK(a.size() == 0);
    CHECK(a.is_object());
    CHECK(a.is_object());

    json b = json::make_array(10);
    CHECK(b.size() == 10);
    CHECK(b.is_array());
    CHECK(b.is_array());

    b = std::move(a);
    CHECK(b.size() == 0);
    CHECK(b.is_object());
    CHECK(b.is_object());

    json c;
    c["key"] = "value";
    CHECK(c.size() == 1);
    CHECK(c.is_object());
    CHECK(c.is_object());

    c = std::move(b);
    CHECK(c.size() == 0);
    CHECK(c.is_object());
    CHECK(c.is_object());
}

TEST_CASE("at_or_null test")
{
    json a = json::parse(R"(
    {
        "key1" : "value1",
        "key2" : {"key3" : "value3"}
    }
    )");

    SECTION("1 arg")
    {
        const json& j = a.at_or_null("key1");
        CHECK(j.as<std::string>() == std::string("value1"));
    }

    SECTION("1 arg proxy")
    {
        const json& j = a["key2"].at_or_null("key3");
        CHECK(j.as<std::string>() == std::string("value3"));
    }

    SECTION("1 arg default")
    {
        const json& j = a.at_or_null("key4");
        CHECK(j.is_null());
    }

    SECTION("1 arg proxy default")
    {
        const json& j = a["key2"].at_or_null("key4");
        CHECK(j.is_null());
    }

    SECTION("1 arg null")
    {
        const json& j = json::null().at_or_null("key1");
        CHECK(j.is_null());
    }
}

TEST_CASE("get_value_or test")
{
    json a = json::parse(R"(
    {
        "key1" : "value1",
        "key2" : {"key3" : "value3"}
    }
    )");

    SECTION("2 arg")
    {
        std::string s1 = a.at("key1").as<std::string>();
        std::string s1a = a.at("key1").as<std::string>();
        std::string s2 = a.get_value_or<std::string>("key4","null");
        REQUIRE_THROWS_AS(a.at("key4"), std::out_of_range);

        CHECK(s1 == std::string("value1"));
        CHECK(s1a == std::string("value1"));

        CHECK(s2 == std::string("null"));
    }

    SECTION("2 arg null")
    {
        std::string s2 = json::null().get_value_or<std::string>("key4","null");
        CHECK(s2 == std::string("null"));
    }
}

TEST_CASE("test_proxy_get")
{
    json a;

    a["object1"] = json();
    a["object1"]["key1"] = "value1";

    std::string s1 = a["object1"].at("key1").as<std::string>();
    std::string s1a = a["object1"].at("key1").as<std::string>();
    std::string s2 = a["object1"].get_value_or<json>("key2",json::null()).as<std::string>();
    CHECK(a["object1"].get_value_or<json>("key2", json::null()).is_null());
    //std::cout << s2 << '\n';
    REQUIRE_THROWS_AS(a["object1"].at("key2").as<std::string>(), std::out_of_range);

    CHECK(std::string("value1") == s1);
    CHECK(std::string("value1") == s1a);
    CHECK(std::string("null") == s2);
}

TEST_CASE("test proxy get_value_or")
{
    json a;

    a["object1"] = json();
    a["object1"]["field1"] = "3.7";
    a["object1"]["field2"] = 1.5;

    std::string s1 = a["object1"].get_value_or<std::string>("field1","default");
    std::string s2 = a["object1"].get_value_or<std::string>("field2","1.0");
    std::string s3 = a["object1"].get_value_or<std::string>("field3","1.0");
    std::string s4 = a["object1"].get_value_or<std::string>("field2","1.0");
    std::string s5 = a["object1"].get_value_or<std::string>("field3","1.0");
    double d1 = a["object1"].get_value_or<double> ("field1",1.0);
    double d2 = a["object1"].get_value_or<double> ("field2",1.0);
    double d3 = a["object1"].get_value_or<double> ("field3",1.0);

    CHECK(std::string("3.7") == s1);
    CHECK(std::string("1.5") == s2);
    CHECK(std::string("1.0") == s3);
    CHECK(std::string("1.5") == s4);
    CHECK(std::string("1.0") == s5);
    CHECK(3.7 == d1);
    CHECK(1.5 == d2);
    CHECK(1 == d3);
}

TEST_CASE("test_set_and_proxy_set")
{
    json a;

    a.insert_or_assign("object1",json());
    a.insert_or_assign("field1","value1");
    a["object1"].insert_or_assign("field2","value2");

    CHECK(std::string("value1") == a["field1"].as<std::string>());
    CHECK(std::string("value2") == a["object1"]["field2"].as<std::string>());
}

TEST_CASE("test_emplace_and_proxy_set")
{
    json a;

    a.try_emplace("object1",json());
    a.try_emplace("field1","value1");
    a["object1"].try_emplace("field2","value2");

    CHECK(std::string("value1") == a["field1"].as<std::string>());
    CHECK(std::string("value2") == a["object1"]["field2"].as<std::string>());
}

TEST_CASE("test_const_member_read")
{
    json a;

    a["field1"] = 10;

    a["field2"];

    const json b(a);

    int val1 = b["field1"].as<int>();
    CHECK(val1 == 10);
    REQUIRE_NOTHROW(b["field2"]);
}

TEST_CASE("test_proxy_const_member_read")
{
    json a;

    a["object1"] = json();
    a["object1"]["field1"] = "value1";
    a["object1"]["field2"]; // Inserts empty object for "field2"

    const json b(a);

    std::string s1 = b["object1"]["field1"].as<std::string>();
    REQUIRE_NOTHROW(b["object1"]["field2"]);

    CHECK(s1 == std::string("value1"));
}

TEST_CASE("test_object_equals")
{
    json a;
    a["field1"] = "value1";

    json b;
    b["field1"] = "value1";

    CHECK(a == b);

    json c;
    c["field1"] = 10;

    CHECK_FALSE(a == c);
}

TEST_CASE("json_object_iterator test 1")
{
    SECTION("object_iterator")
    {
        json a = json::parse(R"({"name1" : "value1","name2" : "value2","name3" : "value3"})");

        json::object_iterator it = a.object_range().begin();
        CHECK((*it).key() == "name1");
        CHECK((*it).value() == json("value1"));
        ++it;
        CHECK((*it).key() == "name2");
        CHECK((*it).value() == json("value2"));

        CHECK((*(it++)).key() == "name2");
        CHECK((*it).key() == "name3");
        CHECK((*it).value() == json("value3"));

        CHECK((*(it--)).key() == "name3");
        CHECK((*it).value() == json("value2"));
        CHECK((*(--it)).value() == json("value1"));

        json::key_value_type member = *it;
        CHECK(member.key() == "name1");
        CHECK(member.value() == json("value1"));
    }
    SECTION("const_object_iterator 1")
    {
        json a = json::parse(R"({"name1" : "value1","name2" : "value2","name3" : "value3"})");

        json::const_object_iterator it = a.object_range().begin();
        CHECK((*it).key() == "name1");
        CHECK((*it).value() == json("value1"));
        ++it;
        CHECK((*it).key() == "name2");
        CHECK((*it).value() == json("value2"));

        CHECK((*(it++)).key() == "name2");
        CHECK((*it).key() == "name3");
        CHECK((*it).value() == json("value3"));

        CHECK((*(it--)).key() == "name3");
        CHECK((*it).value() == json("value2"));

        CHECK((*(--it)).value() == json("value1"));

        json::key_value_type member = *it;
        CHECK(member.key() == "name1");
        CHECK(member.value() == json("value1"));
    }
    SECTION("const_object_iterator 2")
    {
        const json a = json::parse(R"({"name1" : "value1","name2" : "value2","name3" : "value3"})");

        json::const_object_iterator it = static_cast<const json&>(a).object_range().begin();
        CHECK((it == a.object_range().begin()));
        CHECK_FALSE((it == a.object_range().end()));
        CHECK((*it).key() == "name1");
        CHECK((*it).value() == json("value1"));
        ++it;
        CHECK_FALSE((it == a.object_range().end()));
        CHECK((*it).key() == "name2");
        CHECK((*it).value() == json("value2"));

        CHECK((*(it++)).key() == "name2");
        CHECK_FALSE((it == a.object_range().end()));
        CHECK((*it).key() == "name3");
        CHECK((*it).value() == json("value3"));

        CHECK((*(it--)).key() == "name3");
        CHECK((*it).value() == json("value2"));

        CHECK((*(--it)).value() == json("value1"));
        CHECK((it == a.object_range().begin()));

        json::key_value_type member = *it;
        CHECK(member.key() == "name1");
        CHECK(member.value() == json("value1"));

        //*it = member; // Don't want this to compile
    }
    SECTION("json cbegin")
    {
        json a = json::parse(R"({"name1" : "value1","name2" : "value2","name3" : "value3"})");

        json::const_object_iterator it = a.object_range().cbegin();
        CHECK((*it).key() == "name1");
        CHECK((*it).value() == json("value1"));
        ++it;
        CHECK((*it).key() == "name2");
        CHECK((*it).value() == json("value2"));

        CHECK((*(it++)).key() == "name2");
        CHECK((*it).key() == "name3");
        CHECK((*it).value() == json("value3"));

        CHECK((*(it--)).key() == "name3");
        CHECK((*it).value() == json("value2"));
        CHECK((*(--it)).value() == json("value1"));

        json::key_value_type member = *it;
        CHECK(member.key() == "name1");
        CHECK(member.value() == json("value1"));
    }
    SECTION("const json cbegin")
    {
        const json a = json::parse(R"({"name1" : "value1","name2" : "value2","name3" : "value3"})");

        json::const_object_iterator it = a.object_range().cbegin();
        CHECK((*it).key() == "name1");
        CHECK((*it).value() == json("value1"));
        ++it;
        CHECK((*it).key() == "name2");
        CHECK((*it).value() == json("value2"));

        CHECK((*(it++)).key() == "name2");
        CHECK((*it).key() == "name3");
        CHECK((*it).value() == json("value3"));

        CHECK((*(it--)).key() == "name3");
        CHECK((*it).value() == json("value2"));
        CHECK((*(--it)).value() == json("value1"));

        json::key_value_type member = *it;
        CHECK(member.key() == "name1");
        CHECK(member.value() == json("value1"));
    }
}

// accessor tests


TEST_CASE("test_get_with_string_default")
{
    json example;

    std::string s("too long string for short string");
    std::string result = example.get_value_or<std::string>("test", s);
    CHECK(s == result);
}

TEST_CASE("test_compare_with_string")
{
    json a;
    a["key"] = "value";
    a["key1"] = "value1";
    a["key2"] = "value2";
    CHECK(a["key"] == a["key"]);
    CHECK_FALSE((a["key"] == a["key1"]));
    CHECK_FALSE((a["key"] == a["key2"]));
}

TEST_CASE("test_count")
{
    json a;
    a["key1"] = "value1";
    a["key2"] = "value2";

    CHECK(1 == a.count("key1"));
    CHECK(1 == a.count("key2"));
    CHECK(0 == a.count("key3"));

    json b = json::parse(
        "{\"key1\":\"a value\",\"key1\":\"another value\"}"
 );
    CHECK(1 == b.count("key1"));
}

TEST_CASE("test_find")
{
    json obj;

    json::object_iterator it = obj.find("key");
    CHECK((it == obj.object_range().end()));

    obj["key1"] = 10;
    obj["key2"] = true;
    obj["key3"] = 'c';
    obj["key4"] = "value4";

    json::object_iterator it2 =  obj.find("key");
    CHECK((it2 == obj.object_range().end()));

    json::object_iterator it3 =  obj.find("key4");
    CHECK_FALSE((it3 == obj.object_range().end()));
    CHECK(std::string("value4") ==it3->value().as<std::string>());
}

TEST_CASE("test_as")
{
    json obj;
    obj["field1"] = 10;
    obj["field2"] = true;
    obj["char_field"] = 'c';
    obj["string_field"] = "char";

    std::string s = obj["field1"].as<std::string>();
    CHECK(std::string("10") == s);
    int int_val = obj["field2"].as<int>();
    CHECK(1 == int_val);
    int short_val = obj["field2"].as<short>();
    CHECK(short_val == 1);
    int ushort_val = obj["field2"].as<unsigned short>();
    CHECK(ushort_val == static_cast<unsigned short>(1));
    char char_val = obj["field2"].as<char>();
    CHECK(int(char_val) == 1);

    CHECK(obj["char_field"].is<char>());
    CHECK_FALSE(obj["string_field"].is<char>());

    json parent;
    parent["child"] = obj;
    s = parent["child"]["field1"].as<std::string>();
    CHECK(s == std::string("10"));
    int_val = parent["child"]["field2"].as<int>();
    CHECK(int_val == 1);
    short_val = parent["child"]["field2"].as<short>();
    CHECK(short_val == 1);

    //json::object x = parent["child"].as<json::object>();
    // Compile time error, "as<Json::object> not supported"

    json empty;
    CHECK(empty.is_object());
    CHECK(empty.empty());

    //json::object y = empty.as<json::object>();
    // Compile time error, "as<Json::object> not supported"
}

TEST_CASE("test_as2")
{
    json obj;
    obj["field1"] = "10";
    obj["field2"] = "-10";
    obj["field3"] = "10.1";

    CHECK(10 == obj["field1"].as<int>());
    CHECK(-10 ==obj["field2"].as<int>());
    CHECK(10.1 == obj["field3"].as<double>());
}

TEST_CASE("test_is")
{
    json obj;
    obj["field1"] = 10;
    obj["field2"] = -10;
    obj["field3"] = 10U;

    CHECK(obj["field1"].storage_kind() == jsoncons::json_storage_kind::int64);
    CHECK(obj["field2"].storage_kind() == jsoncons::json_storage_kind::int64);
    CHECK(obj["field3"].storage_kind() == jsoncons::json_storage_kind::uint64);

    CHECK_FALSE(obj["field1"].is<std::string>());
    CHECK(obj["field1"].is<short>());
    CHECK(obj["field1"].is<int>());
    CHECK(obj["field1"].is<long>());
    CHECK(obj["field1"].is<long long>());
    CHECK(obj["field1"].is<unsigned int>());
    CHECK(obj["field1"].is<unsigned long>());
    CHECK(obj["field1"].is<unsigned long long>());
    CHECK_FALSE(obj["field1"].is<double>());

    CHECK_FALSE(obj["field2"].is<std::string>());
    CHECK(obj["field2"].is<short>());
    CHECK(obj["field2"].is<int>());
    CHECK(obj["field2"].is<long>());
    CHECK(obj["field2"].is<long long>());
    CHECK_FALSE(obj["field2"].is<unsigned short>());
    CHECK_FALSE(obj["field2"].is<unsigned int>());
    CHECK_FALSE(obj["field2"].is<unsigned long>());
    CHECK_FALSE(obj["field2"].is<unsigned long long>());
    CHECK_FALSE(obj["field2"].is<double>());

    CHECK_FALSE(obj["field3"].is<std::string>());
    CHECK(obj["field3"].is<short>());
    CHECK(obj["field3"].is<int>());
    CHECK(obj["field3"].is<long>());
    CHECK(obj["field3"].is<long long>());
    CHECK(obj["field3"].is<unsigned int>());
    CHECK(obj["field3"].is<unsigned long>());
    CHECK(obj["field3"].is<unsigned long long>());
    CHECK_FALSE(obj["field3"].is<double>());
}

TEST_CASE("test_is2")
{
    json obj = json::parse("{\"field1\":10}");

    CHECK(obj["field1"].storage_kind() == jsoncons::json_storage_kind::uint64);

    CHECK_FALSE(obj["field1"].is<std::string>());
    CHECK(obj["field1"].is<int>());
    CHECK(obj["field1"].is<long>());
    CHECK(obj["field1"].is<long long>());
    CHECK(obj["field1"].is<unsigned int>());
    CHECK(obj["field1"].is<unsigned long>());
    CHECK(obj["field1"].is<unsigned long long>());
    CHECK_FALSE(obj["field1"].is<double>());
}

TEST_CASE("test_is_type")
{
    json obj;
    CHECK(obj.is_object());
    CHECK(obj.is_object());

    // tests for proxy is_type methods
    obj["string"] = "val1";

    CHECK(obj.is_object());
    CHECK(obj.is_object());

    CHECK(obj["string"].is_string());
    CHECK(obj["string"].is<std::string>());

    obj["double"] = 10.7;
    CHECK(obj["double"].is_double());
    CHECK(obj["double"].is<double>());

    obj["int"] = -10;
    CHECK(obj["int"].is_int64());
    CHECK(obj["int"].is<long long>());

    obj["uint"] = 10u;
    CHECK(obj["uint"].is_uint64());
    CHECK(obj["uint"].is<unsigned long long>());

    obj["long"] = static_cast<long>(10);
    CHECK(obj["long"].is_int64());
    CHECK(obj["long"].is<long long>());

    obj["ulong"] = static_cast<unsigned long>(10);
    CHECK(obj["ulong"].is_uint64());
    CHECK(obj["ulong"].is<unsigned long long>());

    obj["longlong"] = static_cast<long long>(10);
    CHECK(obj["longlong"].is_int64());
    CHECK(obj["longlong"].is<long long>());

    obj["ulonglong"] = static_cast<unsigned long long>(10);
    CHECK(obj["ulonglong"].is_uint64());
    CHECK(obj["ulonglong"].is<unsigned long long>());

    obj["true"] = true;
    CHECK(obj["true"].is_bool());
    CHECK(obj["true"].is<bool>());

    obj["false"] = false;
    CHECK(obj["false"].is_bool());
    CHECK(obj["false"].is<bool>());

    obj["null1"] = json::null();
    CHECK(obj["null1"].is_null());

    obj["object"] = json();
    CHECK(obj["object"].is_object());
    CHECK(obj["object"].is_object());

    obj["array"] = json(json_array_arg);
    CHECK(obj["array"].is_array());
    CHECK(obj["array"].is_array());

    // tests for json is_type methods

    json str = obj["string"];
    CHECK(str.is<std::string>());
    CHECK(str.is<std::string>());
}

TEST_CASE("test_object_get_defaults")
{
    json obj;

    obj["field1"] = 1;
    obj["field3"] = "Toronto";

    double x1 = obj.contains("field1") ? obj["field1"].as<double>() : 10.0;
    double x2 = obj.contains("field2") ? obj["field2"].as<double>() : 20.0;


    CHECK(x1 == 1.0);
    CHECK(x2 == 20.0);

    std::string s1 = obj.get_value_or<std::string> ("field3", "Montreal");
    std::string s2 = obj.get_value_or<std::string> ("field4", "San Francisco");

    CHECK(s1 =="Toronto");
    CHECK(s2 == "San Francisco");
}

TEST_CASE("test_object_accessing")
{
    json obj;
    obj["first_name"] = "Jane";
    obj["last_name"] = "Roe";
    obj["events_attended"] = 10;
    obj["accept_waiver_of_liability"] = true;

    CHECK(obj["first_name"].as<std::string>() == "Jane");
    CHECK(obj.at("last_name").as<std::string>() == "Roe");
    CHECK(obj["events_attended"].as<int>() == 10);
    CHECK(obj["accept_waiver_of_liability"].as<bool>());
}

TEST_CASE("test_value_not_found_and_defaults")
{
    json obj;
    obj["first_name"] = "Jane";
    obj["last_name"] = "Roe";

    CHECK_NOTHROW(obj["outdoor_experience"].as<std::string>());

    std::string experience = obj.contains("outdoor_experience") ? obj["outdoor_experience"].as<std::string>() : "";

    CHECK(experience == "{}");
}

TEST_CASE("test_set_override")
{
    json obj;
    obj["first_name"] = "Jane";
    obj["height"] = 0.9;

    obj["first_name"] = "Joe";
    obj["height"] = "0.3";

    CHECK(obj["first_name"] == "Joe");
    CHECK(obj["height"].as<double>() == Approx(0.3).epsilon(0.00000000001));
}

TEST_CASE("try_emplace tests")
{
    json j = json::parse(R"(
    {
        "a" : 1,
        "b" : 2
    }
    )");

    json expected = json::parse(R"(
    {
        "a" : 1,
        "b" : 2,
        "c" : 3
    }
    )");

    SECTION("try_emplace(const string_view_type& name, Args&&... args)")
    {
        j.try_emplace("c",3);

        CHECK(expected == j);
    }

    SECTION("try_emplace(iterator hint, const string_view_type& name, Args&&... args)")
    {
        json::object_iterator it = j.object_range().begin();

        j.try_emplace(it,"c",3);

        CHECK(expected == j);
    }
}

TEST_CASE("test json_object erase with iterator")
{
    SECTION("json erase with iterator")
    {
        json j(jsoncons::json_object_arg);

        j.try_emplace("a", 1);
        j.try_emplace("b", 2);
        j.try_emplace("c", 3);

        auto it = j.object_range().begin();
        while (it != j.object_range().end())
        {
            if (it->key() == "a" || it->key() == "c")
            {
                it = j.erase(it);
            }
            else
            {
                it++;
            }
        }

        CHECK(j.size() == 1);
        CHECK(j.at("b") == 2);
        CHECK(j["b"] == 2);
    }
    SECTION("json erase with iterator 2")
    {
        json j(jsoncons::json_object_arg);

        j.try_emplace("a", 1);
        j.try_emplace("b", 2);
        j.try_emplace("c", 3);

        auto it = j.object_range().begin();
        while (it != j.object_range().end())
        {
            if (it->key() == "a")
            {
                it = j.erase(it,it+2);
            }
            else
            {
                it++;
            }
        }

        CHECK(j.size() == 1);
        CHECK(j.at("c") == 3);
        CHECK(j["c"] == 3);
    }
}

TEST_CASE("test empty json_object iterator")
{
    SECTION("test 1")
    {
        json j;

        json::const_object_iterator it;
        CHECK(!it.has_value());

        it = j.find("Min");
        CHECK(!it.has_value());

        CHECK(!j.object_range().end().has_value());


        CHECK ((bool)(it == j.object_range().end()));
    }
}

// merge tests

TEST_CASE("test_json_merge")
{
    json j = json::parse(R"(
    {
        "a" : 1,
        "b" : 2
    }
    )");
    json j2 = j;

    const json source = json::parse(R"(
    {
        "a" : 2,
        "c" : 3
    }
    )");

    const json expected = json::parse(R"(
    {
        "a" : 1,
        "b" : 2,
        "c" : 3
    }
    )");

    SECTION("test 1")
    {
        j.merge(source);
        CHECK(expected == j);

        j2.merge(j2.object_range().begin()+1,source);
        CHECK(expected == j2);
    }

    SECTION("test 2")
    {
        json empty_object;
        json original = j;

        j.merge(empty_object);

        CHECK(j == original);

        j2.merge(j2.object_range().begin()+1,empty_object);
        CHECK(j2 == original);
    }

    //std::cout << j << '\n';
}

TEST_CASE("test_json_merge_move")
{
    json j = json::parse(R"(
    {
        "a" : "1",
        "b" : [1,2,3]
    }
    )");
        json j2 = j;

    json source = json::parse(R"(
    {
        "a" : "2",
        "c" : [4,5,6]
    }
    )");

    json expected = json::parse(R"(
    {
        "a" : "1",
        "b" : [1,2,3],
        "c" : [4,5,6]
    }
    )");

    SECTION("test 1")
    {
        json source2 = source;

        j.merge(std::move(source));
        CHECK(expected == j);

        j2.merge(std::move(source2));
        CHECK(expected == j2);
    }
}

// merge_or_update tests

TEST_CASE("test_json_merge_or_update")
{
    json j = json::parse(R"(
    {
        "a" : 1,
        "b" : 2
    }
    )");
    json j2 = j;

    const json source = json::parse(R"(
    {
        "a" : 2,
        "c" : 3
    }
    )");

    const json expected = json::parse(R"(
    {
        "a" : 2,
        "b" : 2,
        "c" : 3
    }
    )");

    SECTION("test 1")
    {
        j.merge_or_update(source);
        CHECK(expected == j);

        j2.merge_or_update(j2.object_range().begin()+1,source);
        CHECK(expected == j2);
    }
}

TEST_CASE("test_json_merge_or_update_move")
{
    json j = json::parse(R"(
    {
        "a" : "1",
        "b" : [1,2,3]
    }
    )");
        json j2 = j;

    json source = json::parse(R"(
    {
        "a" : "2",
        "c" : [4,5,6]
    }
    )");

    json expected = json::parse(R"(
    {
        "a" : "2",
        "b" : [1,2,3],
        "c" : [4,5,6]
    }
    )");

    SECTION("test 1")
    {
        json source2 = source;

        j.merge_or_update(std::move(source));
        CHECK(expected == j);

        j2.merge_or_update(std::move(source2));
        CHECK(expected == j2);
    }
}

