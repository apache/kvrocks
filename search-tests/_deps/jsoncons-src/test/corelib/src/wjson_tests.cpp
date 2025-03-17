// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <jsoncons/json_filter.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <iostream>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("test_wjson")
{
    wjson root;
    root[L"field1"] = L"test";
    root[L"field2"] = 3.9;
    root[L"field3"] = true;

    CHECK(root[L"field1"].as<std::wstring>() == L"test");
    CHECK(root[L"field2"].as<double>() == 3.9);
    CHECK(root[L"field3"].as<bool>() == true);

    std::wstring s1 = root[L"field1"].as<std::wstring>();
    CHECK(s1 == L"test");
}

TEST_CASE("test_wjson_escape_u")
{
    std::wstring input = L"[\"\\uABCD\"]";
    std::wistringstream is(input);

    wjson root = wjson::parse(is);

    std::wstring s = root[0].as<std::wstring>();
    CHECK( s.length() == 1 );
    CHECK( s[0] == 0xABCD );
}

TEST_CASE("wjson serialization tests")
{
    jsoncons::wjson testBlock;
    testBlock[L"foo"] = true;
    testBlock[L"bar"] = false;
    testBlock[L"baz"] = true;
    std::wstring testStr;
    testBlock.dump(testStr);

    CHECK(testStr == L"{\"bar\":false,\"baz\":true,\"foo\":true}");
}

TEST_CASE("wjson pretty print tests")
{
    jsoncons::wjson testBlock;
    testBlock[L"foo"] = true;
    testBlock[L"bar"] = false;
    testBlock[L"baz"] = true;
    std::wostringstream actualStr;
    actualStr << jsoncons::pretty_print(testBlock);

    std::wostringstream expectedStr;
    expectedStr << L"{" << '\n';
    expectedStr << L"    \"bar\": false, " << '\n';
    expectedStr << L"    \"baz\": true, " << '\n';
    expectedStr << L"    \"foo\": true" << '\n';
    expectedStr << L"}";

    CHECK(actualStr.str().size() == expectedStr.str().size());
    CHECK(actualStr.str() == expectedStr.str());
}

TEST_CASE("wjson test case")
{
    std::wstring data = LR"(
    {"call":"script","cwd":"C:\\Users\\Robert\\Documents\\Visual Studio 2015\\Projects\\EscPosPrinter\\Release\\","file":"scripts\\pos-submitorder.js","filename":"pos-submitorder.js","lib":"function",
"params":{"data":{"cash":0,"coupons":0,"creditcard":0,"debit":0,"discounts":0,"name":null,"neworder":true,"operator":"","orders":[{"active":"1","addtoitem":"0","bar":"1","cat":"Beer","cooking":"","id":"7","kitchen":"0","modifier":"0","name":"Budwiser","noqty":"1","oneof":"[]","operator":"robert","options":"[]","price":"5","print":"","qty":1,"server":"robert","sideprice":"0","subtotal":5,"type":"Bar","uid":"0242.7559"}],"outstanding":5.25,"payments":[],"server":"robert","status":"0","subtotal":5,"tableid":"quickserv","taxes":0.25,"tip":0,"total":5.25,"uid":"2822.7128","voiditems":[]},"posstation":{"printers":{"kitchen":[{"arguments":{"baud":"9600","bits":"8","nparity":"0","port":"3","stopbit":"0","xonxoff":"5"},"model":"epson","path":"localhost","type":"com"},{"arguments":{"baud":"","bits":"","nparity":"","port":"","stopbit":"","xonxoff":""},"model":"screen","path":"temp-pc","type":"screen"}],"receipt":[{"arguments":{"baud":"9600","bits":"8","nparity":"0","port":"3","stopbit":"0","xonxoff":"5"},"model":"epson","path":"Temp-PC","type":"com"},{"arguments":{"baud":"","bits":"","nparity":"","port":"","stopbit":"","xonxoff":""},"model":"screen","path":"localhost","type":"screen"}]}}},"plugin":"clib"}
    )";

    wjson j = wjson::parse(data);

    std::wstring s = j[L"params"].to_string();

    std::wcout << s << "\n";
}
