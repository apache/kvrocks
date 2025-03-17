// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h"
#endif
#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons/json.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <limits>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("cbor_test_floating_point")
{
    json j1;
    j1["max double"] = (std::numeric_limits<double>::max)();
    j1["max float"] = (std::numeric_limits<float>::max)();

    std::vector<uint8_t> v;
    cbor::encode_cbor(j1, v);

    json j2 = cbor::decode_cbor<json>(v);
    CHECK(j2 == j1);

    json j3 = cbor::decode_cbor<json>(v.begin(), v.end());
    CHECK(j3 == j1);
} 

TEST_CASE("cbor_test")
{
    json j1;
    j1["zero"] = 0;
    j1["one"] = 1;
    j1["two"] = 2;
    j1["null"] = null_type();
    j1["true"] = true;
    j1["false"] = false;
    j1["max int64_t"] = (std::numeric_limits<int64_t>::max)();
    j1["max uint64_t"] = (std::numeric_limits<uint64_t>::max)();
    j1["min int64_t"] = (std::numeric_limits<int64_t>::lowest)();
    j1["max int32_t"] = (std::numeric_limits<int32_t>::max)();
    j1["max uint32_t"] = (std::numeric_limits<uint32_t>::max)();
    j1["min int32_t"] = (std::numeric_limits<int32_t>::lowest)();
    j1["max int16_t"] = (std::numeric_limits<int16_t>::max)();
    j1["max uint16_t"] = (std::numeric_limits<uint16_t>::max)();
    j1["min int16_t"] = (std::numeric_limits<int16_t>::lowest)();
    j1["max int8_t"] = (std::numeric_limits<int8_t>::max)();
    j1["max uint8_t"] = (std::numeric_limits<uint8_t>::max)();
    j1["min int8_t"] = (std::numeric_limits<int8_t>::lowest)();
    j1["max double"] = (std::numeric_limits<double>::max)();
    j1["min double"] = (std::numeric_limits<double>::lowest)();
    j1["max float"] = (std::numeric_limits<float>::max)();
    j1["zero float"] = 0.0;
    j1["min float"] = (std::numeric_limits<float>::lowest)();
    j1["String too long for small string optimization"] = "String too long for small string optimization";

    json ja(json_array_arg);
    ja.push_back(0);
    ja.push_back(1);
    ja.push_back(2);
    ja.push_back(null_type());
    ja.push_back(true);
    ja.push_back(false);
    ja.push_back((std::numeric_limits<int64_t>::max)());
    ja.push_back((std::numeric_limits<uint64_t>::max)());
    ja.push_back((std::numeric_limits<int64_t>::lowest)());
    ja.push_back((std::numeric_limits<int32_t>::max)());
    ja.push_back((std::numeric_limits<uint32_t>::max)());
    ja.push_back((std::numeric_limits<int32_t>::lowest)());
    ja.push_back((std::numeric_limits<int16_t>::max)());
    ja.push_back((std::numeric_limits<uint16_t>::max)());
    ja.push_back((std::numeric_limits<int16_t>::lowest)());
    ja.push_back((std::numeric_limits<int8_t>::max)());
    ja.push_back((std::numeric_limits<uint8_t>::max)());
    ja.push_back((std::numeric_limits<int8_t>::lowest)());
    ja.push_back((std::numeric_limits<double>::max)());
    ja.push_back((std::numeric_limits<double>::lowest)());
    ja.push_back((std::numeric_limits<float>::max)());
    ja.push_back(0.0);
    ja.push_back((std::numeric_limits<float>::lowest)());
    ja.push_back("String too long for small string optimization");

    j1["An array"] = ja;

    std::vector<uint8_t> v;
    cbor::encode_cbor(j1, v);

    json j2 = cbor::decode_cbor<json>(v);
    CHECK(j2 == j1);

    json j3 = cbor::decode_cbor<json>(v.begin(), v.end());
    CHECK(j3 == j1);
} 

TEST_CASE("cbor_test2")
{
    wjson j1;
    j1[L"zero"] = 0;
    j1[L"one"] = 1;
    j1[L"two"] = 2;
    j1[L"null"] = null_type();
    j1[L"true"] = true;
    j1[L"false"] = false;
    j1[L"max int64_t"] = (std::numeric_limits<int64_t>::max)();
    j1[L"max uint64_t"] = (std::numeric_limits<uint64_t>::max)();
    j1[L"min int64_t"] = (std::numeric_limits<int64_t>::lowest)();
    j1[L"max int32_t"] = (std::numeric_limits<int32_t>::max)();
    j1[L"max uint32_t"] = (std::numeric_limits<uint32_t>::max)();
    j1[L"min int32_t"] = (std::numeric_limits<int32_t>::lowest)();
    j1[L"max int16_t"] = (std::numeric_limits<int16_t>::max)();
    j1[L"max uint16_t"] = (std::numeric_limits<uint16_t>::max)();
    j1[L"min int16_t"] = (std::numeric_limits<int16_t>::lowest)();
    j1[L"max int8_t"] = (std::numeric_limits<int8_t>::max)();
    j1[L"max uint8_t"] = (std::numeric_limits<uint8_t>::max)();
    j1[L"min int8_t"] = (std::numeric_limits<int8_t>::lowest)();
    j1[L"max double"] = (std::numeric_limits<double>::max)();
    j1[L"min double"] = (std::numeric_limits<double>::lowest)();
    j1[L"max float"] = (std::numeric_limits<float>::max)();
    j1[L"zero float"] = 0.0;
    j1[L"min float"] = (std::numeric_limits<float>::lowest)();
    j1[L"S"] = L"S";
    j1[L"String too long for small string optimization"] = L"String too long for small string optimization";

    wjson ja(json_array_arg);
    ja.push_back(0);
    ja.push_back(1);
    ja.push_back(2);
    ja.push_back(null_type());
    ja.push_back(true);
    ja.push_back(false);
    ja.push_back((std::numeric_limits<int64_t>::max)());
    ja.push_back((std::numeric_limits<uint64_t>::max)());
    ja.push_back((std::numeric_limits<int64_t>::lowest)());
    ja.push_back((std::numeric_limits<int32_t>::max)());
    ja.push_back((std::numeric_limits<uint32_t>::max)());
    ja.push_back((std::numeric_limits<int32_t>::lowest)());
    ja.push_back((std::numeric_limits<int16_t>::max)());
    ja.push_back((std::numeric_limits<uint16_t>::max)());
    ja.push_back((std::numeric_limits<int16_t>::lowest)());
    ja.push_back((std::numeric_limits<int8_t>::max)());
    ja.push_back((std::numeric_limits<uint8_t>::max)());
    ja.push_back((std::numeric_limits<int8_t>::lowest)());
    ja.push_back((std::numeric_limits<double>::max)());
    ja.push_back((std::numeric_limits<double>::lowest)());
    ja.push_back((std::numeric_limits<float>::max)());
    ja.push_back(0.0);
    ja.push_back((std::numeric_limits<float>::lowest)());
    ja.push_back(L"S");
    ja.push_back(L"String too long for small string optimization");

    j1[L"An array"] = ja;

    std::vector<uint8_t> v;
    cbor::encode_cbor(j1, v);

    wjson j2 = cbor::decode_cbor<wjson>(v);
    CHECK(j2 == j1);

    wjson j3 = cbor::decode_cbor<wjson>(v.begin(), v.end());
    CHECK(j3 == j1);
}

TEST_CASE("cbor_reputon_test")
{
ojson j1 = ojson::parse(R"(
{
   "application": "hiking",
   "reputons": [
   {
       "rater": "HikingAsylum",
       "assertion": "advanced",
       "rated": "Marilyn C",
       "rating": 0.90
     }
   ]
}
)");

    std::vector<uint8_t> v;
    cbor::encode_cbor(j1, v);

    ojson j2 = cbor::decode_cbor<ojson>(v);
    CHECK(j2 == j1);

    ojson j3 = cbor::decode_cbor<ojson>(v.begin(), v.end());
    CHECK(j3 == j1);
}

#if (defined(__GNUC__) || defined(__clang__)) && defined(JSONCONS_HAS_INT128) 
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
TEST_CASE("cbor json constructor __int64 tests")
{
    SECTION("test 1")
    {
        json j1("-18446744073709551617", semantic_tag::bigint);

        __int128 val1 = j1.as<__int128>();

        std::vector<uint8_t> expected = {0xc3,0x49,0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
        std::vector<uint8_t> data;
        cbor::encode_cbor(val1, data);
        CHECK(expected == data);

        auto val2 = cbor::decode_cbor<__int128>(data);

        CHECK((val2 == val1));
    }
}
TEST_CASE("cbor json constructor unsigned __int64 tests")
{
    SECTION("test 1")
    {
        json j1("18446744073709551616", semantic_tag::bigint);

        auto val1 = j1.as<unsigned __int128>();

        std::vector<uint8_t> expected = {0xc2,0x49,0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
        std::vector<uint8_t> data;
        cbor::encode_cbor(val1, data);
        CHECK(expected == data);

        auto val2 = cbor::decode_cbor<unsigned __int128>(data);

        CHECK((val2 == val1));
    }
}
#pragma GCC diagnostic pop
#endif

