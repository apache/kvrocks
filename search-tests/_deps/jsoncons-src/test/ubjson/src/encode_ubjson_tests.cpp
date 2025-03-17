// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/ubjson/ubjson.hpp>
#include <jsoncons/json.hpp>
#include <sstream>
#include <vector>
#include <catch/catch.hpp>

using namespace jsoncons;

void check_encode_ubjson(const std::vector<uint8_t>& expected, const json& j)
{
    std::vector<uint8_t> result;
    ubjson::encode_ubjson(j, result);
    if (result.size() != expected.size())
    {
        std::cout << std::hex << (int)expected[0] << " " << std::hex << (int)result[0] << '\n';
    }
    REQUIRE(result.size() == expected.size());
    for (std::size_t i = 0; i < expected.size(); ++i)
    {
        if (expected[i] != result[i])
        {
            std::cout << "Different " << i << "\n"; 
            for (std::size_t k = 0; k < expected.size(); ++k)
            {
                std::cout << std::hex << (int)expected[k] << " " << std::hex << (int)result[k] << '\n';
            }
        }
        REQUIRE(result[i] == expected[i]);
    }
}

void check_encode_ubjson(const std::vector<uint8_t>& expected, const std::vector<uint8_t>& result)
{
    if (result.size() != expected.size())
    {
        std::cout << std::hex << (int)expected[0] << " " << std::hex << (int)result[0] << '\n';
    }
    REQUIRE(result.size() == expected.size());
    for (std::size_t i = 0; i < expected.size(); ++i)
    {
        if (expected[i] != result[i])
        {
            std::cout << "Different " << i << "\n"; 
            for (std::size_t k = 0; k < expected.size(); ++k)
            {
                std::cout << std::hex << (int)expected[k] << " " << std::hex << (int)result[k] << '\n';
            }
        }
        REQUIRE(result[i] == expected[i]);
    }
}

TEST_CASE("encode_ubjson_test")
{
    check_encode_ubjson({'U',0x00},json(0U));
    check_encode_ubjson({'U',0x01},json(1U));
    check_encode_ubjson({'U',0x0a},json(10U));
    check_encode_ubjson({'U',0x17},json(23U));
    check_encode_ubjson({'U',0x18},json(24U));
    check_encode_ubjson({'U',0x7f},json(127U)); 
    check_encode_ubjson({'U',0xff},json(255U));
    check_encode_ubjson({'I',0x01,0x00},json(256U));
    check_encode_ubjson({'l',0,1,0x00,0x00},json(65536U));
    check_encode_ubjson({'L',0,0,0,1,0,0,0,0},json(4294967296U));

    check_encode_ubjson({'U',0x01},json(1));
    check_encode_ubjson({'U',0x0a},json(10));
    check_encode_ubjson({'U',0x17},json(23)); 
    check_encode_ubjson({'U',0x18},json(24)); 
    check_encode_ubjson({'U',0x7f},json(127)); 

    check_encode_ubjson({'U',0xff},json(255));
    check_encode_ubjson({'I',0x01,0x00},json(256));
    check_encode_ubjson({'l',0,1,0x00,0x00},json(65536));
    check_encode_ubjson({'L',0,0,0,1,0,0,0,0},json(4294967296));
    check_encode_ubjson({'L',0x7f,0xff,0xff,0xff,0xff,0xff,0xff,0xff},json((std::numeric_limits<int64_t>::max)()));

    check_encode_ubjson({'i',0xe0},json(-32));
    check_encode_ubjson({'i',0xff},json(-1)); //

    // negative integers
    check_encode_ubjson({'I',0xff,0},json(-256));
    check_encode_ubjson({'I',0xfe,0xff},json(-257));
    check_encode_ubjson({'l',0xff,0xff,0,0},json(-65536));
    check_encode_ubjson({'l',0xff,0xfe,0xff,0xff},json(-65537));
    check_encode_ubjson({'L',0xff,0xff,0xff,0xff,0,0,0,0},json(-4294967296));
    check_encode_ubjson({'L',0xff,0xff,0xff,0xfe,0xff,0xff,0xff,0xff},json(-4294967297));

    // null, true, false
    check_encode_ubjson({'Z'},json::null()); // 
    check_encode_ubjson({'T'},json(true)); //
    check_encode_ubjson({'F'},json(false)); //

    // floating point
    check_encode_ubjson({'d',0,0,0,0},json(0.0));
    check_encode_ubjson({'d',0xbf,0x80,0,0},json(-1.0));
    check_encode_ubjson({'d',0xcb,0x7f,0xff,0xff},json(-16777215.0));

    // string
    check_encode_ubjson({'S','U',0x00},json(""));
    check_encode_ubjson({'S','U',0x01,' '},json(" "));
    check_encode_ubjson({'S','U',0x1f,'1','2','3','4','5','6','7','8','9','0',
                       '1','2','3','4','5','6','7','8','9','0',
                       '1','2','3','4','5','6','7','8','9','0',
                       '1'},
                 json("1234567890123456789012345678901"));
    check_encode_ubjson({'S','U',0x20,'1','2','3','4','5','6','7','8','9','0',
                            '1','2','3','4','5','6','7','8','9','0',
                            '1','2','3','4','5','6','7','8','9','0',
                            '1','2'},
                 json("12345678901234567890123456789012"));
}
TEST_CASE("encode_ubjson_arrays_and_maps")
{
    check_encode_ubjson({'[','#','U',0x00}, json(json_array_arg));
    check_encode_ubjson({'{','#','U',0x00},json());
    check_encode_ubjson({'[','#','U',0x01,'U',0x00},json::parse("[0]"));
    check_encode_ubjson({'[','#','U',0x02,'U',0x00,'U',0x00},json::parse("[0,0]"));
    check_encode_ubjson({'[','#','U',0x02,
                         '[','#','U',0x01,'U',0x00,
                         'U',0x00},json::parse("[[0],0]"));
    check_encode_ubjson({'[','#','U',0x01,'S','U',0x05,'H','e','l','l','o'},json::parse("[\"Hello\"]"));
    check_encode_ubjson({'{','#','U',0x01,'U',0x02,'o','c','[','#','U',0x01,'U',0x00}, json::parse("{\"oc\": [0]}"));
    check_encode_ubjson({'{','#','U',0x01,'U',0x02,'o','c','[','#','U',0x04,'U',0x00,'U',0x01,'U',0x02,'U',0x03}, json::parse("{\"oc\": [0,1,2,3]}"));
}

TEST_CASE("encode indefinite length ubjson arrays and maps")
{
    std::vector<uint8_t> v;
    ubjson::ubjson_bytes_encoder encoder(v);

    SECTION("[\"Hello\"]")
    {
        encoder.begin_array();
        encoder.string_value("Hello");
        encoder.end_array();

        check_encode_ubjson({'[','S','U',0x05,'H','e','l','l','o',']'}, v);
    }

    SECTION("{\"oc\": [0]}")
    {
        encoder.begin_object();
        encoder.key("oc");
        encoder.begin_array();
        encoder.uint64_value(0);
        encoder.end_array();
        encoder.end_object();

        check_encode_ubjson({'{','U',0x02,'o','c','[','U',0x00,']','}'}, v);
    }

    SECTION("{\"oc\": [0,1,2,3]}")
    {
        encoder.begin_object();
        encoder.key("oc");
        encoder.begin_array();
        encoder.uint64_value(0);
        encoder.uint64_value(1);
        encoder.uint64_value(2);
        encoder.uint64_value(3);
        encoder.end_array();
        encoder.end_object();

        check_encode_ubjson({'{','U',0x02,'o','c','[','U',0x00,'U',0x01,'U',0x02,'U',0x03,']','}'}, v);
    }
}

namespace { namespace ns {

    struct Person
    {
        std::string name;
    };

}}

JSONCONS_ALL_MEMBER_TRAITS(ns::Person, name)
    
TEST_CASE("encode_ubjson overloads")
{
    SECTION("json, stream")
    {
        json person;
        person.try_emplace("name", "John Smith");

        std::string s;
        std::stringstream ss(s);
        ubjson::encode_ubjson(person, ss);
        json other = ubjson::decode_ubjson<json>(ss);
        CHECK(other == person);
    }
    SECTION("custom, stream")
    {
        ns::Person person{"John Smith"};

        std::string s;
        std::stringstream ss(s);
        ubjson::encode_ubjson(person, ss);
        ns::Person other = ubjson::decode_ubjson<ns::Person>(ss);
        CHECK(other.name == person.name);
    }
}

#if defined(JSONCONS_HAS_STATEFUL_ALLOCATOR) && JSONCONS_HAS_STATEFUL_ALLOCATOR == 1

#include <scoped_allocator>
#include <common/free_list_allocator.hpp>

template <typename T>
using MyScopedAllocator = std::scoped_allocator_adaptor<free_list_allocator<T>>;

TEST_CASE("encode_ubjson allocator_set overloads")
{
    MyScopedAllocator<char> temp_alloc(1);

    auto alloc_set = temp_allocator_only(temp_alloc);

    SECTION("json, stream")
    {
        json person;
        person.try_emplace("name", "John Smith");

        std::string s;
        std::stringstream ss(s);
        ubjson::encode_ubjson(alloc_set, person, ss);
        json other = ubjson::decode_ubjson<json>(alloc_set, ss);
        CHECK(other == person);
    }
    SECTION("custom, stream")
    {
        ns::Person person{"John Smith"};

        std::string s;
        std::stringstream ss(s);
        ubjson::encode_ubjson(alloc_set, person, ss);
        ns::Person other = ubjson::decode_ubjson<ns::Person>(alloc_set, ss);
        CHECK(other.name == person.name);
    }
}

#endif
