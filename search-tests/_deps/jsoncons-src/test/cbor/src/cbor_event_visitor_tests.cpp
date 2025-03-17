// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h"
#endif

#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons/json.hpp>

#include <jsoncons/item_event_visitor.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <limits>
#include <catch/catch.hpp>

using namespace jsoncons;

class my_json_visitor : public default_json_visitor
{
    bool visit_begin_object(semantic_tag, const ser_context&, std::error_code&) override
    {
        std::cout << "visit_begin_object\n"; 
        return true;
    }

    bool visit_end_object(const ser_context&, std::error_code&) override
    {
        std::cout << "visit_end_object\n"; 
        return true;
    }
    bool visit_begin_array(semantic_tag, const ser_context&, std::error_code&) override
    {
        std::cout << "visit_begin_array\n"; 
        return true;
    }

    bool visit_end_array(const ser_context&, std::error_code&) override
    {
        std::cout << "visit_end_array\n"; 
        return true;
    }

    bool visit_key(const string_view_type& s, const ser_context&, std::error_code&) override
    {
        std::cout << "visit_key " << s << "\n"; 
        return true;
    }
    bool visit_string(const string_view_type& s, semantic_tag, const ser_context&, std::error_code&) override
    {
        std::cout << "visit_string " << s << "\n"; 
        return true;
    }
    bool visit_int64(int64_t val, semantic_tag, const ser_context&, std::error_code&) override
    {
        std::cout << "visit_int64 " << val << "\n"; 
        return true;
    }
    bool visit_uint64(uint64_t val, semantic_tag, const ser_context&, std::error_code&) override
    {
        std::cout << "visit_uint64 " << val << "\n"; 
        return true;
    }
    bool visit_bool(bool val, semantic_tag, const ser_context&, std::error_code&) override
    {
        std::cout << "visit_bool " << val << "\n"; 
        return true;
    }

    bool visit_typed_array(const span<const uint16_t>& s, 
                                semantic_tag tag, 
                                const ser_context&, 
                                std::error_code&) override  
    {
        std::cout << "visit_typed_array uint16_t " << tag << "\n"; 
        for (auto val : s)
        {
            std::cout << val << "\n";
        }
        std::cout << "\n";
        return true;
    }

    bool visit_typed_array(half_arg_t, const span<const uint16_t>& s,
        semantic_tag tag,
        const ser_context&,
        std::error_code&) override
    {
        std::cout << "visit_typed_array half_arg_t uint16_t " << tag << "\n";
        for (auto val : s)
        {
            std::cout << val << "\n";
        }
        std::cout << "\n";
        return true;
    }
};

TEST_CASE("item_event_visitor cbor 1")
{
    std::vector<uint8_t> input = {0xa2,
                                      0xa1, // object (1), key
                                          0x62,'o','c', // string, key
                                          0x81,0, // array(1), value
                                      0x61, 'a', // string(1), value
                                      0xa0, // object(0), key
                                      0 // value
    };

    json expected = json::parse(R"(
        {"{\"oc\":[0]}":"a","{}":0}
    )");

    SECTION("test 1")
    {
        json_decoder<json> destination;
        item_event_visitor_to_visitor_adaptor visitor{destination};

        cbor::basic_cbor_parser<bytes_source> parser{ bytes_source(input) };

        std::error_code ec;
        parser.parse(visitor, ec);
        CHECK(expected == destination.get_result());
    }
}

TEST_CASE("item_event_visitor cbor 2")
{
    std::vector<uint8_t> input = {0xa2,
                                      0xa2, // object (2), key
                                          0x62,'a','a', // string, key
                                          0x81,0, // array(1), value
                                          0x62,'b','b', // string, key
                                          0x81,1, // array(1), value
                                      0x61, 'a', // string(1), value
                                      0xa0, // object(0), key
                                      0 // value
    };

    json expected = json::parse(R"(
        {"{\"aa\":[0],\"bb\":[1]}":"a","{}":0}
    )");

    SECTION("test 1")
    {
        json_decoder<json> destination;
        item_event_visitor_to_visitor_adaptor visitor{destination};

        cbor::basic_cbor_parser<bytes_source> parser{ bytes_source(input) };

        std::error_code ec;
        parser.parse(visitor, ec);
        //std::cout << destination.get_result() << "\n";
        CHECK(expected == destination.get_result());
    }
}

TEST_CASE("item_event_visitor cbor 3")
{
    std::vector<uint8_t> input = {0xa2,
                                      0xa2, // object (2), key
                                          0x62,'a','a', // string, key
                                          0x81,0, // array(1), value
                                          0xa0, // string, key
                                          0x81,1, // array(1), value
                                      0x61, 'a', // string(1), value
                                      0xa0, // object(0), key
                                      0 // value
    };

    json expected = json::parse(R"(
        {"{\"aa\":[0],{}:[1]}":"a","{}":0}
    )");

    SECTION("test 1")
    {
        json_decoder<json> destination;
        item_event_visitor_to_visitor_adaptor visitor{destination};

        cbor::basic_cbor_parser<bytes_source> parser{ bytes_source(input) };

        std::error_code ec;
        parser.parse(visitor, ec);
        //std::cout << destination.get_result() << "\n";
        CHECK(expected == destination.get_result());
    }
}

TEST_CASE("item_event_visitor cbor 4")
{
    std::vector<uint8_t> input = {0xa2,
                                      0xa2, // object (2), key
                                          0x62,'a','a', // string, key
                                          0x81,0, // array(1), value
                                          0x80, // array, key
                                          0x81,1, // array(1), value
                                      0x61, 'a', // string(1), value
                                      0xa0, // object(0), key
                                      0 // value
    };

    json expected = json::parse(R"(
        {"{\"aa\":[0],[]:[1]}":"a","{}":0}
    )");

    SECTION("test 1")
    {
        json_decoder<json> destination;
        item_event_visitor_to_visitor_adaptor visitor{destination};

        cbor::basic_cbor_parser<bytes_source> parser{ bytes_source(input) };

        std::error_code ec;
        parser.parse(visitor, ec);
        //std::cout << destination.get_result() << "\n";
        CHECK(expected == destination.get_result());
    }
}

TEST_CASE("item_event_visitor cbor 5")
{
    std::vector<uint8_t> input = {0xa2,
                                      0x84, // array(4), key
                                         0,
                                         1,
                                         2,
                                         3,
                                      0x61, 'a', // string(1), value
                                      0x80, // array(0), key
                                      0 // value
    };

    json expected = json::parse(R"(
        {"[0,1,2,3]":"a","[]":0}
    )");

    SECTION("test 1")
    {
        json_decoder<json> destination;
        item_event_visitor_to_visitor_adaptor visitor{destination};

        cbor::basic_cbor_parser<bytes_source> parser{ bytes_source(input) };

        std::error_code ec;
        parser.parse(visitor, ec);
        //std::cout << destination.get_result() << "\n";
        CHECK(expected == destination.get_result());
    }
}

TEST_CASE("item_event_visitor cbor 6")
{
    const std::vector<uint8_t> input = {
        0x9f, // Start indefinte length array
          0x83, // Array of length 3
            0x63, // String value of length 3
              0x66,0x6f,0x6f, // "foo" 
            0x44, // Byte string value of length 4
              0x50,0x75,0x73,0x73, // 'P''u''s''s'
            0xc5, // Tag 5 (bigfloat)
              0x82, // Array of length 2
                0x20, // -1
                0x03, // 3   
          0x83, // Another array of length 3
            0x63, // String value of length 3
              0x62,0x61,0x72, // "bar"
            0xd6, // Expected conversion to base64
            0x44, // Byte string value of length 4
              0x50,0x75,0x73,0x73, // 'P''u''s''s'
            0xc4, // Tag 4 (decimal fraction)
              0x82, // Array of length 2
                0x38, // Negative integer of length 1
                  0x1c, // -29
                0xc2, // Tag 2 (positive bignum)
                  0x4d, // Byte string value of length 13
                    0x01,0x8e,0xe9,0x0f,0xf6,0xc3,0x73,0xe0,0xee,0x4e,0x3f,0x0a,0xd2,
        0xff // "break"
    };

    SECTION("test 1")
    {
        json_decoder<json> destination;
        item_event_visitor_to_visitor_adaptor visitor{destination};

        cbor::basic_cbor_parser<bytes_source> parser{ bytes_source(input) };

        std::error_code ec;
        parser.parse(visitor, ec);
        //std::cout << destination.get_result() << "\n";
    }

    SECTION("test 2")
    {
        auto j1 = cbor::decode_cbor<json>(input);

        auto val = cbor::decode_cbor<std::vector<std::tuple<std::string,jsoncons::byte_string,std::string>>>(input);

        // Serialize back to CBOR
        std::vector<uint8_t> buffer;
        cbor::encode_cbor(val, buffer);
        json j2 = cbor::decode_cbor<json>(buffer);
        CHECK(j2 == j1);
    }
}

TEST_CASE("cbor_parser reset")
{
    std::vector<uint8_t> input1 = {
        0x82,0x01,0x02, // array(2), unsigned(1), unsigned(2)
        0xa1,0x61,0x63,0x04, // map(1), text(1), "c", unsigned(4)
    };

    std::vector<uint8_t> input2 = {
        0xa1,0x61,0x65,0x06, // map(1), text(1), "e", unsigned(6)
    };

    json expected1 = json::parse(R"([1,2])");
    json expected2 = json::parse(R"({"c":4})");
    json expected3 = json::parse(R"({"e":6})");

    json_decoder<json> destination;
    item_event_visitor_to_visitor_adaptor visitor{destination};
    cbor::basic_cbor_parser<bytes_source> parser{ input1 };
    std::error_code ec;

    SECTION("keeping same source")
    {
        parser.parse(visitor, ec);
        REQUIRE_FALSE(ec);
        CHECK(destination.get_result() == expected1);

        destination.reset();
        parser.reset();
        parser.parse(visitor, ec);
        CHECK_FALSE(ec);
        CHECK(parser.stopped());
        // TODO: This fails: CHECK(parser.done());
        CHECK(destination.get_result() == expected2);
    }

    SECTION("with different source")
    {
        parser.parse(visitor, ec);
        REQUIRE_FALSE(ec);
        CHECK(destination.get_result() == expected1);

        destination.reset();
        parser.reset(input2);
        parser.parse(visitor, ec);
        CHECK_FALSE(ec);
        CHECK(parser.stopped());
        // TODO: This fails: CHECK(parser.done());
        CHECK(destination.get_result() == expected3);
    }
}
