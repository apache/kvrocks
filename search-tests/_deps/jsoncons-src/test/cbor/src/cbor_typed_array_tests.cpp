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

static void check_native(std::true_type, 
                         const std::vector<uint8_t>& u,
                         const std::vector<uint8_t>& v)
{
    if (u != v)
    {
        for (auto c : u)
        {
            std::cout << std::hex  << std::setw(2) << std::setfill('0') << (int)c << " ";
        }
        std::cout << "\n";
        for (auto c : v)
        {
            std::cout << std::hex  << std::setw(2) << std::setfill('0') << (int)c << " ";
        }
        std::cout << "\n\n";
    }
    CHECK((u == v));
}

static void check_native(std::false_type, 
                         const std::vector<uint8_t>&,
                         const std::vector<uint8_t>&)
{
}

struct my_cbor_visitor : public default_json_visitor
{
    std::vector<double> v;
private:
    bool visit_typed_array(const span<const double>& data,  
                        semantic_tag,
                        const ser_context&,
                        std::error_code&) override
    {
        //std::cout << "visit_typed_array size: " << data.size() << "\n";
        v = std::vector<double>(data.begin(),data.end());
        return false;
    }
};

TEST_CASE("cbor multi dim row major cursor tests")
{
    SECTION("Tag 86, float64, little endian")
    {
        //std::cout << "CBOR multi dim typed array Tag 86, float64, little endian" << '\n';

        const std::vector<uint8_t> input = {
            0xd8,0x28,0x82,0x82,0x02,0x03,0x86,0x02,0x04,0x08,0x04,0x10,0x19,0x01,0x00
        };

        cbor::cbor_bytes_cursor cursor(input);
        for (; !cursor.done(); cursor.next())
        {
            //const auto& event = cursor.current();
            //std::cout << event.event_type() << " " << event.tag() << "\n";
        }
    }
}

TEST_CASE("cbor typed array cursor tests")
{
    SECTION("Tag 86, float64, little endian")
    {
        //std::cout << "CBOR cursor typed array Tag 86, float64, little endian" << '\n';

        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x56, // Tag 86, float64, little endian, Typed Array
            0x50, // Byte string value of length 16
                0xff,0xff,0xff,0xff,0xff,0xff,0xef,0xff,
                0xff,0xff,0xff,0xff,0xff,0xff,0xef,0x7f
        };

        cbor::cbor_bytes_cursor cursor(input);
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        CHECK(cursor.is_typed_array());

        my_cbor_visitor visitor;
        cursor.read_to(visitor);
        //for (auto item : visitor.v)
        //{
        //    std::cout << item << "\n";
        //}
    }
}

TEST_CASE("cbor typed array tests")
{
    SECTION("Tag 64 (uint8 Typed Array)")
    {
        //std::cout << "CBOR typed array Tag 64 (uint8 Typed Array)" << '\n';

        //std::cout << (int)detail::endian::native << "\n";

        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x40, // Tag 64, uint8, Typed Array
            0x43, // Byte string value of length 3
                0x00,0x01,0xff
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 64\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 3);
        CHECK(j[0].as<uint8_t>() == std::numeric_limits<uint8_t>::lowest());
        CHECK(j[1].as<uint8_t>() == uint8_t(1));
        CHECK(j[2].as<uint8_t>() == (std::numeric_limits<uint8_t>::max)());

        auto u = cbor::decode_cbor<std::vector<uint8_t>>(input);
        std::vector<uint8_t> v;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, v, options);
        CHECK((v == input));
    }

    SECTION("Tags 65 (uint16, big endian)")
    {
        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x41, // Tag 65, uint16, big endian, Typed Array
            0x46, // Byte string value of length 6
                0x00,0x00,0x00,0x01,0xff,0xff
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 65\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 3);
        CHECK(j[0].as<uint16_t>() == std::numeric_limits<uint16_t>::lowest());
        CHECK(j[1].as<uint16_t>() == uint16_t(1));
        CHECK(j[2].as<uint16_t>() == (std::numeric_limits<uint16_t>::max)());

        auto u = cbor::decode_cbor<std::vector<uint16_t>>(input);
        std::vector<uint8_t> v;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, v, options);
        check_native(std::integral_constant<bool,jsoncons::endian::native == jsoncons::endian::big>(),
                     input, v);
    }
    SECTION("Tag 66 (uint32, big endian)")
    {
        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x42, // Tag 66, uint32, big endian, Typed Array
            0x4c, // Byte string value of length 12
                0x00,0x00,0x00,0x00,
                0x00,0x00,0x00,0x01,
                0xff,0xff,0xff,0xff
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 66\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 3);
        CHECK(j[0].as<uint32_t>() == std::numeric_limits<uint32_t>::lowest());
        CHECK(j[1].as<uint32_t>() == uint32_t(1));
        CHECK(j[2].as<uint32_t>() == (std::numeric_limits<uint32_t>::max)());

        auto u = cbor::decode_cbor<std::vector<uint32_t>>(input);
        std::vector<uint8_t> v;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, v, options);
        check_native(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>(),
            input, v);
    }

    SECTION("Tags 67 (uint64,big endian)")
    {
        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x43, // Tag 67, uint64, big endian, Typed Array
            0x58,0x18, // Byte string value of length 24
                0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
                0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x01,
                0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 67\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 3);
        CHECK(j[0].as<uint64_t>() == std::numeric_limits<uint64_t>::lowest());
        CHECK(j[1].as<uint64_t>() == uint64_t(1));
        CHECK(j[2].as<uint64_t>() == (std::numeric_limits<uint64_t>::max)());

        auto u = cbor::decode_cbor<std::vector<uint64_t>>(input);
        std::vector<uint8_t> v;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, v, options);
        check_native(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>(),
            input, v);
    }

    SECTION("Tag 68 (uint8, Typed Array, clamped arithmetic)")
    {
        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x44, // Tag 68, uint8, Typed Array, clamped arithmetic
            0x43, // Byte string value of length 3
                0x00,0x01,0xff
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 68\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        CHECK(j.tag() == semantic_tag::clamped);
        REQUIRE(j.size() == 3);
        CHECK(j[0].as<uint8_t>() == std::numeric_limits<uint8_t>::lowest());
        CHECK(j[1].as<uint8_t>() == uint8_t(1));
        CHECK(j[2].as<uint8_t>() == (std::numeric_limits<uint8_t>::max)());

        auto v = cbor::decode_cbor<std::vector<uint8_t>>(input);
        REQUIRE(v.size() == 3);
        CHECK(v[0] == std::numeric_limits<uint8_t>::lowest());
        CHECK(v[1] == uint8_t(1));
        CHECK(v[2] == (std::numeric_limits<uint8_t>::max)());
    }

    SECTION("Tags 69 (uint16, little endian)")
    {
        //std::cout << "CBOR typed array Tags 69 (uint16, little endian)" << '\n';

        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x45, // Tag 69, uint16, little endian, Typed Array
            0x46, // Byte string value of length 6
                0x00,0x00,0x01,0x00,0xff,0xff
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 69\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 3);
        CHECK(j[0].as<uint16_t>() == std::numeric_limits<uint16_t>::lowest());
        CHECK(j[1].as<uint16_t>() == uint16_t(1));
        CHECK(j[2].as<uint16_t>() == (std::numeric_limits<uint16_t>::max)());

        auto u = cbor::decode_cbor<std::vector<uint16_t>>(input);
        REQUIRE(u.size() == 3);
        CHECK(u[0] == std::numeric_limits<uint16_t>::lowest());
        CHECK(u[1] == uint16_t(1));
        CHECK(u[2] == (std::numeric_limits<uint16_t>::max)());

        auto v = cbor::decode_cbor<std::vector<uint32_t>>(input);
        REQUIRE(v.size() == 3);
        CHECK(v[0] == std::numeric_limits<uint16_t>::lowest());
        CHECK(v[1] == uint16_t(1));
        CHECK(v[2] == (std::numeric_limits<uint16_t>::max)());

        auto w = cbor::decode_cbor<std::vector<uint64_t>>(input);
        REQUIRE(w.size() == 3);
        CHECK(w[0] == std::numeric_limits<uint16_t>::lowest());
        CHECK(w[1] == uint16_t(1));
        CHECK(w[2] == (std::numeric_limits<uint16_t>::max)());

        std::vector<uint8_t> buf;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, buf, options);
        check_native(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::little>(),
            input, buf);
    }

    SECTION("Tags 70 (uint32, little endian)")
    {
        //std::cout << "CBOR typed array Tags 70 (uint32, little endian)" << '\n';

        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x46, // Tag 70, uint32, little endian, Typed Array
            0x4c, // Byte string value of length 12
                0x00,0x00,0x00,0x00,
                0x01,0x00,0x00,0x00,
                0xff,0xff,0xff,0xff
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 70\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 3);
        CHECK(j[0].as<uint32_t>() == std::numeric_limits<uint32_t>::lowest());
        CHECK(j[1].as<uint32_t>() == uint32_t(1));
        CHECK(j[2].as<uint32_t>() == (std::numeric_limits<uint32_t>::max)());

        auto u = cbor::decode_cbor<std::vector<uint32_t>>(input);
        REQUIRE(u.size() == 3);
        CHECK(u[0] == std::numeric_limits<uint32_t>::lowest());
        CHECK(u[1] == uint32_t(1));
        CHECK(u[2] == (std::numeric_limits<uint32_t>::max)());

        auto v = cbor::decode_cbor<std::vector<uint64_t>>(input);
        REQUIRE(v.size() == 3);
        CHECK(v[0] == std::numeric_limits<uint32_t>::lowest());
        CHECK(v[1] == uint32_t(1));
        CHECK(v[2] == (std::numeric_limits<uint32_t>::max)());

        std::vector<uint8_t> buf;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, buf, options);
        check_native(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::little>(),
            input, buf);
    }
    SECTION("Tag 71 (uint64,little endian)")
    {
        //std::cout << "CBOR typed array Tag 71 (uint64,little endian)" << '\n';

        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x47, // Tag 71, uint64, little endian, Typed Array
            0x58,0x18, // Byte string value of length 24
                0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
                0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
                0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 71\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 3);
        CHECK(j[0].as<uint64_t>() == std::numeric_limits<uint64_t>::lowest());
        CHECK(j[1].as<uint64_t>() == uint64_t(1));
        CHECK(j[2].as<uint64_t>() == (std::numeric_limits<uint64_t>::max)());

        auto u = cbor::decode_cbor<std::vector<uint64_t>>(input);
        REQUIRE(u.size() == 3);
        CHECK(u[0] == std::numeric_limits<uint64_t>::lowest());
        CHECK(u[1] == uint64_t(1));
        CHECK(u[2] == (std::numeric_limits<uint64_t>::max)());

        std::vector<uint8_t> buf;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, buf, options);
        check_native(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::little>(),
            input, buf);
    }
    SECTION("Tag 72 (int8)")
    {
        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x48, // Tag 72, sint8, Typed Array
            0x43, // Byte string value of length 3
                0x80,0x01,0x7f
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 72\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 3);
        CHECK(j[0].as<int8_t>() == std::numeric_limits<int8_t>::lowest());
        CHECK(j[1].as<int8_t>() == int8_t(1));
        CHECK(j[2].as<int8_t>() == (std::numeric_limits<int8_t>::max)());

        auto u = cbor::decode_cbor<std::vector<int8_t>>(input);

        std::vector<uint8_t> buf;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, buf, options);
        check_native(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::little>(),
            input, buf);
    }
    SECTION("Tag 73 (int16, big endian)")
    {
        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x49, // Tag 73, sint16, big endian, Typed Array
            0x46, // Byte string value of length 6
                0x80,0x00,
                0x00,0x01,
                0x7f,0xff        
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 73\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 3);
        CHECK(j[0].as<int16_t>() == std::numeric_limits<int16_t>::lowest());
        CHECK(j[1].as<int16_t>() == int16_t(1));
        CHECK(j[2].as<int16_t>() == (std::numeric_limits<int16_t>::max)());

        auto u = cbor::decode_cbor<std::vector<int16_t>>(input);
        REQUIRE(u.size() == 3);
        CHECK(u[0] == std::numeric_limits<int16_t>::lowest());
        CHECK(u[1] == int16_t(1));
        CHECK(u[2] == (std::numeric_limits<int16_t>::max)());

        auto v = cbor::decode_cbor<std::vector<int32_t>>(input);
        REQUIRE(v.size() == 3);
        CHECK(v[0] == std::numeric_limits<int16_t>::lowest());
        CHECK(v[1] == int16_t(1));
        CHECK(v[2] == (std::numeric_limits<int16_t>::max)());

        auto w = cbor::decode_cbor<std::vector<int64_t>>(input);
        REQUIRE(w.size() == 3);
        CHECK(w[0] == std::numeric_limits<int16_t>::lowest());
        CHECK(w[1] == int16_t(1));
        CHECK(w[2] == (std::numeric_limits<int16_t>::max)());

        std::vector<uint8_t> buf;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, buf, options);
        check_native(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>(),
            input, buf);
    }

    SECTION("Tag 74 (int32, big endian)")
    {
        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x4a, // Tag 74, sint32, big endian, Typed Array
            0x4c, // Byte string value of length 12
            0x80,0x00,0x00,0x00,
            0x00,0x00,0x00,0x01,
            0x7f,0xff,0xff,0xff
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 74\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 3);
        CHECK(j[0].as<int32_t>() == std::numeric_limits<int32_t>::lowest());
        CHECK(j[1].as<int32_t>() == int32_t(1));
        CHECK(j[2].as<int32_t>() == (std::numeric_limits<int32_t>::max)());

        auto u = cbor::decode_cbor<std::vector<int32_t>>(input);
        REQUIRE(u.size() == 3);
        CHECK(u[0] == std::numeric_limits<int32_t>::lowest());
        CHECK(u[1] == int32_t(1));
        CHECK(u[2] == (std::numeric_limits<int32_t>::max)());

        auto v = cbor::decode_cbor<std::vector<int64_t>>(input);
        REQUIRE(v.size() == 3);
        CHECK(v[0] == std::numeric_limits<int32_t>::lowest());
        CHECK(v[1] == int32_t(1));
        CHECK(v[2] == (std::numeric_limits<int32_t>::max)());

        std::vector<uint8_t> buf;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, buf, options);
        check_native(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>(),
            input, buf);
    }
    SECTION("Tag 75 (int64,big endian)")
    {
        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x4b, // Tag 75, sint64, big endian, Typed Array
            0x58,0x18, // Byte string value of length 24
                0x80,0x0,0x0,0x0,0x0,0x0,0x0,0x0,
                0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x1,
                0x7f,0xff,0xff,0xff,0xff,0xff,0xff,0xff
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 75\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 3);
        CHECK(j[0].as<int64_t>() == std::numeric_limits<int64_t>::lowest());
        CHECK(j[1].as<int64_t>() == int64_t(1));
        CHECK(j[2].as<int64_t>() == (std::numeric_limits<int64_t>::max)());

        auto u = cbor::decode_cbor<std::vector<int64_t>>(input);
        REQUIRE(u.size() == 3);
        CHECK(u[0] == std::numeric_limits<int64_t>::lowest());
        CHECK(u[1] == int64_t(1));
        CHECK(u[2] == (std::numeric_limits<int64_t>::max)());

        std::vector<uint8_t> buf;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, buf, options);
        check_native(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>(),
            input, buf);
    }
    SECTION("Tag 77 (int16, little endian)")
    {
        //std::cout << "CBOR typed array Tag 77 (int16, little endian)" << '\n';

        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x4d, // Tag 77, sint16, little endian, Typed Array
            0x46, // Byte string value of length 6
                0x00,0x80,
                0x01,0x00,
                0xff,0x7f
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 77\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 3);
        CHECK(j[0].as<int16_t>() == std::numeric_limits<int16_t>::lowest());
        CHECK(j[1].as<int16_t>() == int16_t(1));
        CHECK(j[2].as<int16_t>() == (std::numeric_limits<int16_t>::max)());

        auto u = cbor::decode_cbor<std::vector<int16_t>>(input);
        REQUIRE(u.size() == 3);
        CHECK(u[0] == std::numeric_limits<int16_t>::lowest());
        CHECK(u[1] == int16_t(1));
        CHECK(u[2] == (std::numeric_limits<int16_t>::max)());

        auto v = cbor::decode_cbor<std::vector<int32_t>>(input);
        REQUIRE(v.size() == 3);
        CHECK(v[0] == std::numeric_limits<int16_t>::lowest());
        CHECK(v[1] == int16_t(1));
        CHECK(v[2] == (std::numeric_limits<int16_t>::max)());

        auto w = cbor::decode_cbor<std::vector<int64_t>>(input);
        REQUIRE(w.size() == 3);
        CHECK(w[0] == std::numeric_limits<int16_t>::lowest());
        CHECK(w[1] == int16_t(1));
        CHECK(w[2] == (std::numeric_limits<int16_t>::max)());

        std::vector<uint8_t> buf;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, buf, options);
        check_native(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::little>(),
            input, buf);
    }
    SECTION("Tags 78 (int32, little endian)")
    {
        //std::cout << "CBOR typed array Tags 78 (int32, little endian)" << '\n';

        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x4e, // Tag 78, sint32, little endian, Typed Array
            0x4c, // Byte string value of length 12
                0x00,0x00,0x00,0x80,
                0x01,0x00,0x00,0x00,
                0xff,0xff,0xff,0x7f
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 78\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 3);
        CHECK(j[0].as<int32_t>() == std::numeric_limits<int32_t>::lowest());
        CHECK(j[1].as<int32_t>() == int32_t(1));
        CHECK(j[2].as<int32_t>() == (std::numeric_limits<int32_t>::max)());

        auto u = cbor::decode_cbor<std::vector<int32_t>>(input);
        REQUIRE(u.size() == 3);
        CHECK(u[0] == std::numeric_limits<int32_t>::lowest());
        CHECK(u[1] == int32_t(1));
        CHECK(u[2] == (std::numeric_limits<int32_t>::max)());

        auto v = cbor::decode_cbor<std::vector<int64_t>>(input);
        REQUIRE(v.size() == 3);
        CHECK(v[0] == std::numeric_limits<int32_t>::lowest());
        CHECK(v[1] == int32_t(1));
        CHECK(v[2] == (std::numeric_limits<int32_t>::max)());

        std::vector<uint8_t> buf;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, buf, options);
        check_native(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::little>(),
            input, buf);
    }

    SECTION("Tag 79 (int64,little endian)")
    {
        //std::cout << "CBOR typed array Tag 79 (int64,little endian)" << '\n';

        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x4f, // Tag 79, sint64, little endian, Typed Array
            0x58,0x18, // Byte string value of length 24
                0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x80,
                0x1,0x0,0x0,0x0,0x0,0x0,0x0,0x0,
                0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x7f
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 79\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 3);
        CHECK(j[0].as<int64_t>() == std::numeric_limits<int64_t>::lowest());
        CHECK(j[1].as<int64_t>() == int64_t(1));
        CHECK(j[2].as<int64_t>() == (std::numeric_limits<int64_t>::max)());

        auto u = cbor::decode_cbor<std::vector<int64_t>>(input);
        REQUIRE(u.size() == 3);
        CHECK(u[0] == std::numeric_limits<int64_t>::lowest());
        CHECK(u[1] == int64_t(1));
        CHECK(u[2] == (std::numeric_limits<int64_t>::max)());

        std::vector<uint8_t> buf;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, buf, options);
        check_native(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::little>(),
            input, buf);
    }

    SECTION("Tag 80, float16, big endian")
    {
        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x50, // Tag 80, float16, big endian, Typed Array
            0x48, // Byte string value of length 8
                0x00,0x01,
                0x03,0xff,
                0x04,0x00,
                0x7b,0xff
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 80\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 4);
        CHECK(j[0].as<float>() == Approx(0.000000059605).epsilon(0.00001));
        CHECK(j[1].as<float>() == Approx(0.000060976).epsilon(0.00001));
        CHECK(j[2].as<float>() == Approx(0.000061035).epsilon(0.00001));
        CHECK(j[3].as<float>() == 65504);
    }

    SECTION("Tag 81, float32, big endian")
    {
        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x51, // Tag 81, float32, big endian, Typed Array
            0x48, // Byte string value of length 8
                0xff,0x7f,0xff,0xff,
                0x7f,0x7f,0xff,0xff
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 81\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 2);
        CHECK(j[0].as<float>() == std::numeric_limits<float>::lowest());
        CHECK(j[1].as<float>() == (std::numeric_limits<float>::max)());

        auto u = cbor::decode_cbor<std::vector<float>>(input);

        std::vector<uint8_t> buf;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, buf, options);
        check_native(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>(),
            input, buf);
    }

    SECTION("Tag 82, float64, big endian")
    {
        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x52, // Tag 82, float64, big endian, Typed Array
            0x50, // Byte string value of length 16
                0xff, 0xef, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0x7f, 0xef, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff
        };

        json j = cbor::decode_cbor<json>(input);
        REQUIRE(j.is_array());
        //std::cout << pretty_print(j) << "\n";

        auto u = cbor::decode_cbor<std::vector<double>>(input);

        std::vector<uint8_t> buf;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, buf, options);
        check_native(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>(),
            input, buf);
    }

    SECTION("Tag 83, float128, big endian")
    {
        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x53, // Tag 83, float128, big endian, Typed Array
            0x58,0x40, // Byte string value of length 64
                0xff,0xfe,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,
                0x7f,0xfe,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,
                0xbf,0xff,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
                0x3f,0xff,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00
        };

        //json j = cbor::decode_cbor<json>(input);
        //REQUIRE(j.is_array());
        //REQUIRE(j.size() == 2);

        //CHECK(j[0].as<double>() == std::numeric_limits<double>::lowest());
        //CHECK(j[1].as<double>() == (std::numeric_limits<double>::max)());
    }

    SECTION("Tag 84, float16, little endian")
    {
        //std::cout << "CBOR typed array Tag 84, float16, little endian" << '\n';

        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x54, // Tag 84, float16, little endian, Typed Array
            0x48, // Byte string value of length 8
                0x01,0x00,
                0xff,0x03,
                0x00,0x04,
                0xff,0x7b
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 84\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 4);
        CHECK(j[0].as<float>() == Approx(0.000000059605).epsilon(0.00001));
        CHECK(j[1].as<float>() == Approx(0.000060976).epsilon(0.00001));
        CHECK(j[2].as<float>() == Approx(0.000061035).epsilon(0.00001));
        CHECK(j[3].as<float>() == 65504);
    }
    SECTION("Tag 85, float32, little endian")
    {
        //std::cout << "CBOR typed array Tag 85, float32, little endian" << '\n';

        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x55, // Tag 85, float64, little endian, Typed Array
            0x48, // Byte string value of length 8
                0xff,0xff,0x7f,0xff,
                0xff,0xff,0x7f,0x7f 
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 85\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 2);
        CHECK(j[0].as<float>() == std::numeric_limits<float>::lowest());
        CHECK(j[1].as<float>() == (std::numeric_limits<float>::max)());

        auto u = cbor::decode_cbor<std::vector<float>>(input);
        REQUIRE(u.size() == 2);
        CHECK(u[0] == std::numeric_limits<float>::lowest());
        CHECK(u[1] == (std::numeric_limits<float>::max)());

        std::vector<uint8_t> buf;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, buf, options);
        check_native(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::little>(),
            input, buf);
    }
    SECTION("Tag 86, float64, little endian")
    {
        //std::cout << "CBOR typed array Tag 86, float64, little endian" << '\n';

        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x56, // Tag 86, float64, little endian, Typed Array
            0x50, // Byte string value of length 16
                0xff,0xff,0xff,0xff,0xff,0xff,0xef,0xff,
                0xff,0xff,0xff,0xff,0xff,0xff,0xef,0x7f
        };

        json j = cbor::decode_cbor<json>(input);
        //std::cout << "Tag 86\n" << pretty_print(j) << "\n";
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 2);
        CHECK(j[0].as<double>() == std::numeric_limits<double>::lowest());
        CHECK(j[1].as<double>() == (std::numeric_limits<double>::max)());

        auto u = cbor::decode_cbor<std::vector<double>>(input);
        REQUIRE(u.size() == 2);
        CHECK(u[0] == std::numeric_limits<double>::lowest());
        CHECK(u[1] == (std::numeric_limits<double>::max)());

        std::vector<uint8_t> buf;
        auto options = cbor::cbor_options{}
            .use_typed_arrays(true);
        cbor::encode_cbor(u, buf, options);
        check_native(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::little>(),
            input, buf);
    }

    SECTION("Tag 87, float128, little endian")
    {
        //std::cout << "CBOR typed array Tag 87, float128, little endian" << '\n';

        const std::vector<uint8_t> input = {
            0xd8, // Tag
                0x57, // Tag 87, float128, little endian, Typed Array
            0x58,0x40, // Byte string value of length 64
                0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xfe,0xff,
                0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xfe,0x7f,
                0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0xff,0xbf,
                0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0xff,0x3f 
        };

        //json j = cbor::decode_cbor<json>(input);
        //REQUIRE(j.is_array());
        //REQUIRE(j.size() == 2);
    }
} 

