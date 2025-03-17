// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

#include <jsoncons/json.hpp>

#include "sample_types.hpp"
#include <string>
#include <iomanip>
#include <cassert>

using namespace jsoncons;

void decode_float64_big_endian_array()
{
    const std::vector<uint8_t> input = {
      0xd8,0x52, // Tag 82 (float64 big endian Typed Array)
        0x50,    // Byte string value of length 16
            0xff, 0xef, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0x7f, 0xef, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff
    };

    auto j = cbor::decode_cbor<json>(input);
    std::cout << "(1)\n" << pretty_print(j) << "\n\n";

    auto v = cbor::decode_cbor<std::vector<double>>(input);
    std::cout << "(2)\n";
    for (auto item : v)
    {
        std::cout << std::defaultfloat << item << "\n";
    }
    std::cout << "\n";

    std::vector<uint8_t> output1;
    cbor::encode_cbor(v, output1);

    // output1 contains a classical CBOR array
    std::cout << "(3)\n" << byte_string_view(output1) << "\n\n";

    std::vector<uint8_t> output2;
    auto options = cbor::cbor_options{}
        .use_typed_arrays(true);
    cbor::encode_cbor(v, output2, options);

    // output2 contains a float64, native endian, Typed Array 
    std::cout << "(4)\n" << byte_string_view(output2) << "\n\n";
}

void decode_mult_dim_row_major()
{
    const std::vector<uint8_t> input = {
      0xd8,0x28,     // Tag 40 (multi-dimensional row major array)
        0x82,        // array(2)
          0x82,      // array(2)
            0x02,    // unsigned(2) 1st Dimension
            0x03,    // unsigned(3) 2nd Dimension
          0xd8,0x41,     // Tag 65 (uint16 big endian Typed Array)
            0x4c,        // byte string(12)
              0x00,0x02, // unsigned(2)
              0x00,0x04, // unsigned(4)
              0x00,0x08, // unsigned(8)
              0x00,0x04, // unsigned(4)
              0x00,0x10, // unsigned(16)
              0x01,0x00  // unsigned(256)
    };

    json j = cbor::decode_cbor<json>(input);

    std::cout << j.tag() << "\n";
    std::cout << pretty_print(j) << "\n";
}

void encode_decode_large_typed_array()
{
    std::ios_base::fmtflags f( std::cout.flags() );

    std::vector<float> x(15000000);
    for (std::size_t i = 0; i < x.size(); ++i)
    {
        x[i] = static_cast<float>(i);
    }
    auto options = cbor::cbor_options{}
        .use_typed_arrays(true);

    std::vector<uint8_t> buf;
    cbor::encode_cbor(x, buf, options);

    std::cout << "first 19 bytes:\n\n";
    std::cout << byte_string_view(buf).substr(0, 19) << "\n\n";
/*
    0xd8,0x55 -- Tag 85 (float32 little endian Typed Array)
    0x5a - byte string (four-byte uint32_t for n, and then  n bytes follow)
      03 93 87 00 -- 60000000
        00 00 00 00 -- 0.0f
        00 00 80 3f -- 1.0f
        00 00 00 40 -- 2.0f
*/
    auto y = cbor::decode_cbor<std::vector<float>>(buf);

    assert(y == x);

    std::cout.flags( f );
}

void encode_mult_dim_array()
{
    std::vector<uint8_t> v;

    cbor::cbor_bytes_encoder encoder(v);
    std::vector<std::size_t> shape = { 2,3 };
    encoder.begin_multi_dim(shape, semantic_tag::multi_dim_column_major);
    encoder.begin_array(6);
    encoder.uint64_value(2);
    encoder.uint64_value(4);
    encoder.uint64_value(8);
    encoder.uint64_value(4);
    encoder.uint64_value(16);
    encoder.uint64_value(256);
    encoder.end_array();
    encoder.end_multi_dim();

    std::cout << "(1)\n" << byte_string_view(v) << "\n\n";

    auto j = cbor::decode_cbor<json>(v);
    std::cout << "(2) " << j.tag() << "\n";
    std::cout << pretty_print(j) << "\n\n";
}

void encode_half_array()
{
    std::ios_base::fmtflags f( std::cout.flags() );

    std::vector<uint8_t> buffer;

    auto options = cbor::cbor_options{}
        .use_typed_arrays(true);
    cbor::cbor_bytes_encoder encoder(buffer, options);

    std::vector<uint16_t> values = {0x3bff,0x3c00,0x3c01,0x3555};
    encoder.typed_array(half_arg, values);

    // buffer contains a half precision floating-point, native endian, Typed Array 
    std::cout << "(1)\n" << byte_string_view(buffer) << "\n\n";

    auto j = cbor::decode_cbor<json>(buffer);

    std::cout << "(2)\n";
    for (auto item : j.array_range())
    {
        std::cout << std::boolalpha << item.is_half() 
                  << " " << std::hex << (int)item.as<uint16_t>() 
                  << " " << std::defaultfloat << item.as<double>() << "\n";
    }
    std::cout << "\n";

    std::cout << "(3)\n" << pretty_print(j) << "\n\n";

    std::cout.flags( f );
}

void cursor_example_multi_dim_row_major_typed_array()
{
    const std::vector<uint8_t> input = {
      0xd8,0x28,  // Tag 40 (multi-dimensional row major array)
        0x82,     // array(2)
          0x82,   // array(2)
            0x02,    // unsigned(2) 1st Dimension
            0x03,    // unsigned(3) 2nd Dimension
        0xd8,0x41,     // Tag 65 (uint16 big endian Typed Array)
          0x4c,        // byte string(12)
            0x00,0x02, // unsigned(2)
            0x00,0x04, // unsigned(4)
            0x00,0x08, // unsigned(8)
            0x00,0x04, // unsigned(4)
            0x00,0x10, // unsigned(16)
            0x01,0x00  // unsigned(256)
    };

    cbor::cbor_bytes_cursor cursor(input);
    for (; !cursor.done(); cursor.next())
    {
        const auto& event = cursor.current();
        switch (event.event_type())
        {
            case staj_event_type::begin_array:
                std::cout << event.event_type() 
                          << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::end_array:
                std::cout << event.event_type() 
                          << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::uint64_value:
                std::cout << event.event_type() 
                          << ": " << event.get<uint64_t>() << " " << "(" << event.tag() << ")\n";
                break;
            default:
                std::cout << "Unhandled event type " << event.event_type() 
                          << " " << "(" << event.tag() << ")\n";
                break;
        }
    }
}

void cursor_example_multi_dim_column_major_classical_cbor_array()
{
    const std::vector<uint8_t> input = {
      0xd9,0x04,0x10,  // Tag 1040 (multi-dimensional column major array)
        0x82,     // array(2)
          0x82,   // array(2)
            0x02,    // unsigned(2) 1st Dimension
            0x03,    // unsigned(3) 2nd Dimension
          0x86,   // array(6)
            0x02,           // unsigned(2)   
            0x04,           // unsigned(4)   
            0x08,           // unsigned(8)   
            0x04,           // unsigned(4)   
            0x10,           // unsigned(16)  
            0x19,0x01,0x00  // unsigned(256) 
    };

    cbor::cbor_bytes_cursor cursor(input);
    for (; !cursor.done(); cursor.next())
    {
        const auto& event = cursor.current();
        switch (event.event_type())
        {
            case staj_event_type::begin_array:
                std::cout << event.event_type() 
                          << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::end_array:
                std::cout << event.event_type() 
                          << " " << "(" << event.tag() << ")\n";
                break;
            case staj_event_type::uint64_value:
                std::cout << event.event_type() 
                          << ": " << event.get<uint64_t>() << " " << "(" << event.tag() << ")\n";
                break;
            default:
                std::cout << "Unhandled event type " << event.event_type() 
                          << " " << "(" << event.tag() << ")\n";
                break;
        }
    }
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
        v = std::vector<double>(data.begin(),data.end());
        return false;
    }
};

void read_to_cbor_visitor()
{
    std::vector<double> v{10.0,20.0,30.0,40.0};

    std::vector<uint8_t> buffer;
    auto options = cbor::cbor_options{}
        .use_typed_arrays(true);
    cbor::encode_cbor(v, buffer, options);

    std::cout << "(1)\n";
    std::cout << byte_string_view(buffer) << "\n\n";
/*
    0xd8, // Tag
        0x56, // Tag 86, float64, little endian, Typed Array
    0x58,0x20, // Byte string value of length 32 
        0x00,0x00,0x00,0x00,0x00,0x00,0x24,0x40,
        0x00,0x00,0x00,0x00,0x00,0x00,0x34,0x40, 
        0x00,0x00,0x00,0x00,0x00,0x00,0x3e,0x40, 
        0x00,0x00,0x00,0x00,0x00,0x00,0x44,0x40
*/

    cbor::cbor_bytes_cursor cursor(buffer);
    assert(cursor.current().event_type() == staj_event_type::begin_array);
    assert(cursor.is_typed_array());

    my_cbor_visitor visitor;
    cursor.read_to(visitor);
    std::cout << "(2)\n";
    for (auto item : visitor.v)
    {
        std::cout << item << "\n";
    }
    std::cout << "\n";
}

int main()
{
    std::cout << "\ncbor typed array examples\n\n";
    decode_float64_big_endian_array();
    decode_mult_dim_row_major();
    encode_mult_dim_array();
    encode_half_array();
    cursor_example_multi_dim_row_major_typed_array();
    cursor_example_multi_dim_column_major_classical_cbor_array();
    read_to_cbor_visitor();
    encode_decode_large_typed_array();

    std::cout << "\n\n";
}

