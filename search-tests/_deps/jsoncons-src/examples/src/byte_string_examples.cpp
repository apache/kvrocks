// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>

using namespace jsoncons; // For brevity

void construct_json_byte_string()
{
    std::vector<uint8_t> bytes = {'H','e','l','l','o'};

    // default suggested encoding (base64url)
    json j1(byte_string_arg, bytes);
    std::cout << "(1) "<< j1 << "\n\n";

    // base64 suggested encoding
    json j2(byte_string_arg, bytes, semantic_tag::base64);
    std::cout << "(2) "<< j2 << "\n\n";

    // base16 suggested encoding
    json j3(byte_string_arg, bytes, semantic_tag::base16);
    std::cout << "(3) "<< j3 << "\n\n";
}

void retrieve_json_value_as_byte_string()
{
    json j;
    j["ByteString"] = json(byte_string_arg, std::vector<uint8_t>{ 'H','e','l','l','o' });
    j["EncodedByteString"] = json("SGVsbG8=", semantic_tag::base64);

    std::cout << "(1)\n";
    std::cout << pretty_print(j) << "\n\n";

    // Retrieve a byte string as a std::vector<uint8_t>
    std::vector<uint8_t> v = j["ByteString"].as<std::vector<uint8_t>>();

    // Retrieve a byte string from a text string containing base64 character values
    byte_string bytes2 = j["EncodedByteString"].as<byte_string>();
    std::cout << "(2) " << bytes2 << "\n\n";

    // Retrieve a byte string view  to access the memory that's holding the byte string
    byte_string_view bsv3 = j["ByteString"].as<byte_string_view>();
    std::cout << "(3) " << bsv3 << "\n\n";

    // Can't retrieve a byte string view of a text string 
    try
    {
        byte_string_view bsv4 = j["EncodedByteString"].as<byte_string_view>();
    }
    catch (const std::exception& e)
    {
        std::cout << "(4) "<< e.what() << "\n\n";
    }
}

void serialize_json_byte_string()
{
    std::vector<uint8_t> bytes = {'H','e','l','l','o'};

    json j(byte_string_arg, bytes);

    // default
    std::cout << "(1) "<< j << "\n\n";

    // base16
    auto options2 = json_options{}
        .byte_string_format(byte_string_chars_format::base16);
    std::cout << "(2) "<< print(j, options2) << "\n\n";

    // base64
    auto options3 = json_options{}
        .byte_string_format(byte_string_chars_format::base64);
    std::cout << "(3) "<< print(j, options3) << "\n\n";

    // base64url
    auto options4 = json_options{}
        .byte_string_format(byte_string_chars_format::base64url);
    std::cout << "(4) "<< print(j, options4) << "\n\n";
}

int main()
{
    std::cout << "byte_string examples" << "\n\n";
    construct_json_byte_string();
    serialize_json_byte_string();
    retrieve_json_value_as_byte_string();
}


