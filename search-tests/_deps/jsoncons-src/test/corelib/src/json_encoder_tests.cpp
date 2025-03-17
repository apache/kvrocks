// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>

using namespace jsoncons;

TEST_CASE("test_byte_string_serialization")
{
    std::vector<uint8_t> bytes = {'H','e','l','l','o'};
    json j(byte_string_arg, bytes);

    std::ostringstream os;
    os << j;

    std::string expected; 
    expected.push_back('\"');
    encode_base64url(bytes.begin(),bytes.end(),expected);
    expected.push_back('\"');

    //std::cout << expected << " " << os.str() << '\n';

    CHECK(expected == os.str());
}

struct json_string_encoder_reset_test_fixture
{
    std::string output1;
    std::string output2;
    json_string_encoder encoder;

    json_string_encoder_reset_test_fixture()
        : encoder(output1,
                  json_options().indent_size(0).new_line_chars("")
                                .spaces_around_comma(spaces_option::no_spaces))
    {}

    std::string string1() const {return output1;}
    std::string string2() const {return output2;}
};

struct json_stream_encoder_reset_test_fixture
{
    std::ostringstream output1;
    std::ostringstream output2;
    json_stream_encoder encoder;

    json_stream_encoder_reset_test_fixture()
        : encoder(output1,
                  json_options().indent_size(0).new_line_chars("")
                      .spaces_around_comma(spaces_option::no_spaces))
    {}

    std::string string1() const {return output1.str();}
    std::string string2() const {return output2.str();}
};

struct compact_json_string_encoder_reset_test_fixture
{
    std::string output1;
    std::string output2;
    compact_json_string_encoder encoder;

    compact_json_string_encoder_reset_test_fixture() : encoder(output1) {}
    std::string string1() const {return output1;}
    std::string string2() const {return output2;}
};

struct compact_json_stream_encoder_reset_test_fixture
{
    std::ostringstream output1;
    std::ostringstream output2;
    compact_json_stream_encoder encoder;

    compact_json_stream_encoder_reset_test_fixture() : encoder(output1) {}
    std::string string1() const {return output1.str();}
    std::string string2() const {return output2.str();}
};

TEMPLATE_TEST_CASE("test_json_encoder_reset", "",
                   json_string_encoder_reset_test_fixture,
                   json_stream_encoder_reset_test_fixture,
                   compact_json_string_encoder_reset_test_fixture,
                   compact_json_stream_encoder_reset_test_fixture)
{
    using fixture_type = TestType;
    fixture_type f;

    // Parially encode, reset, then fully encode to same sink
    f.encoder.begin_array();
    f.encoder.string_value("foo");
    f.encoder.uint64_value(42);
    f.encoder.flush();
    CHECK(f.string1() == R"(["foo",42)");
    f.encoder.reset();
    f.encoder.begin_array();
    f.encoder.string_value("foo");
    f.encoder.uint64_value(42);
    f.encoder.end_array();
    f.encoder.flush();
    CHECK(f.string1() == R"(["foo",42["foo",42])");

    // Reset and encode to different sink
    f.encoder.reset(f.output2);
    f.encoder.begin_array();
    f.encoder.string_value("foo");
    f.encoder.uint64_value(42);
    f.encoder.end_array();
    f.encoder.flush();
    CHECK(f.string2() == R"(["foo",42])");
}
