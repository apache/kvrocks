// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <jsoncons/json_reader.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <catch/catch.hpp>

using namespace jsoncons;

struct jsonpath_filter_fixture
{
};

class lenient_error_handler
{
    std::error_code value_;
public:
    lenient_error_handler(std::error_code value)
        : value_(value)
    {
    }

    bool operator()(const std::error_code& ec, const ser_context&) noexcept
    {
        return ec == value_; // if returns true, use default processing
    }
};
#if 0
TEST_CASE("test_parse_small_string1")
{
    std::string input = "\"String\"";

    json_decoder<json> decoder;
    JSONCONS_TRY
    {
        json_string_reader reader(input,decoder);
        reader.read_next();
    }
    JSONCONS_CATCH (const std::exception&)
    {
    }
    CHECK(decoder.is_valid());
}

TEST_CASE("test_parse_small_string2")
{
    std::string input = "\"Str\\\"ing\"";

    json_decoder<json> decoder;
    JSONCONS_TRY
    {
        json_string_reader reader(input, decoder);
        reader.read_next();
    }
    JSONCONS_CATCH (const std::exception&)
    {
    }
    CHECK(decoder.is_valid());
}

TEST_CASE("test_parse_small_string4")
{
    std::string input = "\"Str\\\"ing\"";

    for (std::size_t i = 4; i < input.length(); ++i)
    {
        std::istringstream is(input);
        json_decoder<json> decoder;
        JSONCONS_TRY
        {
            json_stream_reader reader(stream_source<char>(is,i), decoder);
            reader.read_next();
        }
        JSONCONS_CATCH (const std::exception&)
        {
        }
        CHECK(decoder.is_valid());
        CHECK(std::string("Str\"ing") == decoder.get_result().as<std::string>());
    }
}
#endif
TEST_CASE("test_parse_big_string1")
{
    std::string input = "\"Big Str\\\"ing\"";

    for (std::size_t i = 4; i < input.length(); ++i)
    {
        std::istringstream is(input);
        json_decoder<json> decoder;
        JSONCONS_TRY
        {
            json_stream_reader reader(stream_source<char>(is,i), decoder);
            reader.read_next();
        }
        JSONCONS_CATCH (const std::exception&)
        {
        }
        CHECK(decoder.is_valid());
        CHECK(std::string("Big Str\"ing") == decoder.get_result().as<std::string>());
    }
}
#if 0
TEST_CASE("test_parse_big_string2")
{
    std::string input = "\"Big\t Str\\\"ing\"";

    std::istringstream is(input);
    json_decoder<json> decoder;
    lenient_error_handler err_handler(json_errc::illegal_character_in_string);
    JSONCONS_TRY
    {
        json_stream_reader reader(is, decoder, err_handler);
        reader.read_next();
    }
    JSONCONS_CATCH (const std::exception&)
    {
    }
    CHECK(decoder.is_valid());
    CHECK(std::string("Big\t Str\"ing") == decoder.get_result().as<std::string>());
}
#endif

