// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_reader.hpp>
#include <catch/catch.hpp>

using namespace jsoncons; 

TEST_CASE("test json_reader buffered read")
{
    SECTION("string with split buffer")
    {
        std::string str(stream_source<char>::default_max_buffer_size+10, '1');
        for (std::size_t i = 0; i < str.size(); i+= 2)
        {
            str[i] = '0';
        }

        std::string input;
        input.push_back('"');
        input.append(str);
        input.push_back('"');
        std::stringstream is(input);

        auto j = json::parse(is);
        REQUIRE(j.is_string());
        CHECK(j.as<std::string>() == str);
    }

    SECTION("number with split buffer")
    {
        std::string str(stream_source<char>::default_max_buffer_size-7, 'a');
        std::string neg_num("-123456789.123456789");

        std::string input;
        input.push_back('[');
        input.push_back('"');
        input.append(str);
        input.push_back('"');
        input.push_back(',');
        input.append(neg_num);
        input.push_back(']');

        std::stringstream is(input);

        auto j = json::parse(is);

        REQUIRE(j.is_array());
        REQUIRE(j.size() == 2);
        CHECK(j[1].as<double>() == -123456789.123456789);
    }

    SECTION("false with split buffer")
    {
        std::string str;
        str.push_back('[');
        str.push_back('"');
        str.append(stream_source<char>::default_max_buffer_size-8, 'a');
        str.push_back('"');
        str.push_back(',');
        str.append("false");
        str.push_back(']');

        std::stringstream is(str);

        auto j = json::parse(is);
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 2);
        CHECK_FALSE(j[1].as<bool>());
    }

    SECTION("true with split buffer")
    {
        std::string str;
        str.push_back('[');
        str.push_back('"');
        str.append(stream_source<char>::default_max_buffer_size - 6, 'a');
        str.push_back('"');
        str.push_back(',');
        str.append("true");
        str.push_back(']');

        std::stringstream is(str);

        auto j = json::parse(is);
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 2);
        CHECK(j[1].as<bool>());
    }

    SECTION("null with split buffer")
    {
        std::string str;
        str.push_back('[');
        str.push_back('"');
        str.append(stream_source<char>::default_max_buffer_size - 5, 'a');
        str.push_back('"');
        str.push_back(',');
        str.append("null");
        str.push_back(']');

        std::stringstream is(str);

        auto j = json::parse(is);
        REQUIRE(j.is_array());
        REQUIRE(j.size() == 2);
        CHECK(j[1].is_null());
    }
}

void test_json_reader_error(const std::string& text, const std::error_code& ec)
{
    REQUIRE_THROWS(json::parse(text));
    JSONCONS_TRY
    {
        json::parse(text);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        if (e.code() != ec)
        {
            std::cout << text << '\n';
            std::cout << e.code().value() << " " << e.what() << '\n'; 
        }
        CHECK(ec == e.code());
    }
}

void test_json_reader_ec(const std::string& text, const std::error_code& expected)
{
    std::error_code ec;

    std::istringstream is(text);
    json_decoder<json> decoder;
    json_stream_reader reader(is,decoder);

    reader.read(ec);
    //std::cerr << text << '\n';
    //std::cerr << ec.message() 
    //          << " at line " << reader.line() 
    //          << " and column " << reader.column() << '\n';

    CHECK(ec);
    CHECK(expected == ec);
}

TEST_CASE("test_missing_separator")
{
    std::string jtext = R"({"field1"{}})";    

    test_json_reader_error(jtext, jsoncons::json_errc::expected_colon);
    test_json_reader_ec(jtext, jsoncons::json_errc::expected_colon);
}

TEST_CASE("test_read_invalid_value")
{
    std::string jtext = R"({"field1":ru})";    

    test_json_reader_error(jtext,jsoncons::json_errc::expected_value);
    test_json_reader_ec(jtext, jsoncons::json_errc::expected_value);
}

TEST_CASE("test_read_unexpected_end_of_file")
{
    std::string jtext = R"({"field1":{})";    

    test_json_reader_error(jtext, jsoncons::json_errc::unexpected_eof);
    test_json_reader_ec(jtext, jsoncons::json_errc::unexpected_eof);
}


TEST_CASE("test_read_value_not_found")
{
    std::string jtext = R"({"name":})";    

    test_json_reader_error(jtext, jsoncons::json_errc::expected_value);
    test_json_reader_ec(jtext, jsoncons::json_errc::expected_value);
}

TEST_CASE("test_read_escaped_characters")
{
    std::string input("[\"\\n\\b\\f\\r\\t\"]");
    std::string expected("\n\b\f\r\t");

    json o = json::parse(input);
    CHECK(expected == o[0].as<std::string>());
}


TEST_CASE("test_read_expected_colon")
{
    test_json_reader_error("{\"name\" 10}", jsoncons::json_errc::expected_colon);
    test_json_reader_error("{\"name\" true}", jsoncons::json_errc::expected_colon);
    test_json_reader_error("{\"name\" false}", jsoncons::json_errc::expected_colon);
    test_json_reader_error("{\"name\" null}", jsoncons::json_errc::expected_colon);
    test_json_reader_error("{\"name\" \"value\"}", jsoncons::json_errc::expected_colon);
    test_json_reader_error("{\"name\" {}}", jsoncons::json_errc::expected_colon);
    test_json_reader_error("{\"name\" []}", jsoncons::json_errc::expected_colon);
}

TEST_CASE("test_read_expected_key")
{
    test_json_reader_error("{10}", jsoncons::json_errc::expected_key);
    test_json_reader_error("{true}", jsoncons::json_errc::expected_key);
    test_json_reader_error("{false}", jsoncons::json_errc::expected_key);
    test_json_reader_error("{null}", jsoncons::json_errc::expected_key);
    test_json_reader_error("{{}}", jsoncons::json_errc::expected_key);
    test_json_reader_error("{[]}", jsoncons::json_errc::expected_key);
}

TEST_CASE("test_read_expected_value")
{
    test_json_reader_error("[tru]", jsoncons::json_errc::invalid_value);
    test_json_reader_error("[fa]", jsoncons::json_errc::invalid_value);
    test_json_reader_error("[n]", jsoncons::json_errc::invalid_value);
}

TEST_CASE("test_read_primitive_pass")
{
    json val;
    CHECK_NOTHROW((val=json::parse("null")));
    CHECK(val == json::null());
    CHECK_NOTHROW((val=json::parse("false")));
    CHECK(val == json(false));
    CHECK_NOTHROW((val=json::parse("true")));
    CHECK(val == json(true));
    CHECK_NOTHROW((val=json::parse("10")));
    CHECK(val == json(10));
    CHECK_NOTHROW((val=json::parse("1.999")));
    CHECK(val == json(1.999));
    CHECK_NOTHROW((val=json::parse("\"string\"")));
    CHECK(val == json("string"));
}

TEST_CASE("test_read_empty_structures")
{
    json val;
    CHECK_NOTHROW((val=json::parse("{}")));
    CHECK_NOTHROW((val=json::parse("[]")));
    CHECK_NOTHROW((val=json::parse("{\"object\":{},\"array\":[]}")));
    CHECK_NOTHROW((val=json::parse("[[],{}]")));
}

TEST_CASE("test_read_primitive_fail")
{
    test_json_reader_error("null {}", jsoncons::json_errc::extra_character);
    test_json_reader_error("n ", jsoncons::json_errc::invalid_value);
    test_json_reader_error("nu ", jsoncons::json_errc::invalid_value);
    test_json_reader_error("nul ", jsoncons::json_errc::invalid_value);
    test_json_reader_error("false {}", jsoncons::json_errc::extra_character);
    test_json_reader_error("fals ", jsoncons::json_errc::invalid_value);
    test_json_reader_error("true []", jsoncons::json_errc::extra_character);
    test_json_reader_error("tru ", jsoncons::json_errc::invalid_value);
    test_json_reader_error("10 {}", jsoncons::json_errc::extra_character);
    test_json_reader_error("1a ", jsoncons::json_errc::invalid_number);
    test_json_reader_error("1.999 []", jsoncons::json_errc::extra_character);
    test_json_reader_error("1e0-1", jsoncons::json_errc::invalid_number);
    test_json_reader_error("\"string\"{}", jsoncons::json_errc::extra_character);
    test_json_reader_error("\"string\"[]", jsoncons::json_errc::extra_character);
}

TEST_CASE("test_read_multiple")
{
    std::string in="{\"a\":1,\"b\":2,\"c\":3}{\"a\":4,\"b\":5,\"c\":6}";
    //std::cout << in << '\n';

    std::istringstream is(in);

    jsoncons::json_decoder<json> decoder;
    json_stream_reader reader(is,decoder);

    REQUIRE_FALSE(reader.eof());
    reader.read_next();
    json val = decoder.get_result();
    CHECK(1 == val["a"].as<int>());
    REQUIRE_FALSE(reader.eof());
    reader.read_next();
    json val2 = decoder.get_result();
    CHECK(4 == val2["a"].as<int>());
    CHECK(reader.eof());
}

TEST_CASE("json_reader read from string test")
{
    std::string s = R"(
{
  "store": {
    "book": [
      {
        "category": "reference",
        "author": "Margaret Weis",
        "title": "Dragonlance Series",
        "price": 31.96
      },
      {
        "category": "reference",
        "author": "Brent Weeks",
        "title": "Night Angel Trilogy",
        "price": 14.70
      }
    ]
  }
}
)";

    json_decoder<json> decoder;
    json_string_reader reader(s, decoder);
    reader.read();
    json j = decoder.get_result();

    REQUIRE(j.is_object());
    REQUIRE(j.size() == 1);
    REQUIRE(j[0].is_object());
    REQUIRE(j[0].size() == 1);
    REQUIRE(j[0][0].is_array());
    REQUIRE(j[0][0].size() == 2);
    CHECK(j[0][0][0]["category"].as<std::string>() == std::string("reference"));
    CHECK(j[0][0][1]["author"].as<std::string>() == std::string("Brent Weeks"));
}

TEST_CASE("json_reader json lines")
{
    SECTION("json lines")
    {
        std::string data = R"(
    ["Name", "Session", "Score", "Completed"]
    ["Gilbert", "2013", 24, true]
    ["Alexa", "2013", 29, true]
    ["May", "2012B", 14, false]
    ["Deloise", "2012A", 19, true] 
        )";

        std::stringstream is(data);
        json_decoder<json> decoder;
        json_stream_reader reader(is, decoder);

        REQUIRE(!reader.eof());
        reader.read_next();
        CHECK(decoder.get_result() == json::parse(R"(["Name", "Session", "Score", "Completed"])"));
        REQUIRE(!reader.eof());
        reader.read_next();
        REQUIRE(!reader.eof());
        reader.read_next();
        REQUIRE(!reader.eof());
        reader.read_next();
        REQUIRE(!reader.eof());
        reader.read_next();
        CHECK(decoder.get_result() == json::parse(R"(["Deloise", "2012A", 19, true])"));
        CHECK(reader.eof());
    }
}

#if defined(JSONCONS_HAS_STATEFUL_ALLOCATOR) && JSONCONS_HAS_STATEFUL_ALLOCATOR == 1

#include <scoped_allocator>
#include <common/free_list_allocator.hpp>

template <typename T>
using MyScopedAllocator = std::scoped_allocator_adaptor<free_list_allocator<T>>;

TEST_CASE("json_reader stateful allocator tests")
{
    std::string input = R"(
{
  "store": {
    "book": [
      {
        "category": "reference",
        "author": "Margaret Weis",
        "title": "Dragonlance Series",
        "price": 31.96
      },
      {
        "category": "reference",
        "author": "Brent Weeks",
        "title": "Night Angel Trilogy",
        "price": 14.70
      }
    ]
  }
}
)";

    SECTION("stateful allocator")
    {
        using cust_json = basic_json<char,sorted_policy,MyScopedAllocator<char>>;

        MyScopedAllocator<char> my_allocator{1}; 

        json_decoder<cust_json,MyScopedAllocator<char>> decoder(my_allocator,
                                                              my_allocator);
        basic_json_reader<char,string_source<char>,MyScopedAllocator<char>> reader(input, decoder, my_allocator);
        reader.read();

        cust_json j = decoder.get_result();
        //std::cout << pretty_print(j) << "\n";
    }
}
#endif

