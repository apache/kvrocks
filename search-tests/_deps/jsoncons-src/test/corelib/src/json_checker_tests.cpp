// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <ctime>
#include <fstream>
#include <sstream>
#include <utility>
#include <vector>

#include <jsoncons/json.hpp>
#include <jsoncons/json_decoder.hpp>
#include <jsoncons/json_reader.hpp>

#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("test_fail1")
{
    std::string path = "./corelib/input/JSON_checker/fail1.json";
    std::fstream is(path);
    REQUIRE(is);
    CHECK_NOTHROW(json::parse(is));
}

TEST_CASE("test_fail2")
{
    std::string in_file = "./corelib/input/JSON_checker/fail2.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
    }
    CHECK(err == jsoncons::json_errc::unexpected_eof);
}

TEST_CASE("test_fail3")
{
    std::string in_file = "./corelib/input/JSON_checker/fail3.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
    }
    CHECK(err == jsoncons::json_errc::expected_key);
}

TEST_CASE("test_fail4")
{
    std::string in_file = "./corelib/input/JSON_checker/fail4.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
    }
    CHECK(err == jsoncons::json_errc::extra_comma);
}

TEST_CASE("test_fail5")
{
    std::string in_file = "./corelib/input/JSON_checker/fail5.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
    }
    CHECK(err == jsoncons::json_errc::expected_value);
}

TEST_CASE("test_fail6")
{
    std::string in_file = "./corelib/input/JSON_checker/fail6.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::expected_value);
}

TEST_CASE("test_fail7")
{
    std::string in_file = "./corelib/input/JSON_checker/fail7.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
    }
    CHECK(err == jsoncons::json_errc::extra_character);
}

TEST_CASE("test_fail8")
{
    std::string in_file = "./corelib/input/JSON_checker/fail8.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::extra_character);
}

TEST_CASE("test_fail9")
{
    std::string in_file = "./corelib/input/JSON_checker/fail9.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::extra_comma);
}

TEST_CASE("test_fail10")
{
    std::string in_file = "./corelib/input/JSON_checker/fail10.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;
    
    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
    }
    CHECK(err == jsoncons::json_errc::extra_character);
}

TEST_CASE("test_fail11")
{
    std::string in_file = "./corelib/input/JSON_checker/fail11.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::expected_comma_or_rbrace);
}

TEST_CASE("test_fail12")
{
    std::string in_file = "./corelib/input/JSON_checker/fail12.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::expected_value);
}

TEST_CASE("test_fail13")
{
    std::string in_file = "./corelib/input/JSON_checker/fail13.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::leading_zero);
}

TEST_CASE("test_fail14")
{
    std::string in_file = "./corelib/input/JSON_checker/fail14.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::invalid_number);
}

TEST_CASE("test_fail15")
{
    std::string in_file = "./corelib/input/JSON_checker/fail15.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::illegal_escaped_character);
}

TEST_CASE("test_fail16")
{
    std::string in_file = "./corelib/input/JSON_checker/fail16.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::expected_value);
}

TEST_CASE("test_fail17")
{
    std::string in_file = "./corelib/input/JSON_checker/fail17.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::illegal_escaped_character);
}

TEST_CASE("test_fail18")
{
    std::error_code err;

    std::string in_file = "./corelib/input/JSON_checker/fail18.json";
    std::ifstream is(in_file);
    REQUIRE(is);
    JSONCONS_TRY
    {
        auto options = json_options{}
            .max_nesting_depth(19);
        json::parse(is, options);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
         err = e.code();
         //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::max_nesting_depth_exceeded);
}

TEST_CASE("test_fail19")
{
    std::string in_file = "./corelib/input/JSON_checker/fail19.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::expected_colon);
}

TEST_CASE("test_fail20")
{
    std::string in_file = "./corelib/input/JSON_checker/fail20.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::expected_value);
}

TEST_CASE("test_fail21")
{
    std::string in_file = "./corelib/input/JSON_checker/fail21.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::expected_colon);
}

TEST_CASE("test_fail22")
{
    std::string in_file = "./corelib/input/JSON_checker/fail22.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::expected_comma_or_rbracket);
}

TEST_CASE("test_fail23")
{
    std::string in_file = "./corelib/input/JSON_checker/fail23.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::invalid_value);
}

TEST_CASE("test_fail24")
{
    std::string in_file = "./corelib/input/JSON_checker/fail24.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    // Single quote
    CHECK(err == jsoncons::json_errc::single_quote);
}

TEST_CASE("test_fail25")
{
    std::string in_file = "./corelib/input/JSON_checker/fail25.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::illegal_character_in_string);
}

TEST_CASE("test_fail26")
{
    std::string in_file = "./corelib/input/JSON_checker/fail26.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::illegal_escaped_character);
}

TEST_CASE("test_fail27")
{
    std::string in_file = "./corelib/input/JSON_checker/fail27.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::illegal_character_in_string);
}

TEST_CASE("test_fail28")
{
    std::string in_file = "./corelib/input/JSON_checker/fail28.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::illegal_escaped_character);
}

TEST_CASE("test_fail29")
{
    std::string in_file = "./corelib/input/JSON_checker/fail29.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::expected_value);
}

TEST_CASE("test_fail30")
{
    std::string in_file = "./corelib/input/JSON_checker/fail30.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::expected_value);
}

TEST_CASE("test_fail31")
{
    std::string in_file = "./corelib/input/JSON_checker/fail31.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::expected_value);
}

TEST_CASE("test_fail32")
{
    std::string in_file = "./corelib/input/JSON_checker/fail32.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::unexpected_eof);
}

TEST_CASE("test_fail33")
{
    std::string in_file = "./corelib/input/JSON_checker/fail33.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    std::error_code err;

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        err = e.code();
        //std::cout << in_file << " " << e.what() << '\n';
    }
    CHECK(err == jsoncons::json_errc::expected_comma_or_rbracket);
}

TEST_CASE("test_pass1")
{
    std::string in_file = "./corelib/input/JSON_checker/pass1.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        std::cout << in_file << " " << e.what() << '\n';
        throw;
    }
}

TEST_CASE("test_pass2")
{
    std::string in_file = "./corelib/input/JSON_checker/pass2.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        std::cout << in_file << " " << e.what() << '\n';
        throw;
    }
}

TEST_CASE("test_pass3")
{
    std::string in_file = "./corelib/input/JSON_checker/pass3.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    JSONCONS_TRY
    {
        json::parse(is);
    }
    JSONCONS_CATCH (const ser_error& e)
    {
        std::cout << in_file << " " << e.what() << '\n';
        throw;
    }
}

