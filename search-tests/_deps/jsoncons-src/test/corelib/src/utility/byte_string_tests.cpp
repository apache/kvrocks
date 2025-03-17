// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>

#include <ctime>
#include <iostream>
#include <map>
#include <new>
#include <sstream>
#include <utility>
#include <vector>
#include <catch/catch.hpp>

using namespace jsoncons;

// https://tools.ietf.org/html/rfc4648#section-4 test vectors

template <typename CharT>
void check_encode_base64(const std::vector<uint8_t>& input, const std::basic_string<CharT>& expected)
{
    std::basic_string<CharT> result;
    encode_base64(input.begin(),input.end(),result);
    REQUIRE(result.size() == expected.size());
    for (std::size_t i = 0; i < result.size(); ++i)
    {
        CHECK(result[i] == expected[i]);
    }

    std::vector<uint8_t> output;
    decode_base64(result.begin(), result.end(), output);
    REQUIRE(output.size() == input.size());
    for (std::size_t i = 0; i < output.size(); ++i)
    {
        CHECK(output[i] == input[i]);
    }
}

template <typename CharT>
void check_encode_base64url(const std::vector<uint8_t>& input, const std::basic_string<CharT>& expected)
{
    std::basic_string<CharT> result;
    encode_base64url(input.begin(),input.end(),result);
    REQUIRE(result.size() == expected.size());
    for (std::size_t i = 0; i < result.size(); ++i)
    {
        CHECK(result[i] == expected[i]);
    }

    std::vector<uint8_t> output; 
    decode_base64url(result.begin(), result.end(), output);
    REQUIRE(output.size() == input.size());
    for (std::size_t i = 0; i < output.size(); ++i)
    {
        CHECK(output[i] == input[i]);
    }
}

template <typename CharT>
void check_encode_base16(const std::vector<uint8_t>& input, const std::basic_string<CharT>& expected)
{
    std::basic_string<CharT> result;
    encode_base16(input.begin(),input.end(), result);
    REQUIRE(result.size() == expected.size());
    for (std::size_t i = 0; i < result.size(); ++i)
    {
        CHECK(result[i] == expected[i]);
    }

    std::vector<uint8_t> output;
    auto res = decode_base16(result.begin(), result.end(), output);
    REQUIRE(res.ec == conv_errc::success);
    REQUIRE(output.size() == input.size());
    for (std::size_t i = 0; i < output.size(); ++i)
    {
        CHECK(output[i] == input[i]);
    }
}

TEST_CASE("test_base64_conversion")
{
    SECTION("char")
    {
        check_encode_base64<char>({}, "");
        check_encode_base64<char>({'f'}, "Zg==");
        check_encode_base64<char>({'f','o'}, "Zm8=");
        check_encode_base64<char>({'f','o','o'}, "Zm9v");
        check_encode_base64<char>({'f','o','o','b'}, "Zm9vYg==");
        check_encode_base64<char>({'f','o','o','b','a'}, "Zm9vYmE=");
        check_encode_base64<char>({'f','o','o','b','a','r'}, "Zm9vYmFy");
    }
    SECTION("wchar_t")
    {
        check_encode_base64<wchar_t>({}, L"");
        check_encode_base64<wchar_t>({'f'}, L"Zg==");
        check_encode_base64<wchar_t>({'f','o'}, L"Zm8=");
        check_encode_base64<wchar_t>({'f','o','o'}, L"Zm9v");
        check_encode_base64<wchar_t>({'f','o','o','b'}, L"Zm9vYg==");
        check_encode_base64<wchar_t>({'f','o','o','b','a'}, L"Zm9vYmE=");
        check_encode_base64<wchar_t>({'f','o','o','b','a','r'}, L"Zm9vYmFy");
    }
}

TEST_CASE("test_base64url_conversion")
{
    SECTION("char")
    {
        check_encode_base64url<char>({}, "");
        check_encode_base64url<char>({'f'}, "Zg");
        check_encode_base64url<char>({'f','o'}, "Zm8");
        check_encode_base64url<char>({'f','o','o'}, "Zm9v");
        check_encode_base64url<char>({'f','o','o','b'}, "Zm9vYg");
        check_encode_base64url<char>({'f','o','o','b','a'}, "Zm9vYmE");
        check_encode_base64url<char>({'f','o','o','b','a','r'}, "Zm9vYmFy");
    }
    SECTION("wchar_t")
    {
        check_encode_base64url<wchar_t>({}, L"");
        check_encode_base64url<wchar_t>({'f'}, L"Zg");
        check_encode_base64url<wchar_t>({'f','o'}, L"Zm8");
        check_encode_base64url<wchar_t>({'f','o','o'}, L"Zm9v");
        check_encode_base64url<wchar_t>({'f','o','o','b'}, L"Zm9vYg");
        check_encode_base64url<wchar_t>({'f','o','o','b','a'}, L"Zm9vYmE");
        check_encode_base64url<wchar_t>({'f','o','o','b','a','r'}, L"Zm9vYmFy");
    }
}
 
TEST_CASE("test_base16_conversion")
{
    SECTION ("string")
    {
        check_encode_base16<char>({}, "");
        check_encode_base16<char>({'f'}, "66");
        check_encode_base16<char>({'f','o'}, "666F");
        check_encode_base16<char>({'f','o','o'}, "666F6F");
        check_encode_base16<char>({'f','o','o','b'}, "666F6F62");
        check_encode_base16<char>({'f','o','o','b','a'}, "666F6F6261");
        check_encode_base16<char>({'f','o','o','b','a','r'}, "666F6F626172");
    }
    SECTION ("wstring")
    {
        check_encode_base16<wchar_t>({}, L"");
        check_encode_base16<wchar_t>({'f'}, L"66");
        check_encode_base16<wchar_t>({'f','o'}, L"666F");
        check_encode_base16<wchar_t>({'f','o','o'}, L"666F6F");
        check_encode_base16<wchar_t>({'f','o','o','b'}, L"666F6F62");
        check_encode_base16<wchar_t>({'f','o','o','b','a'}, L"666F6F6261");
        check_encode_base16<wchar_t>({'f','o','o','b','a','r'}, L"666F6F626172");
    }
}

TEST_CASE("byte_string_view constructors")
{
    SECTION("test 1")
    {
        std::vector<uint8_t> v = {'f','o','o','b','a','r'};
        byte_string_view bstr(v);
        CHECK(bstr[0] == 'f');
        CHECK(bstr[1] == 'o');
        CHECK(bstr[2] == 'o');
        CHECK(bstr[3] == 'b');
        CHECK(bstr[4] == 'a');
        CHECK(bstr[5] == 'r');

        byte_string_view copied(bstr);
        CHECK(copied == bstr);

        byte_string_view moved(std::move(bstr));
        CHECK(bstr.data() == nullptr);
        CHECK(bstr.size() == 0);

        REQUIRE(moved.size() == 6);
        CHECK(moved[0] == 'f');
        CHECK(moved[1] == 'o');
        CHECK(moved[2] == 'o');
        CHECK(moved[3] == 'b');
        CHECK(moved[4] == 'a');
        CHECK(moved[5] == 'r');
    }
}

TEST_CASE("byte_string mutators")
{
    SECTION("append")
    {
        std::vector<uint8_t> u = {'b','a','z'};
        std::vector<uint8_t> v = {'f','o','o','b','a','r'};
        byte_string bstr(u.data(),3);
        bstr.append(v.data(), 6);

        CHECK(bstr[0] == 'b');
        CHECK(bstr[1] == 'a');
        CHECK(bstr[2] == 'z');
        CHECK(bstr[3] == 'f');
        CHECK(bstr[4] == 'o');
        CHECK(bstr[5] == 'o');
        CHECK(bstr[6] == 'b');
        CHECK(bstr[7] == 'a');
        CHECK(bstr[8] == 'r');
    }
    SECTION("assign")
    {
        std::vector<uint8_t> v = {'f','o','o','b','a','r'};
        byte_string bstr;
        bstr.assign(v.data(), 6);

        CHECK(bstr[0] == 'f');
        CHECK(bstr[1] == 'o');
        CHECK(bstr[2] == 'o');
        CHECK(bstr[3] == 'b');
        CHECK(bstr[4] == 'a');
        CHECK(bstr[5] == 'r');
    }
}

TEST_CASE("byte_string_view iterators")
{
    SECTION("begin/end")
    {
        std::vector<uint8_t> v = {'f','o','o'};
        byte_string_view bstr(v);

        auto it = bstr.begin();
        REQUIRE(it != bstr.end());
        CHECK(*it++ == 'f');
        REQUIRE(it != bstr.end());
        CHECK(*it++ == 'o');
        REQUIRE(it != bstr.end());
        CHECK(*it++ == 'o');
        CHECK(it == bstr.end());
    }
}

