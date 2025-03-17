/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <jsoncons_ext/bson/bson.hpp>
#include <jsoncons_ext/bson/bson_decimal128.hpp>
#include <jsoncons/json.hpp>

#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <limits>
#include <array>
#include <catch/catch.hpp>

namespace bson = jsoncons::bson;

TEST_CASE("test_decimal128_to_string__infinity")
{
    char buf[bson::decimal128_limits::buf_size];
    
    bson::decimal128_t positive_infinity(0x7800000000000000, 0);
    bson::decimal128_t negative_infinity(0xf800000000000000, 0);
    
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), positive_infinity);
        //std::cout << (uint64_t)buf << ", "<< (uint64_t)rc.ptr << "\n";
        CHECK(rc.ec == std::errc());
        std::string expected = "Infinity";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), negative_infinity);
        CHECK(rc.ec == std::errc());
        std::string expected = "-Infinity";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
}


TEST_CASE("test_decimal128_to_string__nan")
{
    /* All the above should just be NaN. */
    char buf[bson::decimal128_limits::buf_size+1];
    
    bson::decimal128_t dec_pnan(0x7c00000000000000, 0);
    bson::decimal128_t dec_nnan(0xfc00000000000000, 0);
    bson::decimal128_t dec_psnan(0x7e00000000000000, 0);
    bson::decimal128_t dec_nsnan(0xfe00000000000000, 0);
    bson::decimal128_t dec_payload_nan(0x7e00000000000000, 12);
    
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), dec_pnan);
        CHECK(rc.ec == std::errc());
        std::string expected = "NaN";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), dec_nnan);
        CHECK(rc.ec == std::errc());
        std::string expected = "NaN";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), dec_psnan);
        CHECK(rc.ec == std::errc());
        std::string expected = "NaN";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), dec_nsnan);
        CHECK(rc.ec == std::errc());
        std::string expected = "NaN";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), dec_payload_nan);
        CHECK(rc.ec == std::errc());
        std::string expected = "NaN";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
}

TEST_CASE("test_decimal128_to_string__regular")
{
    char buf[bson::decimal128_limits::buf_size+1];
    
    bson::decimal128_t one(0x3040000000000000, 0x0000000000000001);
    bson::decimal128_t zero(0x3040000000000000, 0x0000000000000000);
    bson::decimal128_t two(0x3040000000000000, 0x0000000000000002);
    bson::decimal128_t negative_one(0xb040000000000000, 0x0000000000000001);
    bson::decimal128_t negative_zero(0xb040000000000000, 0x0000000000000000);
    bson::decimal128_t tenth(0x303e000000000000, 0x0000000000000001); /* 0.1 */
    /* 0.001234 */
    bson::decimal128_t smallest_regular(0x3034000000000000, 0x00000000000004d2);
    /* 12345789012 */
    bson::decimal128_t largest_regular(0x3040000000000000, 0x0000001cbe991a14);
    /* 0.00123400000 */
    bson::decimal128_t trailing_zeros(0x302a000000000000, 0x00000000075aef40);
    /* 0.1234567890123456789012345678901234 */
    bson::decimal128_t all_digits(0x2ffc3cde6fff9732, 0xde825cd07e96aff2);
    
    /* 5192296858534827628530496329220095 */
    bson::decimal128_t full_house(0x3040ffffffffffff, 0xffffffffffffffff);
    
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), one);
        CHECK(rc.ec == std::errc());
        std::string expected = "1";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), zero);
        CHECK(rc.ec == std::errc());
        std::string expected = "0";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), two);
        CHECK(rc.ec == std::errc());
        CHECK (std::equal(buf, rc.ptr, "2"));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), negative_one);
        CHECK(rc.ec == std::errc());
        std::string expected = "-1";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), negative_zero);
        CHECK(rc.ec == std::errc());
        std::string expected = "-0";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), tenth);
        CHECK(rc.ec == std::errc());
        std::string expected = "0.1";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), smallest_regular);
        CHECK(rc.ec == std::errc());
        std::string expected = "0.001234";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), largest_regular);
        CHECK(rc.ec == std::errc());
        CHECK (std::equal(buf, rc.ptr, "123456789012"));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), trailing_zeros);
        CHECK(rc.ec == std::errc());
        std::string expected = "0.00123400000";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), all_digits);
        CHECK(rc.ec == std::errc());
        CHECK (std::equal(buf, rc.ptr, "0.1234567890123456789012345678901234"));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), full_house);
        CHECK(rc.ec == std::errc());
        CHECK (std::equal(buf, rc.ptr, "5192296858534827628530496329220095"));
    }
}

TEST_CASE("test_decimal128_to_string__scientific")
{
    char buf[bson::decimal128_limits::buf_size+1];
    
    bson::decimal128_t huge(0x5ffe314dc6448d93, 0x38c15b0a00000000); /* 1.000000000000000000000000000000000E+6144 */
    bson::decimal128_t tiny(0x0000000000000000, 0x0000000000000001); /* 1E-6176 */
    bson::decimal128_t neg_tiny(0x8000000000000000, 0x0000000000000001); /* -1E-6176 */
    bson::decimal128_t large(0x3108000000000000, 0x000009184db63eb1); /* 9.999987654321E+112 */
    bson::decimal128_t largest(0x5fffed09bead87c0, 0x378d8e63ffffffff); /* 9.999999999999999999999999999999999E+6144 */
    bson::decimal128_t tiniest(0x0001ed09bead87c0, 0x378d8e63ffffffff); /* 9.999999999999999999999999999999999E-6143 */
    bson::decimal128_t trailing_zero(0x304c000000000000, 0x000000000000041a); /* 1.050E9 */
    bson::decimal128_t one_trailing_zero(0x3042000000000000, 0x000000000000041a); /* 1.050E4 */
    bson::decimal128_t move_decimal(0x3040000000000000, 0x0000000000000069); /* 105 */
    bson::decimal128_t move_decimal_after(0x3042000000000000, 0x0000000000000069); /* 1.05E3 */
    bson::decimal128_t trailing_zero_no_decimal(0x3046000000000000, 0x0000000000000001); /* 1E3 */
    
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), huge);
        CHECK(rc.ec == std::errc());
        std::string expected = "1.000000000000000000000000000000000E+6144";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), tiny);
        CHECK(rc.ec == std::errc());
        std::string expected = "1E-6176";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), neg_tiny);
        CHECK(rc.ec == std::errc());
        CHECK (std::equal(buf, rc.ptr, "-1E-6176"));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), neg_tiny);
        CHECK(rc.ec == std::errc());
        CHECK (std::equal(buf, rc.ptr, "-1E-6176"));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), large);
        CHECK(rc.ec == std::errc());
        CHECK (std::equal(buf, rc.ptr, "9.999987654321E+112"));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), largest);
        CHECK(rc.ec == std::errc());
        CHECK (std::equal(buf, rc.ptr, "9.999999999999999999999999999999999E+6144"));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), tiniest);
        CHECK(rc.ec == std::errc());
        CHECK (std::equal(buf, rc.ptr, "9.999999999999999999999999999999999E-6143"));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), trailing_zero);
        CHECK(rc.ec == std::errc());
        CHECK (std::equal(buf, rc.ptr, "1.050E+9"));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), one_trailing_zero);
        CHECK(rc.ec == std::errc());
        CHECK (std::equal(buf, rc.ptr, "1.050E+4"));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), move_decimal);
        CHECK(rc.ec == std::errc());
        std::string expected = "105";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), move_decimal_after);
        CHECK(rc.ec == std::errc());
        std::string expected = "1.05E+3";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), trailing_zero_no_decimal);
        CHECK(rc.ec == std::errc());
        std::string expected = "1E+3";
        CHECK(static_cast<std::size_t>(rc.ptr - buf) == expected.size());
        CHECK(std::equal(buf, rc.ptr, expected.begin()));
    }
}

TEST_CASE("test_decimal128_to_string__zeros")
{
    char buf[bson::decimal128_limits::buf_size+1];
    
    bson::decimal128_t zero(0x3040000000000000, 0x0000000000000000); /* 0 */
    bson::decimal128_t pos_exp_zero(0x3298000000000000, 0x0000000000000000); /* 0E+300 */
    bson::decimal128_t neg_exp_zero(0x2b90000000000000, 0x0000000000000000); /* 0E-600 */
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), zero);
        CHECK(rc.ec == std::errc());
        CHECK (std::equal(buf, rc.ptr, "0"));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), pos_exp_zero);
        CHECK(rc.ec == std::errc());
        CHECK (std::equal(buf, rc.ptr, "0E+300"));
    }
    {
        auto rc = bson::decimal128_to_chars(buf, buf+sizeof(buf), neg_exp_zero);
        CHECK(rc.ec == std::errc());
        CHECK (std::equal(buf, rc.ptr, "0E-600"));
    }
}

TEST_CASE("test_decimal128_from_string__invalid_inputs")
{
    bson::decimal128_t dec;

   {
       char buf[] = ".";
       auto rc = bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       //std::cout << (uint64_t)rc.ptr << ", " << (uint64_t)(buf+(sizeof(buf)-1)) << "\n";
       CHECK_FALSE(rc.ec == std::errc());
       CHECK((rc.ptr == buf+(sizeof(buf)-1)));
       CHECK (is_nan (dec));
   }
   {
       char buf[] = ".e";
       auto rc = bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = ""; 
       auto rc = bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "invalid"; 
       auto rc = bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "in"; 
       auto rc = bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "i"; 
       auto rc = bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "E02"; 
       auto rc = bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "..1"; 
       auto rc = bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "1abcede"; 
       auto rc = bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "1.24abc"; 
       auto rc = bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "1.24abcE+02"; 
       auto rc = bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "1.24E+02abc2d"; 
       auto rc = bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "E+02"; 
       auto rc = bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "e+02"; 
       auto rc = bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = ".";
       auto rc = bson::decimal128_from_chars(buf, buf+1, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = ".e";
       auto rc = bson::decimal128_from_chars(buf, buf+2, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "";
       auto rc = bson::decimal128_from_chars(buf, buf, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "invalid";
       auto rc = bson::decimal128_from_chars(buf, buf+7, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "in";
       auto rc = bson::decimal128_from_chars(buf, buf+2, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "i";
       auto rc = bson::decimal128_from_chars(buf, buf+1, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "E02";
       auto rc = bson::decimal128_from_chars(buf, buf+3, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "..1";
       auto rc = bson::decimal128_from_chars(buf, buf+3, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "1abcede";
       auto rc = bson::decimal128_from_chars(buf, buf+7, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "1.24abc";
       auto rc = bson::decimal128_from_chars(buf, buf+7, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "1.24abcE+02";
       auto rc = bson::decimal128_from_chars(buf, buf+11, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "1.24E+02abc2d";
       auto rc = bson::decimal128_from_chars(buf, buf+13, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "E+02";
       auto rc = bson::decimal128_from_chars(buf, buf+4, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "e+02";
       auto rc = bson::decimal128_from_chars(buf, buf+4, dec);
       CHECK_FALSE(rc.ec == std::errc());
       CHECK (is_nan (dec));
   }
}

TEST_CASE("test_decimal128_from_string__nan")
{
   bson::decimal128_t dec;

   {
       char buf[] = "NaN"; 
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "+NaN"; 
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "-NaN"; 
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "-nan"; 
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "1e"; 
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "+nan"; 
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "nan"; 
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "Nan"; 
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "+Nan"; 
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "-Nan"; 
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "NaN"; 
       bson::decimal128_from_chars(buf, buf+3, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "+NaN";
       bson::decimal128_from_chars(buf, buf+4, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "-NaN";
       bson::decimal128_from_chars(buf, buf+4, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "-nan";
       bson::decimal128_from_chars(buf, buf+4, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "1e";
       bson::decimal128_from_chars(buf, buf+2, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "+nan";
       bson::decimal128_from_chars(buf, buf+4, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "nan";
       bson::decimal128_from_chars(buf, buf+3, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "Nan";
       bson::decimal128_from_chars(buf, buf+3, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "+Nan";
       bson::decimal128_from_chars(buf, buf+4, dec);
       CHECK (is_nan (dec));
   }
   {
       char buf[] = "-Nan";
       bson::decimal128_from_chars(buf, buf+4, dec);
       CHECK (is_nan (dec));
   }
}

TEST_CASE("test_decimal128_from_string__infinity")
{
   bson::decimal128_t dec;

   {
       char buf[] = "Infinity"; 
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK (is_inf (dec));
   }
   {
       char buf[] = "+Infinity"; 
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK (is_inf (dec));
   }
   {
       char buf[] = "+Inf"; 
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK (is_inf (dec));
   }
   {
       char buf[] = "-Inf"; 
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK (is_neg_inf (dec));
   }
   {
       char buf[] = "-Infinity"; 
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, dec);
       CHECK (is_neg_inf (dec));
   }

   {
       char buf[] = "Infinity";
       bson::decimal128_from_chars(buf, buf+8, dec);
       CHECK (is_inf (dec));
   }
   {
       char buf[] = "+Infinity";
       bson::decimal128_from_chars(buf, buf+9, dec);
       CHECK (is_inf (dec));
   }
   {
       char buf[] = "+Inf";
       bson::decimal128_from_chars(buf, buf+4, dec);
       CHECK (is_inf (dec));
   }
   {
       char buf[] = "-Inf";
       bson::decimal128_from_chars(buf, buf+4, dec);
       CHECK (is_neg_inf (dec));
   }
   {
       char buf[] = "-Infinity";
       bson::decimal128_from_chars(buf, buf+9, dec);
       CHECK (is_neg_inf (dec));
   }
}

TEST_CASE("test_decimal128_from_string__simple")
{
   bson::decimal128_t one;
   bson::decimal128_t negative_one;
   bson::decimal128_t zero;
   bson::decimal128_t negative_zero;
   bson::decimal128_t number;
   bson::decimal128_t number_two;
   bson::decimal128_t negative_number;
   bson::decimal128_t fractional_number;
   bson::decimal128_t leading_zeros;
   bson::decimal128_t leading_insignificant_zeros;

   {
       char buf[] = "1";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, one);
   }
   {
       char buf[] = "-1";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, negative_one);
   }
   {
       char buf[] = "0";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, zero);
   }
   {
       char buf[] = "-0";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, negative_zero);
   }
   {
       char buf[] = "12345678901234567";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, number);
   }
   {
       char buf[] = "989898983458";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, number_two);
   }
   {
       char buf[] = "-12345678901234567";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, negative_number);
   }
   {
       char buf[] = "0.12345";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, fractional_number);
   }
   {
       char buf[] = "0.0012345";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, leading_zeros);
   }
   {
       char buf[] = "00012345678901234567";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, leading_insignificant_zeros);
   }

   CHECK (one == bson::decimal128_t(0x3040000000000000, 0x0000000000000001));
   CHECK (negative_one == bson::decimal128_t(0xb040000000000000, 0x0000000000000001));
   CHECK (zero == bson::decimal128_t(0x3040000000000000, 0x0000000000000000));
   CHECK (negative_zero == bson::decimal128_t(0xb040000000000000, 0x0000000000000000));
   CHECK (number == bson::decimal128_t(0x3040000000000000, 0x002bdc545d6b4b87));
   CHECK (number_two == bson::decimal128_t(0x3040000000000000, 0x000000e67a93c822));
   CHECK (negative_number == bson::decimal128_t(0xb040000000000000, 0x002bdc545d6b4b87));
   CHECK (fractional_number == bson::decimal128_t(0x3036000000000000, 0x0000000000003039));
   CHECK (leading_zeros == bson::decimal128_t(0x3032000000000000, 0x0000000000003039));
   CHECK (leading_insignificant_zeros == bson::decimal128_t(0x3040000000000000, 0x002bdc545d6b4b87));

   {
       char buf[] = "1";
       bson::decimal128_from_chars(buf, buf+1, one);
   }
   {
       char buf[] = "-1";
       bson::decimal128_from_chars(buf, buf+2, negative_one);
   }
   {
       char buf[] = "0";
       bson::decimal128_from_chars(buf, buf+1, zero);
   }
   {
       char buf[] = "-0";
       bson::decimal128_from_chars(buf, buf+2, negative_zero);
   }
   {
       char buf[] = "12345678901234567";
       bson::decimal128_from_chars(buf, buf+17, number);
   }
   {
       char buf[] = "989898983458";
       bson::decimal128_from_chars(buf, buf+12, number_two);
       CHECK (number_two == bson::decimal128_t(0x3040000000000000, 0x000000e67a93c822));
   }
   {
       char buf[] = "-12345678901234567";
       bson::decimal128_from_chars(buf, buf+18, negative_number);
   }

   {
       char buf[] = "0.12345";
       bson::decimal128_from_chars(buf, buf+7, fractional_number);
   }
   {
       char buf[] = "0.0012345";
       bson::decimal128_from_chars(buf, buf+9, leading_zeros);
   }

   {
       char buf[] = "00012345678901234567";
       bson::decimal128_from_chars(buf, buf+20, leading_insignificant_zeros);
   }

   CHECK (one == bson::decimal128_t(0x3040000000000000, 0x0000000000000001));
   CHECK (negative_one == bson::decimal128_t(0xb040000000000000, 0x0000000000000001));
   CHECK (zero == bson::decimal128_t(0x3040000000000000, 0x0000000000000000));
   CHECK (negative_zero == bson::decimal128_t(0xb040000000000000, 0x0000000000000000));
   CHECK (number == bson::decimal128_t(0x3040000000000000, 0x002bdc545d6b4b87));
   CHECK (negative_number == bson::decimal128_t(0xb040000000000000, 0x002bdc545d6b4b87));
   CHECK (fractional_number == bson::decimal128_t(0x3036000000000000, 0x0000000000003039));
   CHECK (leading_zeros == bson::decimal128_t(0x3032000000000000, 0x0000000000003039));
   CHECK (leading_insignificant_zeros == bson::decimal128_t(0x3040000000000000, 0x002bdc545d6b4b87));
}


TEST_CASE("test_decimal128_from_string__scientific")
{
   bson::decimal128_t ten;
   bson::decimal128_t ten_again;
   bson::decimal128_t one;
   bson::decimal128_t huge_exp;
   bson::decimal128_t tiny_exp;
   bson::decimal128_t fractional;
   bson::decimal128_t trailing_zeros;

   {
       char buf[] = "10e0";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, ten);
       CHECK (ten == bson::decimal128_t(0x3040000000000000, 0x000000000000000a));
   }
   {
       char buf[] = "1e1";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, ten_again);
   }
   {
       char buf[] = "10e-1";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, one);
   }

   CHECK (ten == bson::decimal128_t(0x3040000000000000, 0x000000000000000a));
   CHECK (ten_again == bson::decimal128_t(0x3042000000000000, 0x0000000000000001));
   CHECK (one == bson::decimal128_t(0x303e000000000000, 0x000000000000000a));

   {
       char buf[] = "12345678901234567e6111";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, huge_exp);
   }
   {
       char buf[] = "1e-6176";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, tiny_exp);
   }

   CHECK (huge_exp == bson::decimal128_t(0x5ffe000000000000, 0x002bdc545d6b4b87));
   CHECK (tiny_exp == bson::decimal128_t(0x0000000000000000, 0x0000000000000001));

   {
       char buf[] = "-100E-10";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, fractional);
   }
   {
       char buf[] = "10.50E8";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, trailing_zeros);
   }

   CHECK (fractional == bson::decimal128_t(0xb02c000000000000, 0x0000000000000064));
   CHECK (trailing_zeros == bson::decimal128_t(0x304c000000000000, 0x000000000000041a));


   {
       char buf[] = "10e0";
       bson::decimal128_from_chars(buf, buf+4, ten);
   }
   {
       char buf[] = "1e1";
       bson::decimal128_from_chars(buf, buf+3, ten_again);
   }
   {
       char buf[] = "10e-1";
       bson::decimal128_from_chars(buf, buf+5, one);
   }

   CHECK (ten_again == bson::decimal128_t(0x3042000000000000, 0x0000000000000001));
   CHECK (one == bson::decimal128_t(0x303e000000000000, 0x000000000000000a));

   {
       char buf[] = "12345678901234567e6111";
       bson::decimal128_from_chars(buf, buf+22, huge_exp);
   }
   {
       char buf[] = "1e-6176";
       bson::decimal128_from_chars(buf, buf+7, tiny_exp);
   }

   CHECK (huge_exp == bson::decimal128_t(0x5ffe000000000000, 0x002bdc545d6b4b87));
   CHECK (tiny_exp == bson::decimal128_t(0x0000000000000000, 0x0000000000000001));

   {
       char buf[] = "-100E-10";
       bson::decimal128_from_chars(buf, buf+8, fractional);
   }
   {
       char buf[] = "10.50E8";
       bson::decimal128_from_chars(buf, buf+7, trailing_zeros);
   }

   CHECK (fractional == bson::decimal128_t(0xb02c000000000000, 0x0000000000000064));
   CHECK (trailing_zeros == bson::decimal128_t(0x304c000000000000, 0x000000000000041a));
}


TEST_CASE("test_decimal128_from_string__large")
{
   bson::decimal128_t large;
   bson::decimal128_t all_digits;
   bson::decimal128_t largest;
   bson::decimal128_t tiniest;
   bson::decimal128_t full_house;

   {
       char buf[] = "12345689012345789012345";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, large);
   }
   {
       char buf[] = "1234567890123456789012345678901234";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, all_digits);
   }
   {
       char buf[] = "9.999999999999999999999999999999999E+6144";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, largest);
   }
   {
       char buf[] = "9.999999999999999999999999999999999E-6143";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, tiniest);
   }
   {
       char buf[] = "5.192296858534827628530496329220095E+33";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, full_house);
   }

   CHECK (large == bson::decimal128_t(0x304000000000029d, 0x42da3a76f9e0d979));
   CHECK (all_digits == bson::decimal128_t(0x30403cde6fff9732, 0xde825cd07e96aff2));
   CHECK (largest == bson::decimal128_t(0x5fffed09bead87c0, 0x378d8e63ffffffff));
   CHECK (tiniest == bson::decimal128_t(0x0001ed09bead87c0, 0x378d8e63ffffffff));
   CHECK (full_house == bson::decimal128_t(0x3040ffffffffffff, 0xffffffffffffffff));


   {
       char buf[] = "12345689012345789012345";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, large);
   }
   {
       char buf[] = "1234567890123456789012345678901234";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, all_digits);
   }
   {
       char buf[] = "9.999999999999999999999999999999999E+6144";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, largest);
   }
   {
       char buf[] = "9.999999999999999999999999999999999E-6143";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, tiniest);
   }
   {
       char buf[] = "5.192296858534827628530496329220095E+33";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, full_house);
   }

   CHECK (large == bson::decimal128_t(0x304000000000029d, 0x42da3a76f9e0d979));
   CHECK (all_digits == bson::decimal128_t(0x30403cde6fff9732, 0xde825cd07e96aff2));
   CHECK (largest == bson::decimal128_t(0x5fffed09bead87c0, 0x378d8e63ffffffff));
   CHECK (tiniest == bson::decimal128_t(0x0001ed09bead87c0, 0x378d8e63ffffffff));
   CHECK (full_house == bson::decimal128_t(0x3040ffffffffffff, 0xffffffffffffffff));
}


TEST_CASE("test_decimal128_from_string__exponent_normalization")
{
   bson::decimal128_t trailing_zeros;
   bson::decimal128_t one_normalize;
   bson::decimal128_t no_normalize;
   bson::decimal128_t a_disaster;

   {
       char buf[] = "1000000000000000000000000000000000000000";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, trailing_zeros);
   }
   {
       char buf[] = "10000000000000000000000000000000000";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, one_normalize);
   }
   {
       char buf[] = "1000000000000000000000000000000000";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, no_normalize);
   }
   {
       char buf[] = "100000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "0000000000000000000000000000000000";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, a_disaster);
   }

   CHECK (trailing_zeros == bson::decimal128_t(0x304c314dc6448d93, 0x38c15b0a00000000));
   CHECK (one_normalize == bson::decimal128_t(0x3042314dc6448d93, 0x38c15b0a00000000));
   CHECK (no_normalize == bson::decimal128_t(0x3040314dc6448d93, 0x38c15b0a00000000));
   CHECK (a_disaster == bson::decimal128_t(0x37cc314dc6448d93, 0x38c15b0a00000000));


   {
       char buf[] = "1000000000000000000000000000000000000000";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, trailing_zeros);
   }
   {
       char buf[] = "10000000000000000000000000000000000";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, one_normalize);
   }
   {
       char buf[] = "1000000000000000000000000000000000";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, no_normalize);
   }
   {
       char buf[] = "100000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000"
          "0000000000000000000000000000000000";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, a_disaster);

   }
   CHECK (trailing_zeros == bson::decimal128_t(0x304c314dc6448d93, 0x38c15b0a00000000));
   CHECK (one_normalize == bson::decimal128_t(0x3042314dc6448d93, 0x38c15b0a00000000));
   CHECK (no_normalize == bson::decimal128_t(0x3040314dc6448d93, 0x38c15b0a00000000));
   CHECK (a_disaster == bson::decimal128_t(0x37cc314dc6448d93, 0x38c15b0a00000000));
}


TEST_CASE("test_decimal128_from_string__zeros")
{
   bson::decimal128_t zero;
   bson::decimal128_t exponent_zero;
   bson::decimal128_t large_exponent;
   bson::decimal128_t negative_zero;

   {
       char buf[] = "0";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, zero);
   }
   {
       char buf[] = "0e-611";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, exponent_zero);
   }
   {
       char buf[] = "0e+6000";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, large_exponent);
   }
   {
       char buf[] = "-0e-1";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, negative_zero);
   }

   CHECK (zero == bson::decimal128_t(0x3040000000000000, 0x0000000000000000));
   CHECK (exponent_zero == bson::decimal128_t(0x2b7a000000000000, 0x0000000000000000));
   CHECK (large_exponent == bson::decimal128_t(0x5f20000000000000, 0x0000000000000000));
   CHECK (negative_zero == bson::decimal128_t(0xb03e000000000000, 0x0000000000000000));


   {
       char buf[] = "0";
       bson::decimal128_from_chars(buf, buf+1, zero);
   }
   {
       char buf[] = "0e-611";
       bson::decimal128_from_chars(buf, buf+sizeof(buf)-1, exponent_zero);
   }
   {
       char buf[] = "0e+6000";
       bson::decimal128_from_chars(buf, buf+7, large_exponent);
   }
   {
       char buf[] = "-0e-1";
       bson::decimal128_from_chars(buf, buf+5, negative_zero);
   }

   CHECK (zero == bson::decimal128_t(0x3040000000000000, 0x0000000000000000));
   CHECK (exponent_zero == bson::decimal128_t(0x2b7a000000000000, 0x0000000000000000));
   CHECK (large_exponent == bson::decimal128_t(0x5f20000000000000, 0x0000000000000000));
   CHECK (negative_zero == bson::decimal128_t(0xb03e000000000000, 0x0000000000000000));
}

TEST_CASE("test_decimal128_from_string_w_len__special")
{
   bson::decimal128_t number;
   bson::decimal128_t number_two;
   bson::decimal128_t negative_number;

   /* These strings have more bytes than the length indicates. */
   {
       char buf[] = "12345678901234567abcd";
       bson::decimal128_from_chars(buf, buf+17, number);
   }
   {
       char buf[] = "989898983458abcd";
       bson::decimal128_from_chars(buf, buf+12, number_two);
   }
   {
       char buf[] = "-12345678901234567abcd";
       bson::decimal128_from_chars(buf, buf+18, negative_number);
   }

   CHECK (number == bson::decimal128_t(0x3040000000000000, 0x002bdc545d6b4b87));
   CHECK (number_two == bson::decimal128_t(0x3040000000000000, 0x000000e67a93c822));
   CHECK (negative_number == bson::decimal128_t(0xb040000000000000, 0x002bdc545d6b4b87));
}

