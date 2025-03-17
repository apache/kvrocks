// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h"
#endif
#include <jsoncons/utility/bigint.hpp>
#include <jsoncons/json.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("test_positive_bignum")
{
    std::string expected = "18446744073709551616";
    std::vector<uint8_t> v = {1,0,0,0,0,0,0,0,0};
    bigint x = bigint::from_bytes_be(1, v.data(),v.size());

    std::string sx = x.to_string();
    CHECK(expected == sx);

    bigint y(x);
    std::string sy = y.to_string();
    CHECK(expected == sy);

    bigint z;
    z = x;
    std::string sz = y.to_string();
    CHECK(expected == sz);

    SECTION("write_string_hex")
    {
        std::string exp = "10000000000000000";
        std::string s;
        x.write_string_hex(s);
        CHECK(s == exp);
    }

}

TEST_CASE("bignums are equal")
{
    std::string s = "18446744073709551616";
    bigint x = bigint::from_string(s);
    bigint y = bigint::from_string(s);

    bool test = x == y;
    CHECK(test);
}

TEST_CASE("test_negative_bignum")
{
    std::string expected = "-18446744073709551617";
    std::vector<uint8_t> b = {0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
    bigint x = bigint::from_bytes_be(1, b.data(),b.size());
    bigint x2 = -1 - x;

    std::string sx = x2.to_string();
    CHECK(expected == sx);

    bigint y(x2);
    std::string sy = y.to_string();
    CHECK(expected == sy);

    bigint z;
    z = x2;
    std::string sz = z.to_string();
    CHECK(expected == sz);

    int signum;
    std::vector<uint8_t> v;
    x.write_bytes_be(signum, v);

    REQUIRE(v.size() == b.size());
    for (std::size_t i = 0; i < v.size(); ++i)
    {
        REQUIRE(v[i] == b[i]);
    }

    SECTION("write_string_hex")
    {
        std::string exp = "-10000000000000001";
        std::string s;
        x2.write_string_hex(s);
        //std::cout << "bigint: " << expected << ", s: " << s << "\n";
        CHECK(s == exp);
    }
}

TEST_CASE("test_longlong")
{
    long long n = (std::numeric_limits<long long>::max)();

    bigint val(n);

    //std::cout << "long long " << n << " == " << val << '\n';
    //std::cout << val.to_string(16) << '\n';
}

TEST_CASE("test_bignum2")
{
    std::string v = "10000000000000000";
    bigint val = bigint::from_string(v.data());

    //std::cout << val << '\n';
}

TEST_CASE("test_logical_operations")
{
    bigint x = bigint::from_string( "888888888888888888" );
    bigint y = bigint::from_string( "888888888888888888" );

    bigint z = x & y;

    bool test = z == x;
    CHECK(test);
}

TEST_CASE("test_addition")
{
    bigint x = bigint::from_string( "4444444444444444444444444444444" );
    bigint y = bigint::from_string( "4444444444444444444444444444444" );
    bigint a = bigint::from_string( "8888888888888888888888888888888" );

    bigint z = x + y;
    bool test = z == a;
    CHECK(test);
}

TEST_CASE("test_multiplication")
{
    bigint x = bigint::from_string( "4444444444444444444444444444444" );
    bigint a = bigint::from_string( "8888888888888888888888888888888" );

    bigint z = 2*x;
    bool test = z == a;
    CHECK(test);

    z = x*2;

    test = z == a;
    CHECK(test);
}

TEST_CASE("test_conversion_0")
{
    bigint x;

    json j(x);

    bigint y = j.as<bigint>();
    CHECK(bool(x == y));

    std::string s = y.to_string();

    CHECK(s == "0");
}

TEST_CASE("test_traits1")
{
    std::vector<uint8_t> data = {0x01,0x00};
    bigint x = bigint::from_bytes_be(1, data.data(), data.size());

    json j(x);

    bigint y = j.as<bigint>();
    CHECK(bool(x == y));

    std::string s = y.to_string();

    CHECK(s == "256");
}

TEST_CASE("test_traits2")
{
    std::vector<uint8_t> data = {0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
    bigint x = bigint::from_bytes_be(1, data.data(), data.size());

    json j(x);

    bigint y = j.as<bigint>();
    CHECK(bool(x == y));

    std::string s = y.to_string();

    CHECK(s == "18446744073709551616");
}

TEST_CASE("test_traits3")
{
    std::vector<uint8_t> data = {0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
    bigint x = bigint::from_bytes_be(1, data.data(), data.size());

    x = -1 - x;

    int signum;
    std::vector<uint8_t> v;
    x.write_bytes_be(signum,v);

    REQUIRE(signum == -1);
    //for (auto c : v)
    //{
    //    //std::cout << std::hex << (int)c;
    //}
    //std::cout << '\n';

    json j(x);

    bigint y = j.as<bigint>();
    CHECK(bool(x == y));

    std::string s = y.to_string();

    CHECK(s == "-18446744073709551617");
}

TEST_CASE("test shift left")
{
    SECTION("n << 1")
    {
        bigint n = bigint::from_string("1");
        bigint x = n << 1;
        std::string s = x.to_string();
        CHECK(s == "2");
    }
    SECTION("n << 100")
    {
        bigint n(1);
        bigint x = n << 100;
        std::string s = x.to_string();
        CHECK(s == "1267650600228229401496703205376");
    }
    SECTION("n << 100, += 1")
    {
        bigint n(1);
        bigint x = n << 100;
        x += 1;
        std::string s = x.to_string();
        CHECK(s == "1267650600228229401496703205377");
    }
}

TEST_CASE("times 10")
{
    SECTION("1")
    {
        bigint n = bigint::from_string("1234");
        bigint m = n * 10;
        std::string s = m.to_string();
        CHECK(s == "12340");
    }
    SECTION("31")
    {
        std::string expected("1234");
        bigint n = bigint::from_string(expected);

        for (std::size_t i = 0; i < 31; ++i)
        {
            n *= (uint64_t)10;
            expected.push_back('0');
        }
        std::string s = n.to_string();
        CHECK(expected == s);
        //std::cout << "x31: " << s << "\n";
    }
    SECTION("32")
    {
        std::string expected("1234");
        bigint n = bigint::from_string(expected);
        for (std::size_t i = 0; i < 32; ++i)
        {
            n *= (uint64_t)10;
            expected.push_back('0');
        }
        std::string s = n.to_string();
        CHECK(expected == s);
        //std::cout << "x31: " << s << "\n";
    }
}

TEST_CASE("bigint div")
{
#if defined(_MSC_VER) && _MSC_VER >= 1910
    SECTION("bigint")
    {
        bigint big_pos = bigint::from_string("18364494661702398480");
        bigint small_pos = bigint::from_string("65535");
        bigint res_pos = bigint::from_string("280224226164681");
        bigint big_neg = -big_pos;
        bigint small_neg = -small_pos;
        bigint res_neg = -res_pos;

        CHECK((big_neg / big_neg) == bigint(1));
        CHECK((big_neg / small_neg) == res_pos);
        CHECK((big_neg / small_pos) == res_neg);
        CHECK((big_neg / big_pos) == bigint(-1));

        CHECK((small_neg / big_neg) == bigint(0));
        CHECK((small_neg / small_neg) == bigint(1));
        CHECK((small_neg / small_pos) == bigint(-1));
        CHECK((small_neg / big_pos) == bigint(0));

        CHECK((small_pos / big_neg) == bigint(0));
        CHECK((small_pos / small_neg) == bigint(-1));
        CHECK((small_pos / small_pos) == bigint(1));
        CHECK((small_pos / big_pos) == bigint(0));

        CHECK((big_pos / big_neg) == bigint(-1));
        CHECK((big_pos / small_neg) == res_neg);
        CHECK((big_pos / small_pos) == res_pos);
        CHECK((big_pos / big_pos) == bigint(1));
    }
#endif
}

// Source: https://github.com/justmoon/node-bignum/blob/master/test/big.js

TEST_CASE("bigint operations")
{
    SECTION("plus")
    {
        bigint a = bigint::from_string("20178175244496647895629245678926563358862835685868092718528786189298896755892724096350318132354654969715294305656279188466948605121492948268400884893722767401972695174353441");
        bigint b = bigint::from_string("93976986297275963857794534313022836860642008364607162222395304627737845003599751108876721426146679370149373711095582235633733294240624814097369771481147215472578762824607080");
        bigint c = a + b;
        bigint expected = bigint::from_string("114155161541772611753423779991949400219504844050475254940924090817036741759492475205227039558501334339864668016751861424100681899362117762365770656374869982874551457998960521");

        CHECK(expected == (c));
    }

    SECTION("minus")
    {
        bigint a = bigint::from_string("63584976221895260406245934266037944699776129516216688813405106853198139417759498415735161100030933326522675347686646219695144553808051168706779408804756208386011014197185296");
        bigint b = bigint::from_string("75761734353628069683913529566109295493116360791340046058510920764479664838827482335858563500856417188227416490721063436557647698896399869016678013515043471880323279258685478");
        bigint c = a - b;
        bigint expected = bigint::from_string("-12176758131732809277667595300071350793340231275123357245105813911281525421067983920123402400825483861704741143034417216862503145088348700309898604710287263494312265061500182");

        CHECK(expected == (c));
    }

    SECTION("mult")
    {
        bigint a = bigint::from_string("43359329001059048967113581928625959342654930666632400867978208429224464941890190751598229305718587280094852374898291386268967561418738958337632249177044975686477011571044266");
        bigint b = bigint::from_string("1277902648419017187919156692641295109476255233737630537760832794503886212911067061184379695097643279217271150419129022856601771338794256383410400076210073482253089544155377");
        bigint c = a * b;
        bigint expected = bigint::from_string("55409001364124857587521411422210474638575227552776047085010157327559891765943209923363557763419730972781537530948429788352886919273214132899346769031695550850320602049507618052164677667378189154076988316301237199538599598044906690917691500474146296751848053320011822980888915807952984822080201739642211593661864443811046346990267512628848918282");

        CHECK(expected == (c));
    }

    SECTION("div")
    {
        bigint a = bigint::from_string("43359329001059048967113581928625959342654930666632400867978208429224464941890190751598229305718587280094852374898291386268967561418738958337632249177044975686477011571044266");
        bigint b = bigint::from_string("1277902648419017187919156692641295109476255233737630537760832794503886212911067061184379695097643279217271150419129022856601771338794256383410400076210073482253089544155377");
        bigint c = a / b;
        bigint expected = bigint::from_string("33");

        CHECK(expected == (c));
    }

    SECTION("&=")
    {
        bigint a{0};
        bigint b = bigint::from_string("1277902648419017187919156692641295109476255233737630537760832794503886212911067061184379695097643279217271150419129022856601771338794256383410400076210073482253089544155377");
        b &= a;

        CHECK(a == b);
    }

    SECTION("|=")
    {
        bigint a{0};
        bigint b = bigint::from_string("1277902648419017187919156692641295109476255233737630537760832794503886212911067061184379695097643279217271150419129022856601771338794256383410400076210073482253089544155377");
        bigint expected = b;
        b |= a;

        CHECK(expected == b);
    }
}

