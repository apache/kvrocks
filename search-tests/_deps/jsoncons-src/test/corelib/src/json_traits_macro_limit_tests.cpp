// Copyright 2022 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <cstdint>
#include <catch/catch.hpp>

using namespace jsoncons;

namespace {
    namespace ns {

    struct MyStruct
    {
        std::string s1;
        std::string s2;
        std::string s3;
        std::string s4;
        std::string s5;
        std::string s6;
        std::string s7;
        std::string s8;
        std::string s9;
        std::string s10;
        std::string s11;
        std::string s12;
        std::string s13;
        std::string s14;
        std::string s15;
        std::string s16;
        std::string s17;
        std::string s18;
        std::string s19;
        std::string s20;
        std::string s21;
        std::string s22;
        std::string s23;
        std::string s24;
        std::string s25;
        std::string s26;
        std::string s27;
        std::string s28;
        std::string s29;
        std::string s30;
        std::string s31;
        std::string s32;
        std::string s33;
        std::string s34;
        std::string s35;
        std::string s36;
        std::string s37;
        std::string s38;
        std::string s39;
        std::string s40;
        std::string s41;
        std::string s42;
        std::string s43;
        std::string s44;
        std::string s45;
        std::string s46;
        std::string s47;
        std::string s48;
        std::string s49;
        std::string s50;
        std::string s51;
        std::string s52;
        std::string s53;
        std::string s54;
        std::string s55;
        std::string s56;
        std::string s57;
        std::string s58;
        std::string s59;
        std::string s60;
        std::string s61;
        std::string s62;
        std::string s63;
        std::string s64;
        std::string s65;
        std::string s66;
        std::string s67;
        std::string s68;
        std::string s69;
        std::string s70;
    };

    bool operator==(const MyStruct& lhs, const MyStruct& rhs)
    {
        return (lhs.s1 == rhs.s1 &&
                lhs.s2 == rhs.s2 &&
                lhs.s3 == rhs.s3 &&
                lhs.s4 == rhs.s4 &&
                lhs.s5 == rhs.s5 &&
                lhs.s6 == rhs.s6 &&
                lhs.s7 == rhs.s7 &&
                lhs.s8 == rhs.s8 &&
                lhs.s9 == rhs.s9 &&
                lhs.s10 == rhs.s10 &&
                lhs.s11 == rhs.s11 &&
                lhs.s12 == rhs.s12 &&
                lhs.s13 == rhs.s13 &&
                lhs.s14 == rhs.s14 &&
                lhs.s15 == rhs.s15 &&
                lhs.s16 == rhs.s16 &&
                lhs.s17 == rhs.s17 &&
                lhs.s18 == rhs.s18 &&
                lhs.s19 == rhs.s19 &&
                lhs.s20 == rhs.s20 &&
                lhs.s21 == rhs.s21 &&
                lhs.s22 == rhs.s22 &&
                lhs.s23 == rhs.s23 &&
                lhs.s24 == rhs.s24 &&
                lhs.s25 == rhs.s25 &&
                lhs.s26 == rhs.s26 &&
                lhs.s27 == rhs.s27 &&
                lhs.s28 == rhs.s28 &&
                lhs.s29 == rhs.s29 &&
                lhs.s30 == rhs.s30 &&
                lhs.s31 == rhs.s31 &&
                lhs.s32 == rhs.s32 &&
                lhs.s33 == rhs.s33 &&
                lhs.s34 == rhs.s34 &&
                lhs.s35 == rhs.s35 &&
                lhs.s36 == rhs.s36 &&
                lhs.s37 == rhs.s37 &&
                lhs.s38 == rhs.s38 &&
                lhs.s39 == rhs.s39 &&
                lhs.s40 == rhs.s40 &&
                lhs.s41 == rhs.s41 &&
                lhs.s42 == rhs.s42 &&
                lhs.s43 == rhs.s43 &&
                lhs.s44 == rhs.s44 &&
                lhs.s45 == rhs.s45 &&
                lhs.s46 == rhs.s46 &&
                lhs.s47 == rhs.s47 &&
                lhs.s48 == rhs.s48 &&
                lhs.s49 == rhs.s49 &&
                lhs.s50 == rhs.s50 &&
                lhs.s51 == rhs.s51 &&
                lhs.s52 == rhs.s52 &&
                lhs.s53 == rhs.s53 &&
                lhs.s54 == rhs.s54 &&
                lhs.s55 == rhs.s55 &&
                lhs.s56 == rhs.s56 &&
                lhs.s57 == rhs.s57 &&
                lhs.s58 == rhs.s58 &&
                lhs.s59 == rhs.s59 &&
                lhs.s60 == rhs.s60 &&
                lhs.s61 == rhs.s61 &&
                lhs.s62 == rhs.s62 &&
                lhs.s63 == rhs.s63 &&
                lhs.s64 == rhs.s64 &&
                lhs.s65 == rhs.s65 &&
                lhs.s66 == rhs.s66 &&
                lhs.s67 == rhs.s67 &&
                lhs.s68 == rhs.s68 &&
                lhs.s69 == rhs.s69 &&
                lhs.s70 == rhs.s70);
    }

} // namespace ns
} // namespace 
 

JSONCONS_ALL_MEMBER_TRAITS(ns::MyStruct, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,
                                         s11, s12, s13, s14, s15, s16, s17, s18, s19, s20,
                                         s21, s22, s23, s24, s25, s26, s27, s28, s29, s30,
                                         s31, s32, s33, s34, s35, s36, s37, s38, s39, s40,
                                         s41, s42, s43, s44, s45, s46, s47, s48, s49, s50,
                                         s51, s52, s53, s54, s55, s56, s57, s58, s59, s60,
                                         s61, s62, s63, s64, s65, s66, s67, s68, s69, s70)

TEST_CASE("json traits limits tests")
{
    ns::MyStruct val = {"1","2","3","4","5","6","7","8","9","10",
        "11","12","13","14","15","16","17","18","19","20",
        "21","22","23","24","25","26","27","28","29","30",
        "31","32","33","34","35","36","37","38","39","40",
        "41","42","43","44","45","46","47","48","49","50",
        "51","52","53","54","55","56","57","58","59","60",
        "61","62","63","64","65","66","67","68","69","70"};

    SECTION("test1")
    {
        std::string buf;
        jsoncons::encode_json(val,buf);

        ns::MyStruct val2 = jsoncons::decode_json<ns::MyStruct>(buf);

        CHECK(val == val2);
    }
}

