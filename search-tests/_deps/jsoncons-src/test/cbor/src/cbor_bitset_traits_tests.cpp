// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons/json.hpp>

#include <random>
#include <common/test_utilities.hpp>

#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("CBOR std::bitset tests")
{
    SECTION("low test")
    {
        std::bitset<32> i_bs32(0);
        std::string s32;
        cbor::encode_cbor(i_bs32, s32);
        auto o_bs32 = cbor::decode_cbor<std::bitset<32>>(s32);
        CHECK(o_bs32 == i_bs32);

        std::bitset<64> i_bs64(0);
        std::string s64;
        cbor::encode_cbor(i_bs64, s64);
        auto o_bs64 = cbor::decode_cbor<std::bitset<64>>(s64);
        CHECK(o_bs64 == i_bs64);
    }

    SECTION("high test")
    {
        std::bitset<32> i_bs32(0xffffffff);
        std::string s32;
        cbor::encode_cbor(i_bs32, s32);
        auto o_bs32 = cbor::decode_cbor<std::bitset<32>>(s32);
        CHECK(o_bs32 == i_bs32);

        std::bitset<64> i_bs64(0xffffffffffffffff);
        std::string s64;
        cbor::encode_cbor(i_bs64, s64);
        auto o_bs64 = cbor::decode_cbor<std::bitset<64>>(s64);
        CHECK(o_bs64 == i_bs64);
    }

    SECTION("random test")
    {
        std::random_device rd;
        std::mt19937 gen(rd());

        auto rng32  = [&](){return random_binary_string(gen, 32);};
        auto rng65  = [&](){return random_binary_string(gen, 65);};
        auto rng128 = [&]() {return random_binary_string(gen, 128); };
        auto rng129 = [&]() {return random_binary_string(gen, 129); };
        auto rng256 = [&]() {return random_binary_string(gen, 256); };
        auto rng257 = [&]() {return random_binary_string(gen, 257); };
        auto rng512 = [&](){return random_binary_string(gen, 512);};
        auto rng513 = [&](){return random_binary_string(gen, 513);};

        for (std::size_t i = 0; i < 100; ++i)
        {
            std::bitset<32> i_bs32(rng32());
            std::string s32;
            cbor::encode_cbor(i_bs32, s32);
            auto o_bs32 = cbor::decode_cbor<std::bitset<32>>(s32);
            CHECK(o_bs32 == i_bs32);

            std::bitset<65> i_bs65(rng65());
            std::string s65;
            cbor::encode_cbor(i_bs65, s65);
            auto o_bs65 = cbor::decode_cbor<std::bitset<65>>(s65);
            CHECK(o_bs65 == i_bs65);

            std::bitset<128> i_bs128(rng128());
            std::string s128;
            cbor::encode_cbor(i_bs128, s128);
            auto o_bs128 = cbor::decode_cbor<std::bitset<128>>(s128);
            CHECK(o_bs128 == i_bs128);

            std::bitset<129> i_bs129(rng129());
            std::string s129;
            cbor::encode_cbor(i_bs129, s129);
            auto o_bs129 = cbor::decode_cbor<std::bitset<129>>(s129);
            CHECK(o_bs129 == i_bs129);

            std::bitset<256> i_bs256(rng256());
            std::string s256;
            cbor::encode_cbor(i_bs256, s256);
            auto o_bs256 = cbor::decode_cbor<std::bitset<256>>(s256);
            CHECK(o_bs256 == i_bs256);

            std::bitset<257> i_bs257(rng257());
            std::string s257;
            cbor::encode_cbor(i_bs257, s257);
            auto o_bs257 = cbor::decode_cbor<std::bitset<257>>(s257);
            CHECK(o_bs257 == i_bs257);

            std::bitset<512> i_bs512(rng512());
            std::string s512;
            cbor::encode_cbor(i_bs512, s512);
            auto o_bs512 = cbor::decode_cbor<std::bitset<512>>(s512);
            CHECK(o_bs512 == i_bs512);

            std::bitset<513> i_bs513(rng513());
            std::string s513;
            cbor::encode_cbor(i_bs513, s513);
            auto o_bs513 = cbor::decode_cbor<std::bitset<513>>(s513);
            CHECK(o_bs513 == i_bs513);
        }
    }
}

