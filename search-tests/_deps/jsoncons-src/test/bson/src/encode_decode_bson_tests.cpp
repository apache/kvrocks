// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/bson/bson.hpp>
#include <jsoncons/json.hpp>
#include <vector>
#include <catch/catch.hpp>

using namespace jsoncons;

namespace 
{
    class MyIterator
    {
        const uint8_t* p_;
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = uint8_t;
        using difference_type = std::ptrdiff_t;
        using pointer = const uint8_t*; 
        using reference = const uint8_t&;

        MyIterator(const uint8_t* p)
            : p_(p)
        {
        }

        reference operator*() const
        {
            return *p_;
        }

        pointer operator->() const 
        {
            return p_;
        }

        MyIterator& operator++()
        {
            ++p_;
            return *this;
        }

        MyIterator operator++(int) 
        {
            MyIterator temp(*this);
            ++*this;
            return temp;
        }

        bool operator!=(const MyIterator& rhs) const
        {
            return p_ != rhs.p_;
        }
    };

} // namespace

TEST_CASE("encode decode bson source uint8_t")
{
    std::vector<uint8_t> input = {0x27,0x00,0x00,0x00, // Total number of bytes comprising the document (40 bytes) 
            0x02, // URF-8 string
                0x48,0x65,0x6c,0x6c,0x6f, // Hello
                0x00, // trailing byte 
            0x06,0x00,0x00,0x00, // Number bytes in string (including trailing byte)
                0x57,0x6f,0x72,0x6c,0x64, // World
                0x00, // trailing byte
            0x05, // binary
                0x44,0x61,0x74,0x61, // Data
                0x00, // trailing byte
            0x06,0x00,0x00,0x00, // number of bytes
                0x80, // subtype
                0x66,0x6f,0x6f,0x62,0x61,0x72,
            0x00};

    SECTION("from bytes")
    {
        ojson j = bson::decode_bson<ojson>(input);

        std::vector<uint8_t> buffer;
        bson::encode_bson(j, buffer);
        CHECK(buffer == input);
    }

    SECTION("from stream")
    {
        std::string s(reinterpret_cast<const char*>(input.data()), input.size());
        std::stringstream is(std::move(s));

        ojson j = bson::decode_bson<ojson>(is);

        std::vector<uint8_t> buffer;
        bson::encode_bson(j, buffer);
        CHECK(buffer == input);
    }

    SECTION("from iterator source")
    {
        ojson j = bson::decode_bson<ojson>(input.begin(), input.end());

        std::vector<uint8_t> buffer;
        bson::encode_bson(j, buffer);
        CHECK(buffer == input);
    }

    SECTION("from custom iterator source")
    {
        MyIterator it(input.data());
        MyIterator end(input.data() + input.size());

        ojson j = bson::decode_bson<ojson>(it, end);

        std::vector<uint8_t> buffer;
        bson::encode_bson(j, buffer);
        CHECK(buffer == input);
    }
}

namespace { namespace ns {

    struct Person
    {
        std::string name;
    };

}}

JSONCONS_ALL_MEMBER_TRAITS(ns::Person, name)

TEST_CASE("encode_bson overloads")
{
    SECTION("json, stream")
    {
        json person;
        person.try_emplace("name", "John Smith");

        std::string s;
        std::stringstream ss(s);
        bson::encode_bson(person, ss);
        json other = bson::decode_bson<json>(ss);
        CHECK(other == person);
    }
    SECTION("custom, stream")
    {
        ns::Person person{"John Smith"};

        std::string s;
        std::stringstream ss(s);
        bson::encode_bson(person, ss);
        ns::Person other = bson::decode_bson<ns::Person>(ss);
        CHECK(other.name == person.name);
    }
}

TEST_CASE("bson encode array")
{
    SECTION("test1")
    {
        std::vector<uint8_t> expected = {0x13,0x00,0x00,0x00,0x10,0x30,0x00,0x01,0x00,0x00,0x00,0x10,0x31,0x00,0x02,0x00,0x00,0x00,0x00};
        /*
        13,00,00,00, // document has 19 bytes
        10,30,00, // "0"
        01,00,00,00, // 1
        10,31,00, // "1"
        02,00,00,00, // 2
        00 // terminating null
        */

        std::pair<int,int> p{1,2};

        std::vector<uint8_t> data;
        bson::encode_bson(p, data);

        CHECK(expected == data);

        auto p2 = bson::decode_bson<std::pair<int,int>>(data);
        CHECK(p2 == p);
    }
}

#if defined(JSONCONS_HAS_STATEFUL_ALLOCATOR) && JSONCONS_HAS_STATEFUL_ALLOCATOR == 1

#include <common/free_list_allocator.hpp>
#include <scoped_allocator>

template <typename T>
using MyScopedAllocator = std::scoped_allocator_adaptor<free_list_allocator<T>>;
using cust_json = basic_json<char,sorted_policy,MyScopedAllocator<char>>;

TEST_CASE("encode decode bson source with temp_allocator")
{
    std::vector<uint8_t> input = {0x27,0x00,0x00,0x00, // Total number of bytes comprising the document (40 bytes) 
            0x02, // URF-8 string
                0x48,0x65,0x6c,0x6c,0x6f, // Hello
                0x00, // trailing byte 
            0x06,0x00,0x00,0x00, // Number bytes in string (including trailing byte)
                0x57,0x6f,0x72,0x6c,0x64, // World
                0x00, // trailing byte
            0x05, // binary
                0x44,0x61,0x74,0x61, // Data
                0x00, // trailing byte
            0x06,0x00,0x00,0x00, // number of bytes
                0x80, // subtype
                0x66,0x6f,0x6f,0x62,0x61,0x72,
            0x00};

    MyScopedAllocator<char> temp_alloc(2);
    auto alloc_set = temp_allocator_only(temp_alloc);    

    SECTION("from bytes")
    {
        auto j = bson::decode_bson<ojson>(alloc_set, input);

        std::vector<uint8_t> buffer;
        bson::encode_bson(alloc_set, j, buffer);
        CHECK(buffer == input);
    }

    SECTION("from stream")
    {
        std::string s(reinterpret_cast<const char*>(input.data()), input.size());
        std::stringstream is(std::move(s));

        auto j = bson::decode_bson<ojson>(alloc_set, is);

        std::vector<uint8_t> buffer;
        bson::encode_bson(alloc_set, j, buffer);
        CHECK(buffer == input);
    }

}

#endif
