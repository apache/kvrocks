// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>

#include <map>
#include <vector>

#include <catch/catch.hpp>

namespace 
{
    class MyIterator
    {
        const char* p_;
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = char;
        using difference_type = std::ptrdiff_t;
        using pointer = const char*; 
        using reference = const char&;

        MyIterator(const char* p)
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

using namespace jsoncons;

TEST_CASE("encode and decode json")
{
    json j(std::make_pair(false,std::string("foo")));

    SECTION("string source")
    {
        std::string s;
        encode_json(j, s);
        json result = decode_json<json>(s);
        CHECK(result == j);
    }

    SECTION("stream source")
    {
        std::stringstream ss;
        encode_json(j, ss);
        json result = decode_json<json>(ss);
        CHECK(result == j);
    }

    SECTION("iterator source")
    {
        std::string s;
        encode_json(j, s);
        json result = decode_json<json>(s.begin(), s.end());
        CHECK(result == j);
    }
}

TEST_CASE("encode and decode wjson")
{
    wjson j(std::make_pair(false,std::wstring(L"foo")));

    SECTION("string source")
    {
        std::wstring s;
        encode_json(j, s);
        wjson result = decode_json<wjson>(s);
        CHECK(result == j);
    }

    SECTION("stream source")
    {
        std::wstringstream ss;
        encode_json(j, ss);
        wjson result = decode_json<wjson>(ss);
        CHECK(result == j);
    }

    SECTION("iterator source")
    {
        std::wstring s;
        encode_json(j, s);
        wjson result = decode_json<wjson>(s.begin(), s.end());
        CHECK(result == j);
    }
}

TEST_CASE("convert_pair_test")
{
    auto val = std::make_pair(false,std::string("foo"));
    std::string s;

    jsoncons::encode_json(val, s);

    auto result = jsoncons::decode_json<std::pair<bool,std::string>>(s);

    CHECK(val == result);
}

TEST_CASE("convert_vector_test")
{
    std::vector<double> v = {1,2,3,4,5,6};

    std::string s;
    jsoncons::encode_json(v,s);

    auto result = jsoncons::decode_json<std::vector<double>>(s);

    REQUIRE(v.size() == result.size());
    for (std::size_t i = 0; i < result.size(); ++i)
    {
        CHECK(v[i] == result[i]);
    }
}

TEST_CASE("convert_map_test")
{
    std::map<std::string,double> m = {{"a",1},{"b",2}};

    std::string s;
    jsoncons::encode_json(m,s);
    auto result = jsoncons::decode_json<std::map<std::string,double>>(s);

    REQUIRE(result.size() == m.size());
    CHECK(m["a"] == result["a"]);
    CHECK(m["b"] == result["b"]);
}

TEST_CASE("convert_array_test")
{
    std::array<double,4> v{{1,2,3,4}};

    std::string s;
    jsoncons::encode_json(v,s);
    std::array<double, 4> result = jsoncons::decode_json<std::array<double,4>>(s);
    REQUIRE(result.size() == v.size());
    for (std::size_t i = 0; i < result.size(); ++i)
    {
        CHECK(v[i] == result[i]);
    }
}

TEST_CASE("convert vector of vector test")
{
    std::vector<double> u{1,2,3,4};
    std::vector<std::vector<double>> v{u,u};

    std::string s;
    jsoncons::encode_json(v,s);
    auto result = jsoncons::decode_json<std::vector<std::vector<double>>>(s);
    REQUIRE(result.size() == v.size());
    for (const auto& item : result)
    {
        REQUIRE(item.size() == u.size());
        CHECK(item[0] == 1);
        CHECK(item[1] == 2);
        CHECK(item[2] == 3);
        CHECK(item[3] == 4);
    }
}

#if !(defined(__GNUC__) && __GNUC__ <= 5)
TEST_CASE("convert_tuple_test")
{
    using employee_collection = std::map<std::string,std::tuple<std::string,std::string,double>>;

    employee_collection input = 
    { 
        {"John Smith",{"Hourly","Software Engineer",10000}},
        {"Jane Doe",{"Commission","Sales",20000}}
    };

    std::string s;
    jsoncons::encode_json(input, s, indenting::indent);

    json j = json::parse(s);
    REQUIRE(j.is_object());
    REQUIRE(j.size() == 2);
    CHECK(j.contains("John Smith"));
    CHECK(j.contains("Jane Doe"));

    auto employees2 = jsoncons::decode_json<employee_collection>(s);
    REQUIRE(employees2.size() == input.size());
    CHECK(employees2 == input);

}

#endif

TEST_CASE("encode/decode map with integer key")
{
    std::map<int,double> m = {{1,1},{2,2}};

    SECTION("string source")
    {
        std::string s;
        jsoncons::encode_json(m,s);
        auto result = jsoncons::decode_json<std::map<int,double>>(s);

        REQUIRE(result.size() == m.size());
        CHECK(m[1] == result[1]);
        CHECK(m[2] == result[2]);
    }

    SECTION("stream source")
    {
        std::string s;
        jsoncons::encode_json(m,s);
        std::stringstream is(s);

        auto result = jsoncons::decode_json<std::map<int,double>>(is);

        REQUIRE(result.size() == m.size());
        CHECK(m[1] == result[1]);
        CHECK(m[2] == result[2]);
    }

    SECTION("iterator source")
    {
        std::string s;
        jsoncons::encode_json(m,s);

        auto result = jsoncons::decode_json<std::map<int,double>>(s.begin(), s.end());

        REQUIRE(result.size() == m.size());
        CHECK(m[1] == result[1]);
        CHECK(m[2] == result[2]);
    }

    SECTION("custom iterator source")
    {
        std::string s;
        jsoncons::encode_json(m,s);

        MyIterator it(s.data());
        MyIterator end(s.data() + s.length());

        auto result = jsoncons::decode_json<std::map<int,double>>(it, end);

        REQUIRE(result.size() == m.size());
        CHECK(m[1] == result[1]);
        CHECK(m[2] == result[2]);
    }
}

#if defined(JSONCONS_HAS_STATEFUL_ALLOCATOR) && JSONCONS_HAS_STATEFUL_ALLOCATOR == 1

#include <scoped_allocator>
#include <common/free_list_allocator.hpp>

template <typename T>
using MyScopedAllocator = std::scoped_allocator_adaptor<free_list_allocator<T>>;

TEST_CASE("decode_json with work allocator")
{
    MyScopedAllocator<char> temp_alloc(1);

    auto alloc_set = temp_allocator_only(temp_alloc);

    SECTION("convert_vector_test")
    {
        std::vector<double> v = {1,2,3,4,5,6};

        std::string json_text;
        jsoncons::encode_json(alloc_set, v,json_text);

        auto result = jsoncons::decode_json<std::vector<double>>(alloc_set, json_text);

        REQUIRE(v.size() == result.size());
        for (std::size_t i = 0; i < result.size(); ++i)
        {
            CHECK(v[i] == result[i]);
        }
    }

    SECTION("convert_map_test")
    {
        std::map<std::string,double> m = {{"a",1},{"b",2}};

        std::string json_text;
        jsoncons::encode_json(alloc_set, m,json_text);
        auto result = jsoncons::decode_json<std::map<std::string,double>>(alloc_set, json_text);
        REQUIRE(result.size() == m.size());
        CHECK(m["a"] == result["a"]);
        CHECK(m["b"] == result["b"]);
    }

    SECTION("convert vector of vector test")
    {
        std::vector<double> u{1,2,3,4};
        std::vector<std::vector<double>> v{u,u};

        std::string json_text;
        jsoncons::encode_json(alloc_set, v,json_text);
        auto result = jsoncons::decode_json<std::vector<std::vector<double>>>(alloc_set, json_text);
        REQUIRE(result.size() == v.size());
        for (const auto& item : result)
        {
            REQUIRE(item.size() == u.size());
            CHECK(item[0] == 1);
            CHECK(item[1] == 2);
            CHECK(item[2] == 3);
            CHECK(item[3] == 4);
        }
    }

    #if !(defined(__GNUC__) && __GNUC__ <= 5)

    SECTION("convert_tuple_test")
    {
        using employee_collection = std::map<std::string,std::tuple<std::string,std::string,double>>;

        employee_collection employees = 
        { 
            {"John Smith",{"Hourly","Software Engineer",10000}},
            {"Jane Doe",{"Commission","Sales",20000}}
        };

        std::string json_text;
        jsoncons::encode_json(alloc_set, employees, json_text, json_options(), indenting::indent);
        auto employees2 = jsoncons::decode_json<employee_collection>(alloc_set, json_text);
        REQUIRE(employees2.size() == employees.size());
        CHECK(employees2 == employees);
    }

    #endif
}

#endif