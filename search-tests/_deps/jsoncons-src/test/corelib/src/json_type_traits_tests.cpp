// Copyright 2013-2025 Daniel Parker
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
// own vector will always be of an even length 
struct own_vector : std::vector<std::int64_t> { using  std::vector<int64_t>::vector; };

namespace jsoncons {
template <typename Json>
struct json_type_traits<Json, own_vector> {
    static bool is(const Json&) noexcept { return true; }
    static own_vector as(const Json&) { return own_vector(); }
    static Json to_json(const own_vector& val) {
        Json j;
        for (uint64_t i = 0; i<val.size(); i = i + 2) {
            j[std::to_string(val[i])] = val[i + 1];
        }
        return j;
    }
};
}

TEST_CASE("test_trait_type_erasure")
{
    json::object o;

    json val;

    val = o;

    val.insert_or_assign("A",o);
}

TEST_CASE("test_assign_non_const_cstring")
{
    json root;

    const char* p = "A string";
    char* q = const_cast<char*>(p);

    root["Test"] = q;
}

TEST_CASE("test_uint8_t")
{
    uint8_t x = 10;

    json o;
    o["u"] = x;

    CHECK(o["u"].is_number());

    uint8_t y = o["u"].as<uint8_t>();

    CHECK(y == 10);
}

TEST_CASE("test_float_assignment")
{
    float x = 10.5;

    json o;
    o["float"] = x;

    CHECK(o["float"].is_number());

    float y = o["float"].as<float>();

    CHECK(10.5 == Approx(y).epsilon(0.00001));
}

TEST_CASE("test_float")
{
    float x = 10.5;

    json o(x);

    CHECK(o.is<float>());

    float y = o.as<float>();

    CHECK(10.5 == Approx(y).epsilon(0.00001));
}

TEST_CASE("test_unsupported_type")
{
    json o;

    //o["u"] = Info; 
    // compile error
}

TEST_CASE("test_as_json_value")
{
    json a;

    a["first"] = "first"; 
    a["second"] = "second"; 

    CHECK(true == a.is<json>());
    
    json b = a.as<json>();
    CHECK("first" == b["first"].as<std::string>());
    CHECK("second" == b["second"].as<std::string>());
}

TEST_CASE("test_byte_string_as_vector")
{
    json a(byte_string{'H','e','l','l','o'});

    REQUIRE(a.is_byte_string());

    auto bytes = a.as<byte_string>();

    REQUIRE(5 == bytes.size());
    CHECK('H' == bytes[0]);
    CHECK('e' == bytes[1]);
    CHECK('l' == bytes[2]);
    CHECK('l' == bytes[3]);
    CHECK('o' == bytes[4]);
}

TEST_CASE("jsoncons::json_type_traits<optional>")
{
    std::vector<jsoncons::optional<int>> v = { 0,1,jsoncons::optional<int>{} };
    json j = v;

    REQUIRE(j.size() == 3);
    CHECK(j[0].as<int>() == 0);
    CHECK(j[1].as<int>() == 1);
    CHECK(j[2].is_null());

    CHECK(j[0].is<jsoncons::optional<int>>());
    CHECK_FALSE(j[0].is<jsoncons::optional<double>>());
    CHECK(j[1].is<jsoncons::optional<int>>());
    CHECK_FALSE(j[1].is<jsoncons::optional<double>>());
    CHECK(j[2].is<jsoncons::optional<int>>()); // null can be any optinal
}

TEST_CASE("jsoncons::json_type_traits<shared_ptr>")
{
    std::vector<std::shared_ptr<std::string>> v = {std::make_shared<std::string>("Hello"), 
                                                   std::make_shared<std::string>("World"),
                                                   std::shared_ptr<std::string>()};
    json j{v};

    REQUIRE(j.size() == 3);
    CHECK(j[0].as<std::string>() == std::string("Hello"));
    CHECK(j[1].as<std::string>() == std::string("World"));
    CHECK(j[2].is_null());

    CHECK(j[0].is<std::shared_ptr<std::string>>());
    CHECK_FALSE(j[0].is<std::shared_ptr<int>>());
    CHECK(j[1].is<std::shared_ptr<std::string>>());
    CHECK_FALSE(j[1].is<std::shared_ptr<int>>());
    CHECK(j[2].is<std::shared_ptr<std::string>>());
}

TEST_CASE("jsoncons::json_type_traits<unique_ptr>")
{
    std::vector<std::unique_ptr<std::string>> v;
    
    v.emplace_back(jsoncons::make_unique<std::string>("Hello"));
    v.emplace_back(jsoncons::make_unique<std::string>("World"));
    v.emplace_back(std::unique_ptr<std::string>());

    json j{ v };

    REQUIRE(j.size() == 3);
    CHECK(j[0].as<std::string>() == std::string("Hello"));
    CHECK(j[1].as<std::string>() == std::string("World"));
    CHECK(j[2].is_null());

    CHECK(j[0].is<std::unique_ptr<std::string>>());
    CHECK_FALSE(j[0].is<std::unique_ptr<int>>());
    CHECK(j[1].is<std::unique_ptr<std::string>>());
    CHECK_FALSE(j[1].is<std::unique_ptr<int>>());
    CHECK(j[2].is<std::unique_ptr<std::string>>());
}
/*
TEST_CASE("test_own_vector")
{
    jsoncons::json j = own_vector({0,9,8,7});
    std::cout << j;
}
*/

#if defined(JSONCONS_HAS_STD_VARIANT)

namespace { namespace ns {

    enum class Color {yellow, red, green, blue};

    inline
    std::ostream& operator<<(std::ostream& os, Color val)
    {
        switch (val)
        {
            case Color::yellow: os << "yellow"; break;
            case Color::red: os << "red"; break;
            case Color::green: os << "green"; break;
            case Color::blue: os << "blue"; break;
        }
        return os;
    }

    class Fruit 
    {
    private:
        JSONCONS_TYPE_TRAITS_FRIEND
        std::string name_;
        Color color_;
    public:
    };

    class Fabric 
    {
    private:
      JSONCONS_TYPE_TRAITS_FRIEND
      int size_;
      std::string material_;
    public:
    };

    class Basket {
     private:
      JSONCONS_TYPE_TRAITS_FRIEND
      std::string owner_;
      std::vector<std::variant<Fruit, Fabric>> items_;

    public:
        std::string owner() const
        {
            return owner_;
        }

        std::vector<std::variant<Fruit, Fabric>> items() const
        {
            return items_;
        }
    };

} // ns
} // namespace

JSONCONS_ENUM_NAME_TRAITS(ns::Color, (yellow, "YELLOW"), (red, "RED"), (green, "GREEN"), (blue, "BLUE"))

JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Fruit,
                                (name_, "name"),
                                (color_, "color"))
JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Fabric,
                                (size_, "size"),
                                (material_, "material"))
JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Basket,
                                (owner_, "owner"),
                                (items_, "items"))

TEST_CASE("json_type_traits for std::variant")
{
    std::string input = R"(
{
  "owner": "Rodrigo",
  "items": [
    {
      "name": "banana",
      "color": "YELLOW"
    },
    {
      "size": 40,
      "material": "wool"
    },
    {
      "name": "apple",
      "color": "RED"
    },
    {
      "size": 40,
      "material": "cotton"
    }
  ]
}
    )";

    SECTION("test decode and encode")
    {
        ns::Basket basket = decode_json<ns::Basket>(input);

        std::string output;
        encode_json(basket, output, indenting::indent);

        json j1 = json::parse(input);
        json j2 = json::parse(output);
        CHECK(j1 == j2);
    }

    SECTION("std::variant<int, double, bool, std::string, ns::Color> test")
    {
        using variant_type  = std::variant<int, double, bool, std::string, ns::Color>;

        variant_type var1(100);
        variant_type var2(10.1);
        variant_type var3(false);
        variant_type var4(std::string("Hello World"));
        variant_type var5(ns::Color::yellow);

        std::string buffer1;
        jsoncons::encode_json(var1,buffer1);
        std::string buffer2;
        jsoncons::encode_json(var2,buffer2);
        std::string buffer3;
        jsoncons::encode_json(var3,buffer3);
        std::string buffer4;
        jsoncons::encode_json(var4,buffer4);
        std::string buffer5;
        jsoncons::encode_json(var5,buffer5);

        auto v1 = jsoncons::decode_json<variant_type>(buffer1);
        auto v2 = jsoncons::decode_json<variant_type>(buffer2);
        auto v3 = jsoncons::decode_json<variant_type>(buffer3);
        auto v4 = jsoncons::decode_json<variant_type>(buffer4);
        auto v5 = jsoncons::decode_json<variant_type>(buffer5);

        CHECK(v1.index() == 0);
        CHECK(v2.index() == 1);
        CHECK(v3.index() == 2);
        CHECK(v4.index() == 3);
        CHECK(v5.index() == 3);

        CHECK(std::get<0>(v1) == 100);
        CHECK(std::get<1>(v2) == 10.1);
        CHECK(std::get<2>(v3) == false);
        CHECK(std::get<3>(v4) == std::string("Hello World"));
        CHECK(std::get<3>(v5) == std::string("YELLOW"));
    }

    SECTION("std::variant<int, double, bool, ns::Color, std::string> test")
    {
        using variant_type  = std::variant<int, double, bool, ns::Color, std::string>;

        variant_type var1(100);
        variant_type var2(10.1);
        variant_type var3(false);
        variant_type var4(std::string("Hello World"));
        variant_type var5(ns::Color::yellow);

        std::string buffer1;
        jsoncons::encode_json(var1,buffer1);
        std::string buffer2;
        jsoncons::encode_json(var2,buffer2);
        std::string buffer3;
        jsoncons::encode_json(var3,buffer3);
        std::string buffer4;
        jsoncons::encode_json(var4,buffer4);
        std::string buffer5;
        jsoncons::encode_json(var5,buffer5);

        auto v1 = jsoncons::decode_json<variant_type>(buffer1);
        auto v2 = jsoncons::decode_json<variant_type>(buffer2);
        auto v3 = jsoncons::decode_json<variant_type>(buffer3);
        auto v4 = jsoncons::decode_json<variant_type>(buffer4);
        auto v5 = jsoncons::decode_json<variant_type>(buffer5);

        CHECK(v1.index() == 0);
        CHECK(v2.index() == 1);
        CHECK(v3.index() == 2);
        CHECK(v4.index() == 4);
        CHECK(v5.index() == 3);

        CHECK(std::get<0>(v1) == 100);
        CHECK(std::get<1>(v2) == 10.1);
        CHECK(std::get<2>(v3) == false);
        CHECK(std::get<4>(v4) == std::string("Hello World"));
        CHECK(std::get<3>(v5) == ns::Color::yellow);
    }
}

#endif
