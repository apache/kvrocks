// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>

#include <catch/catch.hpp>

#if defined(JSONCONS_HAS_POLYMORPHIC_ALLOCATOR) && JSONCONS_HAS_POLYMORPHIC_ALLOCATOR == 1
#include <memory_resource> 
using namespace jsoncons;

using pmr_json = jsoncons::pmr::json;
using pmr_ojson = jsoncons::pmr::ojson;
    
TEST_CASE("string polymorhic allocator tests")
{
    char buffer1[1024] = {}; // a small buffer on the stack
    memset(buffer1, 0, sizeof(buffer1));
    std::pmr::monotonic_buffer_resource pool1{ std::data(buffer1), std::size(buffer1) };
    std::pmr::polymorphic_allocator<char> alloc1(&pool1);

    char buffer2[1024] = {}; // a small buffer on the stack
    std::pmr::monotonic_buffer_resource pool2{ std::data(buffer2), std::size(buffer2) };
    std::pmr::polymorphic_allocator<char> alloc2(&pool2);

    const char* long_string1 = "String too long for short string";

    CHECK_FALSE(std::allocator_traits<std::pmr::polymorphic_allocator<char>>::is_always_equal::value);
    CHECK_FALSE(alloc1 == alloc2);
    CHECK(alloc1 == alloc1);

    SECTION("construct string")
    {

        std::cout << "Polymorphic Allocator\n\n";

        std::cout << "propagate_on_container_copy_assignment: " << std::allocator_traits<std::pmr::polymorphic_allocator<char>>::propagate_on_container_copy_assignment::value << "\n";
        std::cout << "propagate_on_container_move_assignment: " << std::allocator_traits<std::pmr::polymorphic_allocator<char>>::propagate_on_container_move_assignment::value << "\n";
        std::cout << "propagate_on_container_swap: " << std::allocator_traits<std::pmr::polymorphic_allocator<char>>::propagate_on_container_swap::value << "\n";

        pmr_json j1(long_string1, alloc1);
        pmr_json j2(j1, alloc2);

        CHECK(j1.as<std::string>() == long_string1);
        CHECK(j2.as<std::string>() == long_string1);
        CHECK(j1.cast<pmr_json::long_string_storage>().get_allocator() == alloc1);
        CHECK(j2.cast<pmr_json::long_string_storage>().get_allocator() == alloc2);
        CHECK_FALSE(j1.cast<pmr_json::long_string_storage>().get_allocator() ==
            j2.cast<pmr_json::long_string_storage>().get_allocator());
    }
}

TEST_CASE("Test polymorhic allocator")
{
    char buffer1[1024] = {}; // a small buffer1 on the stack
    std::pmr::monotonic_buffer_resource pool1{ std::data(buffer1), std::size(buffer1) };
    std::pmr::polymorphic_allocator<char> alloc1(&pool1);

    char buffer2[1024] = {}; // a small buffer on the stack
    std::pmr::monotonic_buffer_resource pool2{ std::data(buffer2), std::size(buffer2) };
    std::pmr::polymorphic_allocator<char> alloc2(&pool2);

    const char* a_long_string = "String too long for short string";

    CHECK_FALSE(std::allocator_traits<std::pmr::polymorphic_allocator<char>>::is_always_equal::value);

    pmr_json an_object1(json_object_arg, alloc1);
    an_object1.try_emplace("true", true);
    an_object1.try_emplace("false", false);
    an_object1.try_emplace("null", null_type());
    an_object1.try_emplace("Key to long for short string", a_long_string);

    std::pmr::string key1{"foo", alloc1};
    std::pmr::string key2{"bar", alloc1};

    SECTION("construct string")
    {
        pmr_json j(a_long_string, alloc1);

        CHECK(j.as<std::string>() == a_long_string);
    }

    SECTION("try_emplace json")
    {
        pmr_json j(json_object_arg, alloc1);

        pmr_json an_object1_copy(an_object1);

        j.try_emplace(key1, pmr_json{});
        j.try_emplace(std::move(key2), a_long_string);
        j.try_emplace("baz", an_object1);
        j.try_emplace("qux", std::move(an_object1_copy));

        CHECK(j.size() == 4);
        CHECK(j.at("foo") == pmr_json{});
        CHECK(j.at("bar").as_string_view() == a_long_string);
        CHECK(j.at("baz") == an_object1);
        CHECK(j.at("qux") == an_object1);
    }

    SECTION("try_emplace ojson")
    {
        pmr_ojson j(json_object_arg, alloc1);

        j.try_emplace(key1, pmr_ojson{});
        j.try_emplace(std::move(key2), a_long_string);

        CHECK(j.size() == 2);
        CHECK(j.at("foo") == pmr_ojson{});
        CHECK(j.at("bar").as_string_view() == a_long_string);
    }

    SECTION("insert_or_assign json")
    {
        pmr_json j(json_object_arg, alloc1);

        j.insert_or_assign("foo", pmr_json{});
        j.insert_or_assign("bar", a_long_string);

        CHECK(j.size() == 2);
        CHECK(j.at("foo") == pmr_json{});
        CHECK(j.at("bar").as_string_view() == a_long_string);
    }

    SECTION("insert_or_assign ojson")
    {
        pmr_ojson j(json_object_arg, alloc1);

        j.insert_or_assign("foo", pmr_ojson{});
        j.insert_or_assign("bar", a_long_string);

        CHECK(j.size() == 2);
        CHECK(j.at("foo") == pmr_ojson{});
        CHECK(j.at("bar").as_string_view() == a_long_string);
    }

    SECTION("emplace_back")
    {
        pmr_json j(json_array_arg, alloc1);
        j.emplace_back(1);
        j.emplace_back(a_long_string);

        CHECK(j.size() == 2);
        CHECK(j.at(0) == 1);
        CHECK(j.at(1).as<std::string>() == a_long_string);
    }

    SECTION("push_back")
    {
        pmr_json j(json_array_arg, alloc1);
        j.push_back(1);
        j.push_back(a_long_string);

        CHECK(j.size() == 2);
        CHECK(j.at(0) == 1);
        CHECK(j.at(1).as<std::string>() == a_long_string);
    }

    SECTION("insert")
    {
        pmr_json j(json_array_arg, alloc1);

        j.insert(j.array_range().end(), pmr_json{});
        j.insert(j.array_range().end(), a_long_string);

        CHECK(j.size() == 2);
        CHECK(j[0] == pmr_json{});
        CHECK(j[1].as_string_view() == a_long_string);
    }

    SECTION("parse")
    {
        std::string s = a_long_string;
        std::string input = "\"" + s + "\"";

        json_decoder<pmr_json> decoder(alloc1);
        JSONCONS_TRY
        {
            json_string_reader reader(input,decoder);
            reader.read_next();
        }
        JSONCONS_CATCH (const std::exception&)
        {
        }
        CHECK(decoder.is_valid());
        auto j = decoder.get_result();
        CHECK(j.as<std::string>() == s);
    }
}
#endif // namespace JSONCONS_HAS_2017


