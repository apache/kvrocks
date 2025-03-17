// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <jsoncons/json.hpp>

#include <iostream>
#include <catch/catch.hpp>

using namespace jsoncons;

#if defined(JSONCONS_HAS_STATEFUL_ALLOCATOR) && JSONCONS_HAS_STATEFUL_ALLOCATOR == 1

#include <common/free_list_allocator.hpp>
#include <scoped_allocator>

template <typename T>
using MyScopedAllocator = std::scoped_allocator_adaptor<free_list_allocator<T>>;

using cust_json = basic_json<char,sorted_policy,MyScopedAllocator<char>>;

TEST_CASE("jsonpath stateful allocator test")
{
    std::string input = R"(
{ "store": {
    "book": [ 
      { "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      { "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      },
      { "category": "fiction",
        "author": "Herman Melville",
        "title": "Moby Dick",
        "isbn": "0-553-21311-3",
        "price": 8.99
      }
    ]
  }
}
)";

    SECTION("make_expression")
    {
        json_decoder<cust_json> decoder(MyScopedAllocator<char>(1));

        auto myAlloc = MyScopedAllocator<char>(3);        

        basic_json_reader<char,string_source<char>,MyScopedAllocator<char>> reader(input, decoder, myAlloc);
        reader.read();

        cust_json j = decoder.get_result();

        jsoncons::string_view p{"$..book[?(@.category == 'fiction')].title"};
        auto expr = jsoncons::jsonpath::make_expression<cust_json>(combine_allocators(myAlloc), p);  
        auto result = expr.evaluate(j);

        CHECK(result.size() == 2);
        CHECK(result[0].as_string_view() == "Sword of Honour");
        CHECK(result[1].as_string_view() == "Moby Dick");
    }
    SECTION("json_query 1")
    {
        json_decoder<cust_json,MyScopedAllocator<char>> decoder(MyScopedAllocator<char>(1),
                                                              MyScopedAllocator<char>(2));

        auto myAlloc = MyScopedAllocator<char>(3);        

        basic_json_reader<char,string_source<char>,MyScopedAllocator<char>> reader(input, decoder, myAlloc);
        reader.read();

        cust_json j = decoder.get_result();

        auto result = jsoncons::jsonpath::json_query(combine_allocators(myAlloc), 
            j,"$..book[?(@.category == 'fiction')].title");

        CHECK(result.size() == 2);
        CHECK(result[0].as_string_view() == "Sword of Honour");
        CHECK(result[1].as_string_view() == "Moby Dick");
    }
    SECTION("json_query 2")
    {
        json_decoder<cust_json,MyScopedAllocator<char>> decoder(MyScopedAllocator<char>(1),
                                                              MyScopedAllocator<char>(2));

        auto myAlloc = MyScopedAllocator<char>(3);        

        basic_json_reader<char,string_source<char>,MyScopedAllocator<char>> reader(input, decoder, myAlloc);
        reader.read();

        cust_json j = decoder.get_result();

        jsonpath::json_query(combine_allocators(myAlloc), 
            j, "$..book[?(@.title == 'Sword of Honour')].title", 
            [](const jsoncons::string_view&, const cust_json& title) 
            {
                CHECK((title.as<jsoncons::string_view>() == "Sword of Honour")); 
            }
 );
    }

    SECTION("json_replace 1")
    {
        json_decoder<cust_json> decoder(MyScopedAllocator<char>(1));

        auto myAlloc = MyScopedAllocator<char>(3);        

        basic_json_reader<char,string_source<char>,MyScopedAllocator<char>> reader(input, decoder, myAlloc);
        reader.read();

        cust_json j = decoder.get_result();

        auto res = jsonpath::json_query(combine_allocators(myAlloc), j, "$..book[?(@.price==12.99)].price");

        //std::cout << "res:\n" << pretty_print(res) << "\n\n";

        jsonpath::json_replace(combine_allocators(myAlloc),
            j,"$..book[?(@.price==12.99)].price", 30.9);

        //std::cout << "j:\n" << pretty_print(j) << "\n\n"; 

        CHECK(30.9 == Approx(j["store"]["book"][1]["price"].as<double>()).epsilon(0.001));
    }

    SECTION("json_replace 2")
    {
        json_decoder<cust_json,MyScopedAllocator<char>> decoder(MyScopedAllocator<char>(1),
                                                              MyScopedAllocator<char>(2));

        auto myAlloc = MyScopedAllocator<char>(3);        

        basic_json_reader<char,string_source<char>,MyScopedAllocator<char>> reader(input, decoder, myAlloc);
        reader.read();

        cust_json j = decoder.get_result();

        // make a discount on all books
        jsonpath::json_replace(combine_allocators(myAlloc),
            j, "$.store.book[*].price",
            [](const jsoncons::string_view&, cust_json& price) 
            {
                price = std::round(price.as<double>() - 1.0); 
            }
 );

        CHECK(8.0 == Approx(j["store"]["book"][0]["price"].as<double>()).epsilon(0.001));
        CHECK(12.0 == Approx(j["store"]["book"][1]["price"].as<double>()).epsilon(0.001));
        CHECK(8.0 == Approx(j["store"]["book"][2]["price"].as<double>()).epsilon(0.001));
    }
}

#endif
