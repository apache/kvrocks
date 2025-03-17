// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons/json.hpp>

#include <string_view>
#include <fstream>
#include <cmath>
#include <cassert>

// for brevity
using jsoncons::json; 
namespace jsonpath = jsoncons::jsonpath;
namespace jsonpointer = jsoncons::jsonpointer;

// since 0.172.0
void remove_selected_books()
{
    std::string json_string = R"(
{
    "books":
    [
        {
            "category": "fiction",
            "title" : "A Wild Sheep Chase",
            "author" : "Haruki Murakami",
            "price" : 22.72
        },
        {
            "category": "fiction",
            "title" : "The Night Watch",
            "author" : "Sergei Lukyanenko",
            "price" : 23.58
        },
        {
            "category": "fiction",
            "title" : "The Comedians",
            "author" : "Graham Greene",
            "price" : 21.99
        },
        {
            "category": "memoir",
            "title" : "The Night Watch",
            "author" : "Phillips, David Atlee"
        }
    ]
}
    )";

    json doc = json::parse(json_string);

    auto expr = jsonpath::make_expression<json>("$.books[?(@.category == 'fiction')]");
    std::vector<jsonpath::json_location> locations = expr.select_paths(doc, 
        jsonpath::result_options::sort_descending | jsonpath::result_options::sort_descending);

    for (const auto& location : locations)
    {
        std::cout << jsonpath::to_string(location) << "\n";
    }
    std::cout << "\n";

    for (const auto& location : locations)
    {
        jsonpath::remove(doc, location);
    }

    std::cout << jsoncons::pretty_print(doc) << "\n\n";
}

// since 0.172.0
void remove_selected_books_in_one_step()
{
    std::string json_string = R"(
{
    "books":
    [
        {
            "category": "fiction",
            "title" : "A Wild Sheep Chase",
            "author" : "Haruki Murakami",
            "price" : 22.72
        },
        {
            "category": "fiction",
            "title" : "The Night Watch",
            "author" : "Sergei Lukyanenko",
            "price" : 23.58
        },
        {
            "category": "fiction",
            "title" : "The Comedians",
            "author" : "Graham Greene",
            "price" : 21.99
        },
        {
            "category": "memoir",
            "title" : "The Night Watch",
            "author" : "Phillips, David Atlee"
        }
    ]
}
    )";

    json doc = json::parse(json_string);

    std::size_t n = jsonpath::remove(doc, "$.books[?(@.category == 'fiction')]");

    std::cout << "Number of nodes removed: " << n << "\n\n";

    std::cout << jsoncons::pretty_print(doc) << "\n\n";
}

// since 0.174.0
void replace_example()
{
    std::string json_string = R"(
{"books": [ 
    { "category": "reference",
      "author": "Nigel Rees",
      "title": "Sayings of the Century",
      "price": 8.95
    },
    { "category": "fiction",
      "author": "Evelyn Waugh",
      "title": "Sword of Honour"
    },
    { "category": "fiction",
      "author": "Herman Melville",
      "title": "Moby Dick",
      "isbn": "0-553-21311-3"
    }
  ] 
}
    )";

    json doc = json::parse(json_string);

    json new_price{13.0}; 

    jsonpath::json_location loc0 = jsonpath::json_location::parse("$.books[0].price");
    auto result0 = jsonpath::replace(doc, loc0, new_price);
    assert(result0.second);
    assert(result0.first == std::addressof(doc.at("books").at(0).at("price")));
    assert(doc.at("books").at(0).at("price") == new_price);
    
    jsonpath::json_location loc1 = jsonpath::json_location::parse("$.books[1].price");
    auto result1 = jsonpath::replace(doc, loc1, new_price);
    assert(!result1.second);
    
    // create_if_missing is true
    result1 = jsonpath::replace(doc, loc1, new_price, true);
    assert(result1.second);
    assert(result1.first == std::addressof(doc.at("books").at(1).at("price")));
    assert(doc.at("books").at(1).at("price") == new_price);

    jsonpath::json_location loc2 = jsonpath::json_location::parse("$.books[2].kindle.price");
    auto result2 = jsonpath::replace(doc, loc2, new_price, true);
    assert(result2.second);
    assert(result2.first == std::addressof(doc.at("books").at(2).at("kindle").at("price")));
    assert(doc.at("books").at(2).at("kindle").at("price") == new_price);
    
    std::cout << pretty_print(doc) << "\n\n";
}

void convert_normalized_path_to_json_pointer()
{
    std::string json_string = R"(
{
    "books":
    [
        {
            "category": "fiction",
            "title" : "A Wild Sheep Chase",
            "author" : "Haruki Murakami",
            "price" : 22.72
        },
        {
            "category": "fiction",
            "title" : "The Night Watch",
            "author" : "Sergei Lukyanenko",
            "price" : 23.58
        },
        {
            "category": "fiction",
            "title" : "The Comedians",
            "author" : "Graham Greene",
            "price" : 21.99
        },
        {
            "category": "memoir",
            "title" : "The Night Watch",
            "author" : "Phillips, David Atlee"
        }
    ]
}
    )";

    json doc = json::parse(json_string);

    auto expr = jsonpath::make_expression<json>("$.books[?(@.category == 'fiction')]");
    std::vector<jsonpath::json_location> locations = expr.select_paths(doc, jsonpath::result_options::sort_descending);

    for (const auto& location : locations)
    {
        std::cout << jsonpath::to_string(location) << "\n";
    }
    std::cout << "\n";

    std::vector<jsoncons::jsonpointer::json_pointer> pointers;
    for (const auto& location : locations)
    {
        jsonpointer::json_pointer ptr;
        {
            for (const jsonpath::path_element& element : location)
            {
                if (element.has_name())
                {
                    ptr.append(element.name());
                }
                else
                {
                    ptr.append(element.index());
                }
            }
        }
        pointers.push_back(ptr);
    }

    for (const auto& ptr : pointers)
    {
        std::cout << jsonpointer::to_string(ptr) << "\n";
    }
    std::cout << "\n";
}

int main()
{
    std::cout << "\njsonpath location examples\n\n";

    remove_selected_books();

    convert_normalized_path_to_json_pointer();

    remove_selected_books_in_one_step();

    replace_example();
    
    std::cout << "\n";
}

