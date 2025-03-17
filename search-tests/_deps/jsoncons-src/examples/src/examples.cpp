// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

#include <jsoncons/json.hpp>

#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>
#include <map>

using namespace jsoncons;

void first_example_a()
{
    std::string path = "./input/books.json"; 
    std::fstream is(path);
    if (!is)
    {
        std::cout << "Cannot open " << path << '\n';
        return;
    }
    json books = json::parse(is);

    for (const auto& book : books.at("books").array_range())
    {
        try
        {
            std::string author = book["author"].as<std::string>();
            std::string title = book["title"].as<std::string>();
            double price = book["price"].as<double>();
            std::cout << author << ", " << title << ", " << price << '\n';
        }
        catch (const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
    }
}

void first_example_b()
{
    std::string path = "./input/books.json"; 
    std::fstream is(path);
    if (!is)
    {
        std::cout << "Cannot open " << path << '\n';
        return;
    }
    json books = json::parse(is);

    for (const auto& book : books.at("books").array_range())
    {
        try
        {
            std::string author = book["author"].as<std::string>();
            std::string title = book["title"].as<std::string>();
            std::string price = book.get_value_or<std::string>("price", "N/A");
            std::cout << author << ", " << title << ", " << price << '\n';
        }
        catch (const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
    }
}

void first_example_c()
{
    const json books = json::parse(R"(
    [
        {
            "title" : "Kafka on the Shore",
            "author" : "Haruki Murakami",
            "price" : 25.17
        },
        {
            "title" : "Women: A Novel",
            "author" : "Charles Bukowski",
            "price" : 12.00
        },
        {
            "title" : "Cutter's Way",
            "author" : "Ivan Passer"
        }
    ]
    )");

    auto options = json_options{};

    for (const auto& book : books.array_range())
    {
        try
        {
            std::string author = book["author"].as<std::string>();
            std::string title = book["title"].as<std::string>();
            std::string price;
            book.get_value_or<json>("price", "N/A").dump(price,options);
            std::cout << author << ", " << title << ", " << price << '\n';
        }
        catch (const ser_error& e)
        {
            std::cerr << e.what() << '\n';
        }
    }
}

void first_example_d()
{
    std::string path = "./input/books.json"; 
    std::fstream is(path);
    if (!is)
    {
        std::cout << "Cannot open " << path << '\n';
        return;
    }
    json books = json::parse(is);

    auto options = json_options{}
        .precision(2);

    for (const auto& book : books.at("books").array_range())
    {
        try
        {
            std::string author = book["author"].as<std::string>();
            std::string title = book["title"].as<std::string>();
            if (book.contains("price") && book["price"].is_number())
            {
                double price = book["price"].as<double>();
                std::cout << author << ", " << title << ", " << price << '\n';
            }
            else
            {
                std::cout << author << ", " << title << ", " << "n/a" << '\n';
            }
        }
        catch (const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
    }

}

void second_example_a()
{
    try
    {
        json books(json_array_arg);

        {
            json book;
            book["title"] = "Kafka on the Shore";
            book["author"] = "Haruki Murakami";
            book["price"] = 25.17;
            books.push_back(std::move(book));
        }

        {
            json book;
            book["title"] = "Women: A Novel";
            book["author"] = "Charles Bukowski";
            book["price"] = 12.00;
            books.push_back(std::move(book));
        }

        {
            json book;
            book["title"] = "Cutter's Way";
            book["author"] = "Ivan Passer";
            books.push_back(std::move(book));
        }

        std::cout << pretty_print(books) << '\n';
    }
    catch (const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
}

void object_range_based_for_loop()
{
    json j = json::parse(R"(
{
    "category" : "Fiction",
    "title" : "Pulp",
    "author" : "Charles Bukowski",
    "date" : "2004-07-08",
    "price" : 22.48,
    "isbn" : "1852272007"  
}
)");

    for (const auto& member : j.object_range())
    {
        std::cout << member.key() << " => " << member.value().as<std::string>() << '\n';
    }

#if defined(JSONCONS_HAS_2017)
    // or, since 0.176.0, using C++ 17 structured binding
    for (const auto& [key, value] : j.object_range())
    {
        std::cout << key << " => " << value << '\n';
    }
#endif    
}

void validation_example()
{
    std::string s = R"(
{
    "StartDate" : "2017-03-01",
    "MaturityDate" "2020-12-30"          
}
    )";
    std::stringstream is(s);

    json_stream_reader reader(is);

    std::error_code ec;
    reader.read(ec);
    if (ec)
    {
        std::cout << ec.message() 
                  << " on line " << reader.line()
                  << " and column " << reader.column()
                  << '\n';
    }
}

void get_example()
{
    json j = json::parse(R"(
    {
       "application": "hiking",
       "reputons": [
       {
           "rater": "HikingAsylum",
           "assertion": "advanced",
           "rated": "Marilyn C",
           "rating": 0.90
         }
       ]
    }
    )");

    // Using index or `at` accessors
    std::string result1 = j["reputons"][0]["rated"].as<std::string>();
    std::cout << "(1) " << result1 << '\n';
    std::string result2 = j.at("reputons").at(0).at("rated").as<std::string>();
    std::cout << "(2) " << result2 << '\n';

    // Using JSON Pointer
    std::string result3 = jsonpointer::get(j, "/reputons/0/rated").as<std::string>();
    std::cout << "(3) " << result3 << '\n';

    // Using JSONPath
    json result4 = jsonpath::json_query(j, "$.reputons.0.rated");
    if (result4.size() > 0)
    {
        std::cout << "(4) " << result4[0].as<std::string>() << '\n';
    }
    json result5 = jsonpath::json_query(j, "$..0.rated");
    if (result5.size() > 0)
    {
        std::cout << "(5) " << result5[0].as<std::string>() << '\n';
    }
}

int main()
{
    try
    {
        std::cout << "jsoncons version: " << version() << '\n';

        first_example_a();
        first_example_b();
        first_example_c();
        first_example_d();

        second_example_a();

        object_range_based_for_loop();

        validation_example();

        get_example();
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << '\n';
    }

    return 0;
}
