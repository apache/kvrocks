// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <stdexcept>
#include <string>
#include <cassert>
#include <jsoncons/json.hpp>
#include <fstream>

using namespace jsoncons;

void is_as_examples()
{
    json j = json::parse(R"(
    {
        "k1" : 2147483647,
        "k2" : 2147483648,
        "k3" : -10,
        "k4" : 10.5,
        "k5" : true,
        "k6" : "10.5"
    }
    )");

    std::cout << std::boolalpha << "(1) " << j["k1"].is<int32_t>() << '\n';
    std::cout << std::boolalpha << "(2) " << j["k2"].is<int32_t>() << '\n';
    std::cout << std::boolalpha << "(3) " << j["k2"].is<long long>() << '\n';
    std::cout << std::boolalpha << "(4) " << j["k3"].is<signed char>() << '\n';
    std::cout << std::boolalpha << "(5) " << j["k3"].is<uint32_t>() << '\n';
    std::cout << std::boolalpha << "(6) " << j["k4"].is<int32_t>() << '\n';
    std::cout << std::boolalpha << "(7) " << j["k4"].is<double>() << '\n';
    std::cout << std::boolalpha << "(8) " << j["k5"].is<int>() << '\n';
    std::cout << std::boolalpha << "(9) " << j["k5"].is<bool>() << '\n';
    std::cout << std::boolalpha << "(10) " << j["k6"].is<double>() << '\n';
    std::cout << '\n';
    std::cout << "(1) " << j["k1"].as<int32_t>() << '\n';
    std::cout << "(2) " << j["k2"].as<int32_t>() << '\n';
    std::cout << "(3) " << j["k2"].as<long long>() << '\n';
    std::cout << "(4) " << j["k3"].as<signed char>() << '\n';
    std::cout << "(5) " << j["k3"].as<uint32_t>() << '\n';
    std::cout << "(6) " << j["k4"].as<int32_t>() << '\n';
    std::cout << "(7) " << j["k4"].as<double>() << '\n';
    std::cout << std::boolalpha << "(8) " << j["k5"].as<int>() << '\n';
    std::cout << std::boolalpha << "(9) " << j["k5"].as<bool>() << '\n';
    std::cout << "(10) " << j["k6"].as<double>() << '\n';
}

void byte_string_from_initializer_list()
{
    json j(byte_string{'H','e','l','l','o'});
    byte_string bytes = j.as<byte_string>();

    std::cout << "(1) "<< bytes << "\n\n";

    std::cout << "(2) ";
    for (auto b : bytes)
    {
        std::cout << (char)b;
    }
    std::cout << "\n\n";

    std::cout << "(3) " << j << '\n';
}

void byte_string_from_char_array()
{
    std::vector<uint8_t> u = {'H','e','l','l','o'};

    json j(byte_string_arg, u, semantic_tag::base64);

    auto bytes = j.as<std::vector<uint8_t>>();
    std::cout << "(1) ";
    for (auto b : bytes)
    {
        std::cout << (char)b;
    }
    std::cout << "\n\n";

    std::string s;
    encode_json(j, s); // tag information is lost 
    std::cout << "(2) " << s << "\n\n";

    auto sj = decode_json<json>(s);

    // provide hint
    auto v = sj.as<std::vector<uint8_t>>(byte_string_arg,
                                         semantic_tag::base64);

    assert(v == u);
}

void introspection_example()
{
    std::string path = "./input/books.json"; 
    std::fstream is(path);
    if (!is)
    {
        std::cout << "Cannot open " << path << '\n';
        return;
    }
    json val = json::parse(is);
    std::cout << std::boolalpha;
    std::cout << "Is this an object? " << val.is_object() << ", or an array? " << val.is_array() << '\n';

    if (val.at("books").is_array())
    {
        std::size_t index = 0;
        for (const auto& book : val.at("books").array_range())
        {
            std::cout << "Is element " << index++ << " an object? " << book.is_object() << '\n';
            if (book.is_object())
            {
                for (auto it = book.object_range().begin(); it != book.object_range().end(); ++it){
                    std::cout << "Is member " << it->key() << " a string? " << it->value().is<std::string>() << ", or a double? " << it->value().is<double>() << ", or perhaps an int? " << it->value().is<int>() << '\n';

                }
            }
        }
    }
}

void operator_at_examples()
{
    json image_formats(json_array_arg, {"JPEG","PSD","TIFF","DNG"});

    json color_spaces(json_array_arg);
    color_spaces.push_back("sRGB");
    color_spaces.push_back("AdobeRGB");
    color_spaces.push_back("ProPhoto RGB");

    json export_settings;
    export_settings["File Format Options"]["Color Spaces"] = std::move(color_spaces);
    export_settings["File Format Options"]["Image Formats"] = std::move(image_formats);

    std::cout << pretty_print(export_settings) << "\n\n";
}

void return_value_null_or_default_example()
{
    json j(json_object_arg, {{"author","Evelyn Waugh"},{"title","Sword of Honour"}});

    std::cout << "(1) " << j.at_or_null("author").as<std::string>() << "\n";
    std::cout << "(2) " << j.at_or_null("title").as<std::string>() << "\n";
    std::cout << "(3) " << j.at_or_null("category").as<std::string>() << "\n";
    std::cout << "(4) " << j.get_value_or<std::string>("category","fiction") << "\n";
}

void reverse_object_iterator()
{
    ojson j;
    j["city"] = "Toronto";
    j["province"] = "Ontario";
    j["country"] = "Canada";

    for (auto it = j.object_range().crbegin(); it != j.object_range().crend(); ++it)
    {
        std::cout << it->key() << " => " << it->value().as<std::string>() << '\n';
    }
    std::cout << "\n";
}

int main()
{
    is_as_examples();

    introspection_example();

    byte_string_from_initializer_list();

    operator_at_examples();

    return_value_null_or_default_example();

    byte_string_from_char_array();

    reverse_object_iterator();
}


