// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <iostream>

void incremental_parsing_example()
{
    jsoncons::json_decoder<jsoncons::json> decoder;
    jsoncons::json_parser parser;
    try
    {
        parser.update("[fal");
        parser.parse_some(decoder);
        std::cout << "(1) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.update("se,");
        parser.parse_some(decoder);
        std::cout << "(2) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.update("9");
        parser.parse_some(decoder);
        std::cout << "(3) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.update("0]");
        parser.parse_some(decoder);
        std::cout << "(4) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.finish_parse(decoder);
        std::cout << "(5) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.check_done();
        std::cout << "(6) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        jsoncons::json j = decoder.get_result();
        std::cout << "(7) " << j << "\n\n";
    }
    catch (const jsoncons::ser_error& e)
    {
        std::cout << e.what() << '\n';
    }
}

void parse_nan_replacement_example()
{
    std::string s = R"(
        {
           "A" : "NaN",
           "B" : "Infinity",
           "C" : "-Infinity"
        }
    )";

    auto options = jsoncons::json_options{}
        .nan_to_str("NaN")
        .inf_to_str("Infinity");

    jsoncons::json_decoder<jsoncons::json> decoder;
    jsoncons::json_parser parser(options);
    try
    {
        parser.update(s.data(), s.size());   // since 1.0.0
        parser.parse_some(decoder);
        parser.finish_parse(decoder);
        parser.check_done();
    }
    catch (const jsoncons::ser_error& e)
    {
        std::cout << e.what() << '\n';
    }

    jsoncons::json j = decoder.get_result(); // performs move
    if (j["A"].is<double>())
    {
        std::cout << "A: " << j["A"].as<double>() << '\n';
    }
    if (j["B"].is<double>())
    {
        std::cout << "B: " << j["B"].as<double>() << '\n';
    }
    if (j["C"].is<double>())
    {
        std::cout << "C: " << j["C"].as<double>() << '\n';
    }
}

int main()
{
    std::cout << "\njson_parser examples\n\n";
    
    incremental_parsing_example();
    parse_nan_replacement_example();

    std::cout << '\n';
}



