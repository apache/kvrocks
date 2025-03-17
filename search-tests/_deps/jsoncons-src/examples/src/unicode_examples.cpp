// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <string>
#include <iostream>
#include <jsoncons/json.hpp>

using namespace jsoncons;

void read_and_write_escaped_unicode()
{
    std::string input = "[\"\\u8A73\\u7D30\\u95B2\\u89A7\\uD800\\uDC01\\u4E00\"]";
    json value = json::parse(input);
    auto options = json_options{}
        .escape_all_non_ascii(true);
    std::string output;
    value.dump(output,options);

    std::cout << "Input:" << '\n';
    std::cout << input << '\n';
    std::cout << '\n';
    std::cout << "Output:" << '\n';
    std::cout << output << '\n';
}

int main()
{
    std::cout << "\nUnicode examples\n\n";
    read_and_write_escaped_unicode();
    std::cout << '\n';
}

