// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <jsoncons/json.hpp>

#include <stdexcept>
#include <string>
#include <vector>
#include <map>
#include <iostream>

using namespace jsoncons;

void constructor_examples()
{   
    json j1; // An empty object
    std::cout << "(1) " << j1 << "\n";

    json j2(json_object_arg, {{"baz", "qux"}, {"foo", "bar"}}); // An object 
    std::cout << "(2) " << j2 << "\n";

    json j3(json_array_arg, {"bar", "baz"}); // An array 
    std::cout << "(3) " << j3 << "\n";
  
    json j4(json::null()); // A null value
    std::cout << "(4) " << j4 << "\n";
    
    json j5(true); // A boolean value
    std::cout << "(5) " << j5 << "\n";

    double x = 1.0/7.0;

    json j6(x); // A double value
    std::cout << "(6) " << j6 << "\n";

    json j8("Hello"); // A text string
    std::cout << "(8) " << j8 << "\n";

    std::vector<int> v = {10,20,30};
    json j9 = v; // From a sequence container
    std::cout << "(9) " << j9 << "\n";

    std::map<std::string, int> m{ {"one", 1}, {"two", 2}, {"three", 3} };
    json j10 = m; // From an associative container
    std::cout << "(10) " << j10 << "\n";

    std::vector<uint8_t> bytes = {'H','e','l','l','o'};
    json j11(byte_string_arg, bytes); // A byte string
    std::cout << "(11) " << j11 << "\n";

    json j12(half_arg, 0x3bff);
    std::cout << "(12) " << j12.as_double() << "\n";

    // An object value with four members
    json obj;
    obj["first_name"] = "Jane";
    obj["last_name"] = "Roe";
    obj["events_attended"] = 10;
    obj["accept_waiver_of_liability"] = true;

    std::string first_name = obj["first_name"].as<std::string>();
    std::string last_name = obj.at("last_name").as<std::string>();
    int events_attended = obj["events_attended"].as<int>();
    bool accept_waiver_of_liability = obj["accept_waiver_of_liability"].as<bool>();

    // An array value with four elements
    json arr(json_array_arg);
    arr.push_back(j1);
    arr.push_back(j2);
    arr.push_back(j3);
    arr.push_back(j4);

    std::cout << pretty_print(arr) << "\n\n";
}

void json_const_pointer_arg_example()
{
    std::string input = R"(
    {
      "machines": [
        {"id": 1, "state": "running"},
        {"id": 2, "state": "stopped"},
        {"id": 3, "state": "running"}
      ]
    }        
    )";

    json j = json::parse(input);

    json j_v(json_array_arg);
    for (const auto& item : j.at("machines").array_range())
    {
        if (item.at("state").as<std::string>() == "running")
        {
            j_v.emplace_back(json_const_pointer_arg, &item);
        }
    }

    std::cout << "\n(1)\n" << pretty_print(j_v) << "\n\n";

    for (const auto& item : j_v.array_range())
    {
        std::cout << "json type: " << item.type() << ", storage kind: " << item.storage_kind() << "\n";
    }

    json j2 = deep_copy(j_v);

    std::cout << "\n(2)\n" << pretty_print(j2) << "\n\n";

    for (const auto& item : j2.array_range())
    {
        std::cout << "json type: " << item.type() << ", storage kind: " << item.storage_kind() << "\n";
    }
}

int main()
{   
    constructor_examples();
    json_const_pointer_arg_example();
}

