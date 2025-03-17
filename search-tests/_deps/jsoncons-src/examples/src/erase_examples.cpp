// jsoncons_test.cpp : Defines the entry point for the console application.
//

#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

#include <jsoncons/json.hpp>

#include <sstream>
#include <algorithm>

// For brevity
using jsoncons::json;
namespace jsonpointer = jsoncons::jsonpointer; 
namespace jsonpath = jsoncons::jsonpath;

    std::string input = R"(
[
    {
        "address": "ashdod",
        "email": "ron10@gmail.com",
        "first name": "ron",
        "id": "756746783",
        "last name": "cohen",
        "phone": "0526732996",
        "salary": 3000,
        "type": "manager"
    },
    {
        "address": "ashdod",
        "email": "nirlevy120@gmail.com",
        "first name": "nir",
        "id": "11884398",
        "last name": "levy",
        "phone": "0578198932",
        "salary": 4500,
        "type": "manager"
    }
]
    )";


void erase1()
{
    try
    {
        // Read from input
        std::istringstream is(input);
        json instance = json::parse(is);
 
        // Locate the item to be erased
        auto it = std::find_if(instance.array_range().begin(), instance.array_range().end(), 
                               [](const json& item){return item.at("id") == std::string("756746783");});
 
        // If found, erase it
        if (it != instance.array_range().end())
        {
            instance.erase(it);
        }

        // Write to output file
        std::ostringstream os;
        instance.dump_pretty(os);
        std::cout << os.str() << "\n\n";
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << '\n';    
    }
}

void erase2()
{
    // Read from input file
    std::istringstream is(input);
    json instance = json::parse(is);

    // Select all records except ones with id '756746783' 
    auto result = jsonpath::json_query(instance, "$.*[?(@.id != '756746783')]");

    // Write to output file
    std::ostringstream os;
    result.dump_pretty(os);
    std::cout << os.str() << "\n\n";
}

void erase3()
{
    try
    {
        // Read from input file
        std::istringstream is(input);
        json instance = json::parse(is);

        // Remove first record identified by JSONPointer
        jsonpointer::remove(instance, "/0");

        // Write to output file
        std::ostringstream os;
        instance.dump_pretty(os);
        std::cout << os.str() << "\n\n";
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << '\n';    
    }
}

int main()
{
    std::cout << "\nErase\n\n";
    erase1();
    erase2();
    erase3();
    std::cout << '\n';
}

