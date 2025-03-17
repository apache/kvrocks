// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_cursor.hpp>
#include <jsoncons/staj_iterator.hpp>
#include <string>
#include <sstream>
#include "sample_types.hpp"

using namespace jsoncons;

// Example JSON text
const std::string array_example = R"(
[ 
  { 
      "employeeNo" : "101",
      "name" : "Tommy Cochrane",
      "title" : "Supervisor"
  },
  { 
      "employeeNo" : "102",
      "name" : "Bill Skeleton",
      "title" : "Line manager"
  }
]
)";

const std::string object_example = R"(
{
   "application": "hiking",
   "reputons": [
   {
       "rater": "HikingAsylum.array_example.com",
       "assertion": "advanced",
       "rated": "Marilyn C",
       "rating": 0.90
     }
   ]
}
)";

void staj_array_iterator_example()
{
    std::istringstream is(array_example);

    json_stream_cursor cursor(is);

    auto view = staj_array<json>(cursor);

    for (const auto& j : view)
    {
        std::cout << pretty_print(j) << "\n";
    }
    std::cout << "\n\n";
}

void staj_array_iterator_example2()
{
    std::istringstream is(array_example);

    json_stream_cursor cursor(is);

    auto view = staj_array<ns::employee>(cursor);

    for (const auto& val : view)
    {
        std::cout << val.employeeNo << ", " << val.name << ", " << val.title << "\n";
    }
    std::cout << "\n\n";
}

void staj_object_iterator_example()
{
    json_string_cursor cursor(object_example);

    auto view = staj_object<std::string,json>(cursor);

    for (const auto& kv : view)
    {
        std::cout << kv.first << ":\n" << pretty_print(kv.second) << "\n";
    }
    std::cout << "\n\n";
}

int main()
{
    std::cout << "\nstaj_iterator examples\n\n";

    staj_array_iterator_example();

    staj_array_iterator_example2();

    staj_object_iterator_example();

    std::cout << "\n";
}

