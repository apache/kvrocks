// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/jmespath/jmespath.hpp>
#include <jsoncons/json.hpp>

#include <string>
#include <fstream>
#include <cassert>

// for brevity
using jsoncons::json; 
namespace jmespath = jsoncons::jmespath;

void search_example() 
{
    std::string jtext = R"(
    {
      "locations": [
        {"name": "Seattle", "state": "WA"},
        {"name": "New York", "state": "NY"},
        {"name": "Bellevue", "state": "WA"},
        {"name": "Olympia", "state": "WA"}
      ]
    }        
    )";

    std::string expr = "locations[?state == 'WA'].name | sort(@) | {WashingtonCities: join(', ', @)}";

    json doc = json::parse(jtext);

    json result = jmespath::search(doc, expr);

    std::cout << pretty_print(result) << "\n\n";
}

void jmespath_expression_example()
{ 
    std::string jtext = R"(
        {
          "people": [
            {
              "age": 20,
              "other": "foo",
              "name": "Bob"
            },
            {
              "age": 25,
              "other": "bar",
              "name": "Fred"
            },
            {
              "age": 30,
              "other": "baz",
              "name": "George"
            }
          ]
        }        
    )";

    auto expr = jmespath::make_expression<json>("people[?age > `20`].[name, age]");

    json doc = json::parse(jtext);

    json result = expr.evaluate(doc);

    std::cout << pretty_print(result) << "\n\n";
}

void let_example()
{
    auto doc = json::parse(R"(
[
  {"home_state": "WA",
   "states": [
     {"name": "WA", "cities": ["Seattle", "Bellevue", "Olympia"]},
     {"name": "CA", "cities": ["Los Angeles", "San Francisco"]},
     {"name": "NY", "cities": ["New York City", "Albany"]}
   ]
  },
  {"home_state": "NY",
   "states": [
     {"name": "WA", "cities": ["Seattle", "Bellevue", "Olympia"]},
     {"name": "CA", "cities": ["Los Angeles", "San Francisco"]},
     {"name": "NY", "cities": ["New York City", "Albany"]}
   ]
  }
]
    )");

    std::string query = R"([*].[let $home_state = home_state in states[? name == $home_state].cities[]][])";
    auto expr = jmespath::make_expression<json>(query);

    json result = expr.evaluate(doc);

    auto options = jsoncons::json_options{}
        .array_array_line_splits(jsoncons::line_split_kind::same_line);
    std::cout << pretty_print(result, options) << "\n";
}

void expression_with_params_example() // since 1.3.0
{
    auto doc = json::parse(R"(
{
    "results": [
         {
              "name": "test1",
              "uuid": "33bb9554-c616-42e6-a9c6-88d3bba4221c"
          },
          {
              "name": "test2",
              "uuid": "acde070d-8c4c-4f0d-9d8a-162843c10333"
          }
    ]
}
    )");

    auto expr = jmespath::make_expression<json>("results[*].[name, uuid, $hostname]");

    auto result = expr.evaluate(doc, {{"hostname", json{"localhost"}}});

    auto options = jsoncons::json_options{}
        .array_array_line_splits(jsoncons::line_split_kind::same_line);
    std::cout << pretty_print(result) << "\n";
}

#if defined(_MSC_VER)

#include <execution>
#include <concurrent_vector.h> // microsoft PPL library

void query_json_lines_in_parallel() 
{

    std::vector<std::string> lines = { {
        R"({"name": "Seattle", "state" : "WA"})",
        R"({ "name": "New York", "state" : "NY" })",
        R"({ "name": "Bellevue", "state" : "WA" })",
        R"({ "name": "Olympia", "state" : "WA" })"
} };

    auto expr = jsoncons::jmespath::make_expression<jsoncons::json>(
        R"([@][?state=='WA'].name)");

    concurrency::concurrent_vector<std::string> result;

    auto f = [&](const std::string& line)
        {
            const auto j = jsoncons::json::parse(line);
            const auto r = expr.evaluate(j);
            if (!r.empty())
                result.push_back(r.at(0).as<std::string>());
        };

    std::for_each(std::execution::par, lines.begin(), lines.end(), f);

    for (const auto& s : result)
    {
        std::cout << s << "\n";
    }
}

#endif

int main()
{
    std::cout << "\nJMESPath examples\n\n";
    search_example();
    jmespath_expression_example();
    let_example();
    expression_with_params_example();

#if defined(_MSC_VER)
    query_json_lines_in_parallel();
#endif

    std::cout << "\n";
}

