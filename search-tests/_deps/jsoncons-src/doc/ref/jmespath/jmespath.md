### jmespath extension

The jmespath extension implements [JMESPath](https://jmespath.org/). JMESPath is a query language 
for transforming JSON documents into other JSON documents.
It's supported in both the AWS and Azure CLI and has libraries available in a number of languages.

Since 1.3.0, the jsoncons implementation supports JMESPath Lexical Scoping using the new 
[let expression](https://github.com/jmespath/jmespath.jep/blob/main/proposals/0018-lexical-scope.md), 
added to the language [on Mar 31, 2023](https://github.com/jmespath/jmespath.jep/pull/18).  

### Compliance level

Fully compliant. The jsoncons implementation passes all [compliance tests](https://github.com/jmespath/jmespath.test).

### Classes
<table border="0">
  <tr>
    <td><a href="jmespath_expression.md">jmespath_expression</a></td>
    <td>Represents the compiled form of a JMESPath string.</td> 
  </tr>
</table>

### Functions

<table border="0">
  <tr>
    <td><a href="search.md">search</a></td>
    <td>Searches for all values that match a JMESPath expression</td> 
  </tr>
  <tr>
    <td><a href="make_expression.md">make_expression</a></td>
    <td>Returns a compiled JMESPath expression for later evaluation. (since 0.159.0)</td> 
  </tr>
</table>
    
### Examples

[search function](#eg1)  
[jmespath_expression](#eg2)  
[custom_functions (since 1.0.0)](#eg3)  
[JMESPath Lexical Scoping using the new let expression (since 1.3.0)](#eg4)  
[Late binding of variables to an initial (global) scope via parameters (since 1.3.0)](#eg5)  

 <div id="eg1"/>

#### search function

[jsoncons::jmespath::search](search.md) takes two arguments, a [basic_json](../basic_json.md) 
and a JMESPath expression string, and returns a `basic_json` result. This is the simplest way to
compile and evaluate a JMESPath expression.

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jmespath/jmespath.hpp>

using jsoncons::json; 
namespace jmespath = jsoncons::jmespath;

int main() 
{
    // This examples is from the JMESPath front page
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
```
Output:
```
{
    "WashingtonCities": "Bellevue, Olympia, Seattle"
}
```

Credit to [JMESPath](https://jmespath.org/) for this example

 <div id="eg2"/>

#### jmespath_expression

A [jsoncons::jmespath::jmespath_expression](jmespath_expression.md) 
represents the compiled form of a JMESPath string. It allows you to 
evaluate a single compiled expression on multiple JSON documents.
A `jmespath_expression` is immutable and thread-safe. 

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jmespath/jmespath.hpp>

using jsoncons::json; 
namespace jmespath = jsoncons::jmespath;

int main()
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

 // auto expr = jmespath::jmespath_expression<json>::compile("people[?age > `20`].[name, age]"); // until 0.159.0
    auto expr = jmespath::make_expression<json>("people[?age > `20`].[name, age]");              // since 0.159.0

    json doc = json::parse(jtext);

    json result = expr.evaluate(doc);

    std::cout << pretty_print(result) << "\n\n";
}
```
Output:
```
[
    ["Fred", 25],
    ["George", 30]
]
```

Credit to [JMESPath Tutorial](https://jmespath.org/tutorial.html) for this Example

 <div id="eg3"/>

#### custom_functions (since 1.0.0)

```cpp
#include <chrono>
#include <thread>
#include <string>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jmespath/jmespath.hpp>

namespace jmespath = jsoncons::jmespath;

// When adding custom functions, they are generally placed in their own project's source code and namespace.
namespace myspace {

template <typename Json>
class my_custom_functions : public jmespath::custom_functions<Json>
{
    using reference = const Json&;
    using pointer = const Json*;

    static thread_local size_t current_index;
public:
    my_custom_functions()
    {
        this->register_function("current_index", // function name
            0,                                   // number of arguments   
            [](const jsoncons::span<const jmespath::parameter<Json>> params,
                jmespath::eval_context<Json>& context,
                std::error_code& ec) -> Json
            {
                return Json{current_index};
            }
        );
        this->register_function("generate_array", // function name
            4,                                    // number of arguments   
            [](const jsoncons::span<const jmespath::parameter<Json>> params,
                jmespath::eval_context<Json>& context,
                std::error_code& ec) -> Json
            {
                JSONCONS_ASSERT(4 == params.size());

                if (!(params[0].is_value() && params[2].is_expression()))
                {
                    ec = jmespath::jmespath_errc::invalid_argument;
                    return context.null_value();
                }

                reference ctx = params[0].value();
                reference countValue = get_value(ctx, context, params[1]);
                const auto& expr = params[2].expression();
                const auto& argDefault = params[3];

                if (!countValue.is_number())
                {
                    ec = jmespath::jmespath_errc::invalid_argument;
                    return context.null_value();
                }

                Json result{jsoncons::json_array_arg};
                int count = countValue.template as<int>();
                for (size_t i = 0; i < count; i++)
                {
                    current_index = i;
                    std::error_code ec2;

                    reference ele = expr.evaluate(ctx, context, ec2); 

                    if (ele.is_null())
                    {
                        auto defaultVal = get_value(ctx, context, argDefault);
                        result.emplace_back(std::move(defaultVal));
                    }
                    else
                    {
                        result.emplace_back(ele);
                    }
                }
                current_index = 0;

                return result;
            }
        );
        this->register_function("add", // function name
            2,                         // number of arguments   
            [](jsoncons::span<const jmespath::parameter<Json>> params,
                jmespath::eval_context<Json>& context,
                std::error_code& ec) -> Json
            {
                JSONCONS_ASSERT(2 == params.size());

                if (!(params[0].is_value() && params[1].is_value()))
                {
                    ec = jmespath::jmespath_errc::invalid_argument;
                    return context.null_value();
                }

                reference arg0 = params[0].value();
                reference arg1 = params[1].value();
                if (!(arg0.is_number() && arg1.is_number()))
                {
                    ec = jmespath::jmespath_errc::invalid_argument;
                    return context.null_value();
                }

                if (arg0.is<int64_t>() && arg1.is<int64_t>())
                {
                    int64_t v = arg0.template as<int64_t>() + arg1.template as<int64_t>();
                    return Json(v);
                }
                else
                {
                    double v = arg0.template as<double>() + arg1.template as<double>();
                    return Json(v);
                }
            }
        );
    }

    static reference get_value(reference ctx, jmespath::eval_context<Json>& context,
        const jmespath::parameter<Json>& param)
    {
        if (param.is_expression())
        {
            const auto& expr = param.expression();
            std::error_code ec;
            return expr.evaluate(ctx, context, ec);
        }
        else
        {
            return param.value();
        }
    }
};

template <typename Json>
thread_local size_t my_custom_functions<Json>::current_index = 0;

} // namespace myspace

using json = jsoncons::json;
   
int main()
{
    std::string jtext = R"(
          {
            "devices": [
              {
                "position": 1,
                "id": "id-xxx",
                "state": 1
              },
              {
                "position": 5,
                "id": "id-yyy",
                "state": 1
              },
              {
                "position": 9,
                "id": "id-mmm",
                "state": 2
              }
            ]
          }        
    )";
  
    auto expr = jmespath::make_expression<json>("generate_array(devices, `16`, &[?position==add(current_index(), `1`)] | [0], &{id: '', state: `0`, position: add(current_index(), `1`)})",
        myspace::my_custom_functions<json>{});
  
    auto doc = json::parse(jtext);
  
    auto result = expr.evaluate(doc);
  
    auto options = jsoncons::json_options{}
        .array_object_line_splits(jsoncons::line_split_kind::same_line);
    std::cout << pretty_print(result, options) << "\n\n";
}
```

Output:

```json
[
    {"id": "id-xxx", "position": 1, "state": 1},
    {"id": "", "position": 2, "state": 0},
    {"id": "", "position": 3, "state": 0},
    {"id": "", "position": 4, "state": 0},
    {"id": "id-yyy", "position": 5, "state": 1},
    {"id": "", "position": 6, "state": 0},
    {"id": "", "position": 7, "state": 0},
    {"id": "", "position": 8, "state": 0},
    {"id": "id-mmm", "position": 9, "state": 2},
    {"id": "", "position": 10, "state": 0},
    {"id": "", "position": 11, "state": 0},
    {"id": "", "position": 12, "state": 0},
    {"id": "", "position": 13, "state": 0},
    {"id": "", "position": 14, "state": 0},
    {"id": "", "position": 15, "state": 0},
    {"id": "", "position": 16, "state": 0}
]
```

Credit to [PR #560](https://github.com/danielaparker/jsoncons/pull/560) for this example


 <div id="eg4"/>

#### JMESPath Lexical Scoping using the new let expression (since 1.3.0)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jmespath/jmespath.hpp>
#include <iostream>

using jsoncons::json; 
namespace jmespath = jsoncons::jmespath;

int main()
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
```

Output:

```json
[
    ["Seattle", "Bellevue", "Olympia"],
    ["New York City", "Albany"]
]
```

Credit to [JEP: 18 Lexical Scoping](https://github.com/jmespath/jmespath.jep/blob/main/proposals/0018-lexical-scope.md) for this example

 <div id="eg5"/>

#### Late binding of variables to an initial (global) scope via parameters (since 1.3.0)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jmespath/jmespath.hpp>
#include <iostream>

using jsoncons::json;
namespace jmespath = jsoncons::jmespath;

int main()
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
```

Output:

```json
[
    [
        "test1",
        "33bb9554-c616-42e6-a9c6-88d3bba4221c",
        "localhost"
    ],
    [
        "test2",
        "acde070d-8c4c-4f0d-9d8a-162843c10333",
        "localhost"
    ]
]
```

Credit to [JEP: 18 Lexical Scoping](https://github.com/jmespath/jmespath.jep/blob/main/proposals/0018-lexical-scope.md) for this example

