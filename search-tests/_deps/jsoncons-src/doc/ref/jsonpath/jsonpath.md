### jsonpath extension

The jsonpath extension implements [Stefan Goessner's JSONPath](http://goessner.net/articles/JsonPath/).
It provides functions for search and "search and replace" using JSONPath expressions.

### Classes
<table border="0">
  <tr>
    <td><a href="jsonpath_expression.md">jsonpath_expression</a></td>
    <td>Represents the compiled form of a JSONPath string. (since 0.161.0)</td> 
  </tr>
  <tr>
    <td><a href="basic_json_location.md">basic_json_location</a></td>
    <td>Represents the location of a specific value in a JSON document. (since 0.172.0)</td> 
  </tr>
  <tr>
    <td><a href="basic_path_node.md">basic_path_node</a></td>
    <td>Represents a normalized path as a singly linked list where each node has a pointer to its (shared) parent node. (since 0.172.0)</td> 
  </tr>
</table>

### Functions

<table border="0">
  <tr>
    <td><a href="make_expression.md">make_expression</a></td>
    <td>Returns a compiled JSONPath expression for later evaluation. (since 0.161.0)</td> 
  </tr>
  <tr>
    <td><a href="json_query.md">json_query</a></td>
    <td>Searches for all values that match a JSONPath expression</td> 
  </tr>
  <tr>
    <td><a href="json_replace.md">json_replace</a></td>
    <td>Search and replace using JSONPath expressions.</td> 
  </tr>
  <tr>
    <td><a href="flatten.md">flatten<br>unflatten</a></td>
    <td>Flattens a json object or array.</td> 
  </tr>
</table>
    
### jsoncons JSONPath

[JSONPath](http://goessner.net/articles/JsonPath/) is a loosely standardized syntax for querying JSON. 
There are many implementations and they differ in significant ways, see Christoph Burgmer's 
[JSONPath comparison](https://cburgmer.github.io/json-path-comparison/). For details about the jsoncons
implementation, see the document [JsonCons JSONPath](https://danielaparker.github.io/JsonCons.Net/articles/JsonPath/JsonConsJsonPath.html).

In addition, the C++ implementation supports the following features:

- A length property on arrays and strings that returns the number of elements in an array, or the number of codepoints in a string,
e.g. `$[?(@.length == 2)]`.

- Custom functions, allowing the user to augment the list of built in JSONPath functions with user-provided functions (since 0.164.0).

### Examples

The examples use the sample data file `books.json`, 

```json
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
```

[Using json_query](#eg1)  
[Using json_replace](#eg2)  
[Using make_expression](#eg3)  
[Custom functions](#eg4)  

 <div id="eg1"/>

#### Using json_query

```
#include <fstream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

using json = jsoncons::json;
namespace jsonpath = jsoncons::jsonpath;

int main()
{
    std::ifstream is(/*path_to_books_file*/);
    json data = json::parse(is);

    auto result1 = jsonpath::json_query(data, "$.books[1,1,3].title");
    std::cout << "(1)\n" << pretty_print(result1) << "\n\n";

    auto result2 = jsonpath::json_query(data, "$.books[1,1,3].title",
                                        jsonpath::result_options::path);
    std::cout << "(2)\n" << pretty_print(result2) << "\n\n";

    //auto result3 = jsonpath::json_query(data, "$.books[1,1,3].title",
    //                                    jsonpath::result_options::value | 
    //                                    jsonpath::result_options::nodups);  (until 0.164.0)
    auto result3 = jsonpath::json_query(data, "$.books[1,1,3].title", 
                                        jsonpath::result_options::nodups);    (since 0.164.0) 
    std::cout << "(3)\n" << pretty_print(result3) << "\n\n";

    //auto result4 = jsonpath::json_query(data, "$.books[1,1,3].title", 
    //                                    jsonpath::result_options::nodups);  (until 0.164.0)
    auto result4 = jsonpath::json_query(data, "$.books[1,1,3].title", 
                                        jsonpath::result_options::nodups | 
                                        jsonpath::result_options::path);      (since 0.164.0)
    std::cout << "(4)\n" << pretty_print(result4) << "\n\n";
}
```
Output:
```
(1)
[
    "The Night Watch",
    "The Night Watch",
    "The Night Watch"
]

(2)
[
    "$['books'][1]['title']",
    "$['books'][1]['title']",
    "$['books'][3]['title']"
]

(3)
[
    "The Night Watch",
    "The Night Watch"
]

(4)
[
    "$['books'][1]['title']",
    "$['books'][3]['title']"
]
```

 <div id="eg2"/>

#### Using json_replace

```
#include <fstream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

using json = jsoncons::json;
namespace jsonpath = jsoncons::jsonpath;

int main()
{
    std::ifstream is(/*path_to_books_file*/);
    json data = json::parse(is);

    auto f = [](const std::string& /*location*/, json& price) 
    {
        price = std::round(price.as<double>() - 1.0);
    };
    jsonpath::json_replace(data, "$.books[*].price", f);

    std::cout << pretty_print(data) << "\n";
}
```
Output:
```
{
    "books": [
        {
            "author": "Haruki Murakami",
            "category": "fiction",
            "price": 22.0,
            "title": "A Wild Sheep Chase"
        },
        {
            "author": "Sergei Lukyanenko",
            "category": "fiction",
            "price": 23.0,
            "title": "The Night Watch"
        },
        {
            "author": "Graham Greene",
            "category": "fiction",
            "price": 21.0,
            "title": "The Comedians"
        },
        {
            "author": "Phillips, David Atlee",
            "category": "memoir",
            "title": "The Night Watch"
        }
    ]
}
```

 <div id="eg3"/>

#### Using make_expression

A [jsoncons::jsonpath::Using make_expression](Using make_expression.md) 
represents the compiled form of a JSONPath expression. It allows you to 
evaluate a single compiled expression on multiple JSON documents.
A `Using make_expression` is immutable and thread-safe. 

```
#include <fstream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

using json = jsoncons::json;
namespace jsonpath = jsoncons::jsonpath;

int main()
{
    auto expr = jsonpath::make_expression<json>("$.books[1,1,3].title");

    std::ifstream is(/*path_to_books_file*/);
    json data = json::parse(is);

    json result1 = expr.evaluate(data);
    std::cout << "(1)\n" << pretty_print(result1) << "\n\n";

    json result2 = expr.evaluate(data, jsonpath::result_options::path);
    std::cout << "(2)\n" << pretty_print(result2) << "\n\n";

    //json result3 = expr.evaluate(data, jsonpath::result_options::value |
    //                                   jsonpath::result_options::nodups);   (until 0.164.0)
    json result3 = expr.evaluate(data, jsonpath::result_options::nodups);     (since 0.164.0)
    std::cout << "(3)\n" << pretty_print(result3) << "\n\n";

    //json result4 = expr.evaluate(data, jsonpath::result_options::nodups);   (until 0.164.0)
    json result4 = expr.evaluate(data, jsonpath::result_options::nodups |
                                       jsonpath::result_options::path);       (since 0.164.0)
    std::cout << "(4)\n" << pretty_print(result4) << "\n\n";
}
```
Output:
```
(1) 
[
    "The Night Watch",
    "The Night Watch",
    "The Night Watch"
]

(2) 
[
    "$['books'][1]['title']",
    "$['books'][1]['title']",
    "$['books'][3]['title']"
]

(3) 
[
    "The Night Watch",
    "The Night Watch"
]

(4) 
[
    "$['books'][1]['title']",
    "$['books'][3]['title']"
]
```

 <div id="eg4"/>

#### Custom functions

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

using json = jsoncons::json;
namespace jsonpath = jsoncons::jsonpath;

template <typename Json>
class my_custom_functions : public jsonpath::custom_functions<Json>
{
public:
    my_custom_functions()
    {
        this->register_function("divide", // function name
             2,                           // number of arguments   
             [](jsoncons::span<const jsonpath::parameter<Json>> params, 
                std::error_code& ec) -> Json 
             {
               const Json& arg0 = params[0].value();    
               const Json& arg1 = params[1].value();    

               if (!(arg0.is_number() && arg1.is_number())) 
               {
                   ec = jsonpath::jsonpath_errc::invalid_type; 
                   return Json::null();
               }
               return Json(arg0.as<double>() / arg1.as<double>());
             }
       );
    }
};

int main()
{
    json root = json::parse(R"([{"foo": 60, "bar": 10},{"foo": 60, "bar": 5}])");

    json result = jsonpath::json_query(root, 
                                       "$[?(divide(@.foo, @.bar) == 6)]", 
                                       jsonpath::result_options(), 
                                       my_custom_functions<json>());   // (since 0.164.0)

    std::cout << pretty_print(result) << "\n\n";
}
```
Output:
```
[{"bar": 10,"foo": 60}]
```
