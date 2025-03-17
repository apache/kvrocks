### jsoncons::jsonpath::jsonpath_expression::evaluate

```cpp
Json evaluate(reference root, result_options options = result_options()); (1)
```
```cpp
template <typename BinaryOp>
void evaluate(reference root, BinaryOp op, 
              result_options options = result_options());  (2)
```

(1) Evaluates the root value against the compiled JSONPath expression and returns an array of values or 
normalized path expressions. 

(2) Evaluates the root value against the compiled JSONPath expression and calls a provided
callback repeatedly with the results.

Note: This function is kept for backwards compatability. New code should use the [select](select.md) function. 

#### Parameters

<table>
  <tr>
    <td>root</td>
    <td>Root JSON value</td> 
  </tr>
  <tr>
    <td><code>op</code></td>
    <td>A function object that accepts a path and a reference to a Json value.
It must have function call signature equivalent to
<br/><br/><code>void fun(const Json::string_view_type& path, const Json& val);</code><br/><br/>
  </tr>
  <tr>
    <td>result_options</td>
    <td>Result options, a bitmask of type <a href="result_options.md">result_options</></td> 
  </tr>
</table>

### Examples

The examples below uses the sample data file `books.json`, 

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

#### Select values from root value

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

using json = jsoncons::json;
namespace jsonpath = jsoncons::jsonpath;

int main()
{
    auto expr = jsonpath::make_expression<json>("$.books[?(@.price > avg($.books[*].price))].title");

    std::ifstream is(/*path_to_books_file*/);
    json root = json::parse(is);

    json result = expr.evaluate(root);
    std::cout << pretty_print(result) << "\n\n";
}
```
Output:
```
[
    "The Night Watch"
]
```

#### Select values with locations from root value

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

using json = jsoncons::json;
namespace jsonpath = jsoncons::jsonpath;

int main()
{
    auto expr = jsonpath::make_expression<json>("$.books[?(@.price >= 22.0)]");

    std::ifstream is(/*path_to_books_file*/);
    json root = json::parse(is);

    auto callback = [](const std::string& path, const json& val)
    {
       std::cout << path << ": " << val << "\n";
    };

    expr.evaluate(root, callback, jsonpath::result_options::path);
}
```
Output:
```
$['books'][0]: {"author":"Haruki Murakami","category":"fiction","price":22.72,"title":"A Wild Sheep Chase"}
$['books'][1]: {"author":"Sergei Lukyanenko","category":"fiction","price":23.58,"title":"The Night Watch"}
```

