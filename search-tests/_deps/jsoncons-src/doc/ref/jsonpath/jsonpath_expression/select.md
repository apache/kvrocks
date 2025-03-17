### jsoncons::jsonpath::jsonpath_expression::select

```cpp
Json select(const_reference root, result_options options = result_options()); (1) (since 0.172.0)
```

```cpp
template <typename BinaryOp>
void select(const_reference root, BinaryOp op, 
    result_options options = result_options());                                     (2) (since 0.172.0)
```

(1) Evaluates the root value against the compiled JSONPath expression and returns an array of values.

(2) Evaluates the root value against the compiled JSONPath expression and calls a provided
callback repeatedly with the results.

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
<br/><br/><code>void fun(const basic_path_node&lt;Json::char_type&gt;& path, const Json& val);</code><br/><br/>
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

#### Receive locations and values selected from a root value (since 0.172.0)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

using json = jsoncons::json;
namespace jsonpath = jsoncons::jsonpath;

int main()
{
    auto expr = jsoncons::jsonpath::make_expression<json>("$.books[*]");

    std::ifstream is(/*path_to_books_file*/);
    json doc = json::parse(is);

    auto op = [&](const jsonpath::path_node& path, const json& value)
    {
        if (value.at("category") == "memoir" && !value.contains("price"))
        {
            std::cout << jsonpath::to_string(path) << ": " << value << "\n";
        }
    };

    expr.select(root, op);
}
```
Output:
```
$['books'][3]: {"author":"Phillips, David Atlee","category":"memoir","title":"The Night Watch"}
```

