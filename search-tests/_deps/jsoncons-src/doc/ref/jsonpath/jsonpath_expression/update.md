### jsoncons::jsonpath::jsonpath_expression::update

```cpp
template <typename BinaryOp>
void update(reference root, BinaryOp op);                                   (1) (since 0.172.0)
```

(1) Evaluates the root value against the compiled JSONPath expression and calls a provided
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
<br/><br/><code>void fun(const basic_path_node&lt;Json::char_type&gt;& path, Json& val);</code><br/><br/>
  </tr>
</table>

The callback receives nodes with duplicates removed, and paths sorted in descending order.

### Examples

#### Update in place

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

    auto callback = [](const jsonpath::path_node& /*path*/, json& book)
    {
        if (book.at("category") == "memoir" && !book.contains("price"))
        {
            book.try_emplace("price", 140.0);
        }
    };

    expr.update(doc, callback);
}
```
Output:
```
{
    "author": "Phillips, David Atlee",
    "category": "memoir",
    "price": 140.0,
    "title": "The Night Watch"
}
```

