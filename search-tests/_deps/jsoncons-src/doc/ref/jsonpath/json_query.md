### jsoncons::jsonpath::json_query

```cpp
#include <jsoncons_ext/jsonpath/json_query.hpp>
```

```cpp
template <typename Json>
Json json_query(const Json& root value, 
    const Json::string_view_type& expr,                                     (until 0.164.0)
    result_options options = result_options());                             
                                                                       (1)
template <typename Json>                                              
Json json_query(const Json& root value, 
    const Json::string_view_type& expr,                                     (since 0.164.0)
    result_options options = result_options(),
    const custom_functions<Json>& funcs = custom_functions<Json>());                    
```
```cpp
template <typename Json,typename BinaryCallback>
void json_query(const Json& root value, 
    const Json::string_view_type& expr,                                     (until 0.164.0)
    BinaryCallback callback
    result_options options = result_options());       
                                                                       (2)
template <typename Json,typename BinaryCallback>                        
void json_query(const Json& root value, 
    const Json::string_view_type& expr,                                     (since 0.164.0)
    BinaryCallback callback,
    result_options options = result_options(),
    const custom_functions<Json>& funcs = custom_functions<Json>());        
```

```cpp
template <typename Json,typename TempAllocator>                                              
Json json_query(const allocator_set<Json::allocator_type,TempAllocator>& alloc_set, 
    const Json& root value, const Json::string_view_type& expr,        (3) (since 0.170.0)
    result_options options = result_options(),
    const custom_functions<Json>& funcs = custom_functions<Json>());                    
```
```cpp
template <typename Json,typename BinaryCallback,typename TempAllocator>                        
void json_query(const allocator_set<Json::allocator_type,TempAllocator>& alloc_set, 
    const Json& root value, const Json::string_view_type& expr,        (4) (since 0.170.0)
    BinaryCallback callback,
    result_options options = result_options(),
    const custom_functions<Json>& funcs = custom_functions<Json>());                      
```
(1) Evaluates the root value against the JSONPath expression `expr` and returns an array of values or 
normalized path expressions. 

(2) Evaluates the root value against the JSONPath expression `expr` and calls a provided
callback repeatedly with the results. 

(3)-(4) Same as (1-2) except that `alloc` is used to allocate memory during expression compilation and evaluation.

#### Parameters

<table>
  <tr>
    <td><code>root value</code></td>
    <td>Json value</td> 
  </tr>
  <tr>
    <td><code>expr</code></td>
    <td>JSONPath expression string</td> 
  </tr>
  <tr>
    <td><code>callback</code></td>
    <td>A function object that accepts a path and a reference to a Json value. 
It must have function call signature equivalent to
<br/><br/><code>
void fun(const Json::string_view_type& path, const Json& val);
</code><br/><br/>
  </tr>
  <tr>
    <td><code>type</code> (until 0.161.0))</td>
    <td>Since 0.161.0, typedefed to <code>jsonpath_options</code>.</td> 
  </tr>
  <tr>
    <td><code>options</code> (since 0.161.0)</td>
    <td>Result options, a bitmask of type <a href="result_options.md">result_options</></td> 
  </tr>
</table>

#### Return value

(1) returns an array containing either values or normalized path expressions matching the JSONPath expression, 
or an empty array if there is no match.

#### Exceptions

Throws a [jsonpath_error](jsonpath_error.md) if JSONPath parsing fails.

### Examples

#### Store examples

The examples below use the sample data file `store.json`.

```json
{ "store": {
    "book": [ 
      { "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      { "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      },
      { "category": "fiction",
        "author": "Herman Melville",
        "title": "Moby Dick",
        "isbn": "0-553-21311-3",
        "price": 8.99
      },
      { "category": "fiction",
        "author": "J. R. R. Tolkien",
        "title": "The Lord of the Rings",
        "isbn": "0-395-19395-8",
        "price": 22.99
      }
    ],
    "bicycle": {
      "color": "red",
      "price": 19.95
    }
  }
}
```

```cpp    
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <fstream>

namespace jsonpath = jsoncons::jsonpath;

int main()
{
    std::ifstream is("./input/store.json");
    auto booklist = jsoncons::json::parse(is);

    // The authors of books that are cheaper than $10
    jsoncons::json result1 = jsonpath::json_query(booklist, "$.store.book[?(@.price < 10)].author");
    std::cout << "(1) " << result1 << "\n";

    // The number of books
    jsoncons::json result2 = jsonpath::json_query(booklist, "length($..book)");
    std::cout << "(2) " << result2 << "\n";

    // The third book
    jsoncons::json result3 = jsonpath::json_query(booklist, "$..book[2]");
    std::cout << "(3)\n" << pretty_print(result3) << "\n";

    // All books whose author's name starts with Evelyn
    jsoncons::json result4 = jsonpath::json_query(booklist, "$.store.book[?(@.author =~ /Evelyn.*?/)]");
    std::cout << "(4)\n" << pretty_print(result4) << "\n";

    // The titles of all books that have isbn number
    jsoncons::json result5 = jsonpath::json_query(booklist, "$..book[?(@.isbn)].title");
    std::cout << "(5) " << result5 << "\n";

    // All authors and titles of books
    jsoncons::json result6 = jsonpath::json_query(booklist, "$['store']['book']..['author','title']");
    std::cout << "(6)\n" << pretty_print(result6) << "\n";

    // Union of two ranges of book titles
    jsoncons::json result7 = jsonpath::json_query(booklist, "$..book[1:2,2:4].title");
    std::cout << "(7) " << result7 << "\n";

    // Union of a subset of book titles identified by index
    jsoncons::json result8 = jsonpath::json_query(booklist, "$.store[@.book[0].title,@.book[1].title,@.book[3].title]");
    std::cout << "(8) " << result8 << "\n";

    // Union of third book title and all book titles with price > 10
    jsoncons::json result9 = jsonpath::json_query(booklist, "$.store[@.book[3].title,@.book[?(@.price > 10)].title]");
    std::cout << "(9) " << result9 << "\n";

    // Intersection of book titles with category fiction and price < 15
    jsoncons::json result10 = jsonpath::json_query(booklist, "$.store.book[?(@.category == 'fiction' && @.price < 15)].title");
    std::cout << "(10) " << result10 << "\n";

    // Normalized path expressions
    jsoncons::json result11 = jsonpath::json_query(booklist, "$.store.book[?(@.author =~ /Evelyn.*?/)]", jsonpath::result_options::path);
    std::cout << "(11) " << result11 << "\n";

    // All titles whose author's second name is 'Waugh'
    jsoncons::json result12 = jsonpath::json_query(booklist,"$.store.book[?(tokenize(@.author,'\\\\s+')[1] == 'Waugh')].title");
    std::cout << "(12) " << result12 << "\n";

    // All keys in the second book
    jsoncons::json result13 = jsonpath::json_query(booklist,"keys($.store.book[1])");
    std::cout << "(13) " << result13 << "\n";
}
```
Output:
```
(1) ["Nigel Rees","Herman Melville"]
(2) [4]
(3)
[
    {
        "author": "Herman Melville",
        "category": "fiction",
        "isbn": "0-553-21311-3",
        "price": 8.99,
        "title": "Moby Dick"
    }
]
(4)
[
    {
        "author": "Evelyn Waugh",
        "category": "fiction",
        "price": 12.99,
        "title": "Sword of Honour"
    }
]
(5) ["Moby Dick","The Lord of the Rings"]
(6)
[
    "Nigel Rees",
    "Sayings of the Century",
    "Evelyn Waugh",
    "Sword of Honour",
    "Herman Melville",
    "Moby Dick",
    "J. R. R. Tolkien",
    "The Lord of the Rings"
]
(7) ["Sword of Honour","Moby Dick","The Lord of the Rings"]
(8) ["Sayings of the Century","Sword of Honour","The Lord of the Rings"]
(9) ["The Lord of the Rings","Sword of Honour","The Lord of the Rings"]
(10) ["Sword of Honour","Moby Dick"]
(11) ["$['store']['book'][1]"]
(12) ["Sword of Honour"]
(13) [["author","category","price","title"]]
```

#### Result options

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

using jsoncons::json;
namespace jsonpath = jsoncons::jsonpath;

int main()
{
    std::string s = "[1,2,3,4,5]";
    json data = json::parse(s);
    std::string path = "$[4,1,1]";

    auto result1 = jsonpath::json_query(data, path);
    std::cout << "(1) " << result1 << "\n\n";

    auto result2 = jsonpath::json_query(data, path, jsonpath::result_options::path);
    std::cout << "(2) " << result2 << "\n\n";

    //auto result3 = jsonpath::json_query(data, path, 
    //                                    jsonpath::result_options::value | 
    //                                    jsonpath::result_options::sort); (until 0.164.0)
    auto result3 = jsonpath::json_query(data, path, 
                                        jsonpath::result_options::sort);   (since 0.164.0)  
    std::cout << "(3) " << result3 << "\n\n";

    //auto result4 = jsonpath::json_query(data, path, 
    //                                    jsonpath::result_options::sort);   (until 0.164.0)
    auto result4 = jsonpath::json_query(data, path, 
                                        jsonpath::result_options::sort | 
                                        jsonpath::result_options::path);     (since 0.164.0)
    std::cout << "(4) " << result4 << "\n\n";

    //auto result5 = jsonpath::json_query(data, path, 
    //                                    jsonpath::result_options::value | 
    //                                    jsonpath::result_options::nodups); (until 0.164.0)
    auto result5 = jsonpath::json_query(data, path, 
                                        jsonpath::result_options::nodups);   (since 0.164.0)
    std::cout << "(5) " << result5 << "\n\n";

    //auto result6 = jsonpath::json_query(data, path, 
    //                                    jsonpath::result_options::nodups); (until 0.164.0)
    auto result6 = jsonpath::json_query(data, path, 
                                        jsonpath::result_options::nodups | 
                                        jsonpath::result_options::path);   (since 0.164.0)
    std::cout << "(6) " << result6 << "\n\n";

    //auto result7 = jsonpath::json_query(data, path, 
    //                                    jsonpath::result_options::value | 
    //                                    jsonpath::result_options::nodups | 
    //                                    jsonpath::result_options::sort);   (until 0.164.0)
    auto result7 = jsonpath::json_query(data, path, 
                                        jsonpath::result_options::nodups | 
                                        jsonpath::result_options::sort);     (since 0.164.0)
    std::cout << "(7) " << result7 << "\n\n";

    //auto result8 = jsonpath::json_query(data, path, 
    //                                    jsonpath::result_options::nodups | 
    //                                    jsonpath::result_options::sort);  (until 0.164.0)
    auto result8 = jsonpath::json_query(data, path, 
                                        jsonpath::result_options::nodups | 
                                        jsonpath::result_options::sort |
                                        jsonpath::result_options::path);    (since 0.164.0)
    std::cout << "(8) " << result8 << "\n\n";
}
```
Output:
```json
(1) [5,2,2]

(2) ["$[4]","$[1]","$[1]"]

(3) [2,2,5]

(4) ["$[1]","$[1]","$[4]"]

(5) [5,2]

(6) ["$[4]","$[1]"]

(7) [2,5]

(8) ["$[1]","$[4]"]
```

#### Callback

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

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <fstream>

using jsoncons::json;
namespace jsonpath = jsoncons::jsonpath;

int main()
{
    std::ifstream is(/*path_to_books_file*/);
    json data = json::parse(is);
    std::string path = "$.books[?(@.price >= 22.0)]";

    auto callback = [](const std::string& path, const json& val)
    {
        std::cout << path << ": " << val << "\n";
    };
    jsonpath::json_query(data, path, callback, jsonpath::result_options::path);
}
```
Output:
```
$['books'][0]: {"author":"Haruki Murakami","category":"fiction","price":22.72,"title":"A Wild Sheep Chase"}
$['books'][1]: {"author":"Sergei Lukyanenko","category":"fiction","price":23.58,"title":"The Night Watch"}
```

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
                                       my_custom_functions<json>());

    std::cout << pretty_print(result) << "\n\n";
}
```
Output:
```
[{"bar": 10,"foo": 60}]
```
