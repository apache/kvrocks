### jsoncons::jsonpath::make_expression

```cpp
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
```

```cpp
template <typename Json>
jsonpath_expression<Json> make_expression(const Json::string_view_type& expr);      (until 0.164.0)
```
```cpp
template <typename Json>                                                           (1)
jsonpath_expression<Json> make_expression(const Json::string_view_type& expr,
    const custom_functions<Json>& funcs = custom_functions<Json>());                (since 0.164.0)
```
```cpp
template <typename Json>
jsonpath_expression<Json> make_expression(const Json::string_view_type& expr,
    std::error_code& ec);                                                       (2)
```
```cpp
template <typename Json>                                                           
jsonpath_expression<Json> make_expression(const Json::string_view_type& expr,
    const custom_functions<Json>& funcs, std::error_code& ec);                          (3) (since 0.164.0)
```
```cpp
template <typename Json,typename TempAllocator>                                              (4) (since 0.170.0)
jsonpath_expression<Json> make_expression(const allocator_set<Json::allocator_type,TempAllocator>& alloc_set, 
    Json::string_view_type& expr,
    const custom_functions<Json>& funcs = custom_functions<Json>());                
```
```cpp
template <typename Json,typename TempAllocator>                                              (5) (since 0.170.0)
jsonpath_expression<Json> make_expression(const allocator_set<Json::allocator_type,TempAllocator>& alloc_set, 
    Json::string_view_type& expr, std::error_code& ec);                
```
```cpp
template <typename Json,typename TempAllocator>                                              (6) (since 0.170.0)
jsonpath_expression<Json> make_expression(const allocator_set<Json::allocator_type,TempAllocator>& alloc_set, 
    Json::string_view_type& expr,
    const custom_functions<Json>& funcs, std::error_code& ec);                
```

(1) Makes a [jsonpath_expression](jsonpath_expression.md) from the JSONPath expression `expr`.

(2-3) Makes a [jsonpath_expression](jsonpath_expression.md) from the JSONPath expression `expr`.

(4-6) Same as (1-3) except that `alloc` is used to allocate memory during expression compilation and evaluation.

#### Parameters

<table>
  <tr>
    <td><code>expr</code></td>
    <td>JSONPath expression string</td> 
  </tr>
  <tr>
    <td><code>funcs</code></td>
    <td>Custom functions</td> 
  </tr>
  <tr>
    <td><code>ec</code></td>
    <td>out-parameter for reporting errors in the non-throwing overload</td> 
  </tr>
</table>

#### Return value

Returns a [jsonpath_expression](jsonpath_expression.md) object that represents the JSONPath expression.

#### Exceptions

(1) throws a [jsonpath_error](jsonpath_error.md) if JSONPath compilation fails.

(2) sets the out-parameter `ec` to the [jsonpath_error_category](jsonpath_errc.md) if JSONPath compilation fails. 

### Examples

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

    auto expr = jsonpath::make_expression<json>("$[?(divide(@.foo, @.bar) == 6)]", 
                                                my_custom_functions<json>());
    json result = expr.evaluate(root);

    std::cout << result << "\n\n";
}
```
Output:
```
[{"bar": 10,"foo": 60}]
```

#### make_expression with stateful allocator

```cpp
#include <string_view> // Assuming C++ 17
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include "free_list_allocator.hpp" // for free_list_allocator

using my_alloc = free_list_allocator<char>; // an allocator with a single-argument constructor
using my_json = jsoncons::basic_json<char,jsoncons::sorted_policy,my_alloc>;

namespace jsonpath = jsoncons::jsonpath;

int main()
{
    auto alloc = my_alloc(1);        

    // until 0.171.0    
    // jsoncons::json_decoder<my_json,my_alloc> decoder(jsoncons::result_allocator_arg, 
    //    alloc, alloc); 

    // since 0.170.1
    jsoncons::json_decoder<my_json,my_alloc> decoder(alloc, alloc); 

    std::ifstream is(/*path_to_books_file*/);

    jsoncons::basic_json_reader<char,jsoncons::stream_source<char>,my_alloc> reader(is, decoder, alloc);
    reader.read();

    my_json doc = decoder.get_result();

    std::string_view p{"$.books[?(@.category == 'fiction')].title"};
    auto expr = jsonpath::make_expression<my_json>(combine_allocators(alloc), p);  
    auto result = expr.evaluate(doc);

    std::cout << pretty_print(result) << "\n\n";
}
```
Output:
```json
[
    "A Wild Sheep Chase",
    "The Night Watch",
    "The Comedians"
]
```
