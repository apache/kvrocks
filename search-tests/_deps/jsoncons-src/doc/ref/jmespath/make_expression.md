### jsoncons::jmespath::make_expression

```cpp
#include <jsoncons_ext/jmespath/jmespath.hpp>

template <typename Json>
jmespath_expression<Json> make_expression(const json::string_view_type& expr);        (until 1.0.0)
                                                                                 (1)
template <typename Json>
jmespath_expression<Json> make_expression(const Json::string_view_type& expr,         (since 1.0.0)
    const custom_functions<Json>& funcs = custom_functions<Json>());

template <typename Json>
jmespath_expression<Json> make_expression(const json::string_view_type& expr,    (2)
    std::error_code& ec);                                                        

template <typename Json>
jmespath_expression<Json> make_expression(const Json::string_view_type& expr,    (3)   (since 1.0.0)  
    const custom_functions<Json>& funcs,
    std::error_code& ec)
```

Returns a compiled JMESPath expression for later evaluation.

#### Parameters

<table>
  <tr>
    <td>expr</td>
    <td>JMESPath expression</td> 
  </tr>
  <tr>
    <td>funcs</td>
    <td>Custom functions</td> 
  </tr>
  <tr>
    <td>ec</td>
    <td>out-parameter for reporting errors in the non-throwing overload</td> 
  </tr>
</table>

#### Return value

Returns a `jmespath_expression` object that represents the JMESPath expression.

#### Exceptions

(1) Throws a [jmespath_error](jmespath_error.md) if JMESPath compilation fails.

(2) Sets the out-parameter `ec` to the [jmespath_error_category](jmespath_errc.md) if JMESPath compilation fails. 

