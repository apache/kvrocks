### jsoncons::jmespath::search

```cpp
#include <jsoncons_ext/jmespath/jmespath.hpp>

template<Json>
Json search(const Json& doc, 
            const Json::string_view_type& expr); (1)

template<Json>
Json search(const Json& doc, 
            const Json::string_view_type& expr,
            std::error_code& ec); (2)
```

Returns a Json value.

#### Parameters

<table>
  <tr>
    <td>doc</td>
    <td>Json value</td> 
  </tr>
  <tr>
    <td>expr</td>
    <td>JMESPath expression</td> 
  </tr>
  <tr>
    <td>ec</td>
    <td>out-parameter for reporting errors in the non-throwing overload</td> 
  </tr>
</table>

#### Return value

Returns a Json value.

#### Exceptions

(1) Throws a [jmespath_error](jmespath_error.md) if JMESPath evaluation fails.

(2) Sets the out-parameter `ec` to the [jmespath_error_category](jmespath_errc.md) if JMESPath evaluation fails. 

