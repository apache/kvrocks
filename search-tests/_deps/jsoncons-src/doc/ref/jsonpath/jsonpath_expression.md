### jsoncons::jsonpath::jsonpath_expression

```cpp
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

template <typename Json>
class jsonpath_expression
```

#### Member functions

<table border="0">
  <tr>
    <td><a href="jsonpath_expression/evaluate.md">evaluate</a></td>
    <td>Select values from a JSON document</td> 
  </tr>
  <tr>
    <td><a href="jsonpath_expression/select.md">select (since 0.172.0)</a></td>
    <td>Select values from a JSON document</td> 
  </tr>
  <tr>
    <td><a href="jsonpath_expression/select_paths.md">select_paths (since 0.172.0)</a></td>
    <td>Select paths of values selected from a JSON document</td> 
  </tr>
  <tr>
    <td><a href="jsonpath_expression/update.md">update (since 0.172.0)</a></td>
    <td>Update a JSON document in place</td> 
  </tr>
</table>

```cpp
template <typename BinaryOp>
void update(const_reference root, BinaryOp op);                                   (1) (since 0.172.0)
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
<br/><br/><code>
void fun(const basic_path_node&lt;Json::char_type&gt;& path, Json& val);
</code><br/><br/>
  </tr>
</table>

#### Non-member functions

<table border="0">
  <tr>
    <td><a href="make_expression.md">make_expression</a></td>
    <td>Returns a `jsonpath_expression` for later evaluation. (since 0.161.0)</td> 
  </tr>
</table>

