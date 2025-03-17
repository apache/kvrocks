### jsoncons::key_value 

```cpp
template <typename KeyT,typename ValueT>
class key_value
```

`key_value` stores a key (name) and a json value

#### Member types

Member type                         |Definition
------------------------------------|------------------------------
`key_type`        |KeyT
`value_type`      |ValueT
`string_view_type`|ValueT::string_view_type

#### Accessors
    
    const key_type& key() const

    const json& value() const

    json& value()

#### Non member functions

<table border="0">
  <tr>
    <td><code>bool operator==(const key_value& lhs, const key_value& rhs)</code></td>
    <td>Returns <code>true</true> if two key_value objects compare equal, <code>false</true> otherwise.</td> 
  </tr>
  <tr>
    <td><code>bool operator!=(const key_value& lhs, const key_value& rhs)</code></td>
    <td>Returns <code>true</true> if two key_value objects do not compare equal, <code>false</true> otherwise.</td> 
  </tr>
  <tr>
    <td><code>bool operator<(const key_value& lhs, const key_value& rhs)</code></td>
    <td>Compares the contents of lhs and rhs lexicographically.</td> 
  </tr>
  <tr>
    <td><code>bool operator<=(const key_value& lhs, const key_value& rhs)</code></td>
    <td>Compares the contents of lhs and rhs lexicographically.</td> 
  </tr>
  <tr>
    <td><code>bool operator>(const key_value& lhs, const key_value& rhs)</code></td>
    <td>Compares the contents of lhs and rhs lexicographically.</td> 
  </tr>
  <tr>
    <td><code>bool operator>=(const key_value& lhs, const key_value& rhs)</code></td>
    <td>Compares the contents of lhs and rhs lexicographically.</td> 
  </tr>
</table>


