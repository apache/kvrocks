### `jsoncons::basic_json::operator=`

```cpp
basic_json& operator=(const basic_json& rhs);
basic_json& operator=(basic_json&& rhs) noexcept; (1)

template <typename T>
basic_json& operator=(const T& rhs); (2)

basic_json& operator=(const char_type* rhs); (3)
```

(1) Assigns a new `json` value to a `json` variable, replacing it's current contents.

(2) Assigns the templated value to a `json` variable using [json_type_traits](json_type_traits.md).

