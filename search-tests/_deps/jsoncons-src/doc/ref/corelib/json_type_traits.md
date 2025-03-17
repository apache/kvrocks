### jsoncons::json_type_traits

```cpp
#include <jsoncons/json_type_traits.hpp>
```

<br>

`json_type_traits` defines a compile time template based interface for conversion between a `basic_json` value
and a value of some other type. `json_type_traits` implementations must specialize this traits class:

```cpp
template <typename Json,typename T,typename Enable=void>
struct json_type_traits
{
    using allocator_type = Json::allocator_type;

    static constexpr bool is(const Json&) noexcept;

    static T as(const Json&);

    static Json to_json(const T& val);

    static Json to_json(const T& val, const allocator_type& alloc);
};
```

The function `json_type_traits<Json,T>::is(const Json& j)` indictates whether `j` satisfies 
the requirements of type `T`. This function supports the type selection strategy when
converting a `Json` value to the proper derived class in the polymorphic case, and
when converting a `Json` value to the proper alternative type in the variant case.  

The function `json_type_traits<Json,T>::as(const Json& j)` tries to convert `j`
to a value of type `T`.

The functions `json_type_traits<Json,T>::to_json(const T& val)` and 
`json_type_traits<Json,T>::to_json(const T& val, const allocator_type& alloc)`
try to convert `val` into a `Json` value.

jsoncons includes specializiations for most types in the standard library. 
And it includes convenience macros that make specializing `json_type_traits` for your own types easier.

[Built-in Specializations](json_type_traits/built-in-specializations.md)

[Convenience Macros](json_type_traits/convenience-macros.md)

[Custom Specializations](json_type_traits/custom-specializations.md)


